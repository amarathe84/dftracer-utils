#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/checkpoint.h>
#include <dftracer/utils/indexer/constants.h>
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/indexer/inflater.h>
#include <dftracer/utils/utils/filesystem.h>
#include <dftracer/utils/utils/platform_compat.h>
#include <picosha2.h>
#include <sqlite3.h>
#include <zlib.h>

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace dftracer::utils {

// ==============================================================================
// Helper Structures
// ==============================================================================

struct CheckpointData {
    size_t uc_offset;
    size_t c_offset;
    int bits;
    unsigned char window[indexer::constants::ZLIB_WINDOW_SIZE];

    CheckpointData() : uc_offset(0), c_offset(0), bits(0) {
        memset(window, 0, sizeof(window));
    }
};

// ==============================================================================
// Error Handling Helper
// ==============================================================================

class ErrorHandler {
   public:
    static void validate_parameters(size_t ckpt_size) {}

    static void check_indexer_state(sqlite3 *db) {
        if (!db) {
            throw IndexerError(IndexerError::Type::DATABASE_ERROR,
                               "Database connection is not open");
        }
    }

    static void validate_line_range(size_t start_line, size_t end_line) {
        if (start_line == 0 || end_line == 0 || start_line > end_line) {
            throw IndexerError(
                IndexerError::Type::INVALID_ARGUMENT,
                "Invalid line range: start_line and end_line must be > 0 and "
                "start_line <= end_line");
        }
    }
};

class Indexer::Impl {
   public:
    Impl(const std::string &gz_path, const std::string &idx_path,
         size_t ckpt_size, bool force_rebuild)
        : gz_path_(gz_path),
          gz_path_logical_path_(get_logical_path(gz_path)),
          idx_path_(idx_path),
          ckpt_size_(ckpt_size),
          force_rebuild_(force_rebuild),
          db_(nullptr),
          db_opened_(false),
          cached_file_id_(-1) {
        ErrorHandler::validate_parameters(ckpt_size_);
        DFTRACER_UTILS_LOG_DEBUG("Created DFT indexer for gz: %s and index: %s",
                                 gz_path.c_str(), idx_path.c_str());
    }

    ~Impl() {
        if (db_opened_ && db_) {
            sqlite3_close(db_);
        }
    }

    bool need_rebuild() const;
    void build();

    bool is_valid() const { return true; }

    const std::string &get_gz_path() const { return gz_path_; }

    const std::string &get_idx_path() const { return idx_path_; }

    size_t get_checkpoint_size() const { return ckpt_size_; }

    uint64_t get_max_bytes() const;
    uint64_t get_num_lines() const;
    int find_file_id(const std::string &gz_path) const;
    bool find_checkpoint(size_t target_offset, Checkpoint &checkpoint) const;
    std::vector<Checkpoint> get_checkpoints() const;
    std::vector<Checkpoint> find_checkpoints_by_line_range(
        size_t start_line, size_t end_line) const;
    int get_file_id() const;

   private:
    // Helper methods
    bool index_exists_and_valid(const std::string &idx_path) const;
    int init_schema(sqlite3 *db) const;
    int build_index_internal(sqlite3 *db, int file_id,
                             const std::string &gz_path,
                             size_t ckpt_size) const;

    // Database cleanup helpers
    int cleanup_existing_data(sqlite3 *db, int file_id) const;
    int insert_metadata(sqlite3 *db, int file_id, size_t ckpt_size,
                        uint64_t total_lines, uint64_t total_uc_size) const;
    int process_chunks(FILE *fp, sqlite3 *db, int file_id, size_t ckpt_size,
                       uint64_t &total_lines_out,
                       uint64_t &total_uc_size_out) const;
    int create_checkpoint(Inflater &inflater, CheckpointData *checkpoint,
                          size_t uc_offset) const;
    int compress_window(const unsigned char *window, size_t window_size,
                        unsigned char **compressed,
                        size_t *compressed_size) const;
    int save_checkpoint(sqlite3 *db, int file_id,
                        const CheckpointData *checkpoint) const;

    std::string get_logical_path(const std::string &path) const;

    std::string gz_path_;
    std::string gz_path_logical_path_;
    std::string idx_path_;
    size_t ckpt_size_;
    bool force_rebuild_;
    sqlite3 *db_;
    bool db_opened_;
    mutable int cached_file_id_;
};

bool Indexer::Impl::index_exists_and_valid(const std::string &idx_path) const {
    FILE *f = fopen(idx_path.c_str(), "rb");
    if (!f) return false;
    fclose(f);

    sqlite3 *db;
    if (sqlite3_open(idx_path.c_str(), &db) != SQLITE_OK) {
        return false;
    }

    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT name FROM sqlite_master WHERE type='table' AND "
        "name IN ('checkpoints', 'metadata', 'files')";
    int table_count = 0;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            table_count++;
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    return table_count >= 3;
}

int Indexer::Impl::cleanup_existing_data(sqlite3 *db, int file_id) const {
    const char *cleanup_queries[] = {
        "DELETE FROM checkpoints WHERE file_id = ?;",
        "DELETE FROM metadata WHERE file_id = ?;"};

    for (const char *query : cleanup_queries) {
        sqlite3_stmt *stmt = nullptr;
        if (sqlite3_prepare_v2(db, query, -1, &stmt, nullptr) != SQLITE_OK) {
            DFTRACER_UTILS_LOG_ERROR(
                "Failed to prepare cleanup statement '%s': %s", query,
                sqlite3_errmsg(db));
            return -1;
        }
        sqlite3_bind_int(stmt, 1, file_id);
        int result = sqlite3_step(stmt);
        if (result != SQLITE_DONE) {
            DFTRACER_UTILS_LOG_ERROR(
                "Failed to execute cleanup statement '%s' for file_id %d: %d - "
                "%s",
                query, file_id, result, sqlite3_errmsg(db));
            sqlite3_finalize(stmt);
            return -1;
        }
        sqlite3_finalize(stmt);
    }
    DFTRACER_UTILS_LOG_DEBUG(
        "Successfully cleaned up existing data for file_id %d", file_id);
    return 0;
}

int Indexer::Impl::process_chunks(FILE *fp, sqlite3 *db, int file_id,
                                  size_t checkpoint_size,
                                  uint64_t &total_lines_out,
                                  uint64_t &total_uc_size_out) const {
    // Reset file pointer to beginning for gzip decompression
    if (fseeko(fp, 0, SEEK_SET) != 0) {
        return -4;
    }

    try {
        SqliteStmt st_checkpoint(
            db,
            "INSERT INTO checkpoints(file_id, checkpoint_idx, uc_offset, "
            "uc_size, "
            "c_offset, c_size, bits, dict_compressed, num_lines) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");

        Inflater inflater(fp);

        DFTRACER_UTILS_LOG_DEBUG(
            "Starting sequential checkpoint creation with checkpoint_size=%zu "
            "bytes",
            checkpoint_size);

        size_t checkpoint_idx = 0;
        size_t current_uc_offset = 0;  // Current uncompressed offset
        size_t checkpoint_start_uc_offset =
            0;  // Start of current checkpoint (uncompressed)

        unsigned char buffer[constants::PROCESS_BUFFER_SIZE];
        uint64_t total_lines = 0;

        while (true) {
            size_t bytes_read;
            size_t c_off;

            if (!inflater.process(buffer, sizeof(buffer), &bytes_read,
                                  &c_off)) {
                break;
            }

            if (bytes_read == 0) {
                // End of file reached - handle any remaining data
                break;
            }

            // Count lines in this buffer
            for (size_t i = 0; i < bytes_read; i++) {
                if (buffer[i] == '\n') {
                    total_lines++;
                }
            }

            current_uc_offset += bytes_read;

            // Check if current checkpoint has reached the target size
            size_t current_checkpoint_size =
                current_uc_offset - checkpoint_start_uc_offset;
            if (current_checkpoint_size >= checkpoint_size &&
                (inflater.stream.data_type & 0xc0) ==
                    0x80 &&                        // At deflate block boundary
                inflater.stream.avail_out == 0) {  // Output buffer is consumed

                // Create checkpoint at current position
                size_t checkpoint_uc_size =
                    current_uc_offset - checkpoint_start_uc_offset;

                // Create checkpoint for random access
                CheckpointData checkpoint_data;
                if (create_checkpoint(inflater, &checkpoint_data,
                                      current_uc_offset) == 0) {
                    unsigned char *compressed_dict = nullptr;
                    size_t compressed_dict_size = 0;

                    if (compress_window(checkpoint_data.window,
                                        indexer::constants::ZLIB_WINDOW_SIZE,
                                        &compressed_dict,
                                        &compressed_dict_size) == 0) {
                        // Insert checkpoint into database
                        sqlite3_bind_int(st_checkpoint, 1, file_id);
                        sqlite3_bind_int64(
                            st_checkpoint, 2,
                            static_cast<int64_t>(checkpoint_idx));
                        sqlite3_bind_int64(
                            st_checkpoint, 3,
                            static_cast<int64_t>(current_uc_offset));
                        sqlite3_bind_int64(
                            st_checkpoint, 4,
                            static_cast<int64_t>(checkpoint_uc_size));
                        sqlite3_bind_int64(
                            st_checkpoint, 5,
                            static_cast<int64_t>(checkpoint_data.c_offset));
                        sqlite3_bind_int64(
                            st_checkpoint, 6,
                            static_cast<int64_t>(
                                checkpoint_data
                                    .c_offset));  // c_size same as c_offset for
                                                  // simplicity
                        sqlite3_bind_int(st_checkpoint, 7,
                                         checkpoint_data.bits);
                        sqlite3_bind_blob(
                            st_checkpoint, 8, compressed_dict,
                            static_cast<int>(compressed_dict_size),
                            SQLITE_TRANSIENT);
                        sqlite3_bind_int64(st_checkpoint, 9,
                                           static_cast<int64_t>(
                                               0));  // Approximate line count -
                                                     // not tracked precisely
                        sqlite3_step(st_checkpoint);
                        sqlite3_reset(st_checkpoint);

                        DFTRACER_UTILS_LOG_DEBUG(
                            "Created checkpoint %zu: uc_offset=%zu, size=%zu "
                            "bytes",
                            checkpoint_idx, checkpoint_start_uc_offset,
                            checkpoint_uc_size);

                        free(compressed_dict);

                        // Setup for next checkpoint
                        checkpoint_idx++;
                        checkpoint_start_uc_offset = current_uc_offset;
                    }
                }
            }
        }

        // Always create a final checkpoint if we have any remaining data
        if (current_uc_offset > checkpoint_start_uc_offset) {
            size_t checkpoint_uc_size =
                current_uc_offset - checkpoint_start_uc_offset;

            // Create a special checkpoint for remaining data (might be without
            // deflate boundary)
            if (checkpoint_start_uc_offset == 0) {
                // This is a checkpoint starting from the beginning - no
                // dictionary needed
                sqlite3_bind_int(st_checkpoint, 1, file_id);
                sqlite3_bind_int64(st_checkpoint, 2,
                                   static_cast<int64_t>(checkpoint_idx));
                sqlite3_bind_int64(
                    st_checkpoint, 3,
                    static_cast<int64_t>(checkpoint_start_uc_offset));
                sqlite3_bind_int64(st_checkpoint, 4,
                                   static_cast<int64_t>(checkpoint_uc_size));
                sqlite3_bind_int64(st_checkpoint, 5,
                                   static_cast<int64_t>(0));  // c_offset
                sqlite3_bind_int64(st_checkpoint, 6,
                                   static_cast<int64_t>(0));  // c_size
                sqlite3_bind_int(st_checkpoint, 7, 0);        // bits
                sqlite3_bind_blob(st_checkpoint, 8, nullptr, 0,
                                  SQLITE_TRANSIENT);  // No dictionary
                sqlite3_bind_int64(
                    st_checkpoint, 9,
                    static_cast<int64_t>(total_lines));  // Total lines in file
                sqlite3_step(st_checkpoint);
                sqlite3_reset(st_checkpoint);

                DFTRACER_UTILS_LOG_DEBUG(
                    "Created start checkpoint %zu: uc_offset=%zu, size=%zu "
                    "bytes (no "
                    "dictionary)",
                    checkpoint_idx, checkpoint_start_uc_offset,
                    checkpoint_uc_size);
            } else {
                // Try to create a regular checkpoint with dictionary if
                // possible
                CheckpointData checkpoint_data;
                if (create_checkpoint(inflater, &checkpoint_data,
                                      current_uc_offset) == 0) {
                    unsigned char *compressed_dict = nullptr;
                    size_t compressed_dict_size = 0;

                    if (compress_window(checkpoint_data.window,
                                        indexer::constants::ZLIB_WINDOW_SIZE,
                                        &compressed_dict,
                                        &compressed_dict_size) == 0) {
                        sqlite3_bind_int(st_checkpoint, 1, file_id);
                        sqlite3_bind_int64(
                            st_checkpoint, 2,
                            static_cast<int64_t>(checkpoint_idx));
                        sqlite3_bind_int64(
                            st_checkpoint, 3,
                            static_cast<int64_t>(checkpoint_start_uc_offset));
                        sqlite3_bind_int64(
                            st_checkpoint, 4,
                            static_cast<int64_t>(checkpoint_uc_size));
                        sqlite3_bind_int64(
                            st_checkpoint, 5,
                            static_cast<int64_t>(checkpoint_data.c_offset));
                        sqlite3_bind_int64(
                            st_checkpoint, 6,
                            static_cast<int64_t>(checkpoint_data.c_offset));
                        sqlite3_bind_int(st_checkpoint, 7,
                                         checkpoint_data.bits);
                        sqlite3_bind_blob(
                            st_checkpoint, 8, compressed_dict,
                            static_cast<int>(compressed_dict_size),
                            SQLITE_TRANSIENT);
                        sqlite3_bind_int64(
                            st_checkpoint, 9,
                            static_cast<int64_t>(0));  // Approximate line count
                        sqlite3_step(st_checkpoint);
                        sqlite3_reset(st_checkpoint);

                        DFTRACER_UTILS_LOG_DEBUG(
                            "Created final checkpoint %zu: uc_offset=%zu, "
                            "size=%zu bytes",
                            checkpoint_idx, checkpoint_start_uc_offset,
                            checkpoint_uc_size);

                        free(compressed_dict);
                    }
                }
            }
            checkpoint_idx++;
        }

        total_lines_out = total_lines;
        total_uc_size_out = current_uc_offset;
        DFTRACER_UTILS_LOG_DEBUG(
            "Indexing complete: created %zu checkpoints, %llu total lines, %zu "
            "total "
            "UC bytes",
            checkpoint_idx, total_lines, current_uc_offset);
        return 0;
    } catch (const std::exception &e) {
        DFTRACER_UTILS_LOG_ERROR("Error during checkpoint processing: %s",
                                 e.what());
        return -3;
    }
}

bool Indexer::Impl::need_rebuild() const {
    // Check if index exists and is valid
    if (!index_exists_and_valid(idx_path_)) {
        DFTRACER_UTILS_LOG_INFO(
            "Index rebuild needed: index does not exist or is invalid in %s",
            idx_path_.c_str());
        return true;
    }

    // Check if checkpoint size differs
    // size_t existing_ckpt_size = get_existing_ckpt_size(idx_path_);
    // if (existing_ckpt_size > 0)
    // {
    //     size_t diff = std::abs(existing_ckpt_size - ckpt_size_);
    //     if (diff > 0.1)
    //     {
    //         // Allow small floating point differences
    //         DFTRACER_UTILS_LOG_DEBUG("Index rebuild needed: checkpoint size
    //         differs (%zu bytes vs %zu bytes)",
    //                       existing_ckpt_size, ckpt_size_);
    //         return true;
    //     }
    // }

    // Check if file content has changed using SHA256
    std::string stored_sha256;
    time_t stored_mtime;
    if (get_stored_file_info(idx_path_, gz_path_logical_path_, stored_sha256,
                             stored_mtime)) {
        // quick check using modification time as optimization
        // time_t current_mtime = get_file_mtime(indexer->gz_path);
        // if (current_mtime != stored_mtime && current_mtime > 0 &&
        // stored_mtime > 0)
        // {
        //     DFTRACER_UTILS_LOG_DEBUG("Index rebuild needed: file modification
        //     time changed"); return 1;
        // }

        // If we have a stored SHA256, calculate current SHA256 and compare
        if (!stored_sha256.empty()) {
            std::string current_sha256 = calculate_file_sha256(gz_path_);
            if (current_sha256.empty()) {
                throw Indexer::Error(
                    Indexer::Error::FILE_ERROR,
                    "Failed to calculate SHA256 for " + gz_path_);
            }

            if (current_sha256 != stored_sha256) {
                DFTRACER_UTILS_LOG_INFO(
                    "Index rebuild needed: file SHA256 changed (%s vs %s)",
                    (current_sha256.substr(0, 16) + "...").c_str(),
                    (stored_sha256.substr(0, 16) + "...").c_str());
                return true;
            }
        } else {
            // No stored SHA256, this might be an old index format
            DFTRACER_UTILS_LOG_INFO(
                "Index rebuild needed: no SHA256 stored in index (old format)");
            return true;
        }
    } else {
        // Could not get stored file info, assume rebuild needed
        DFTRACER_UTILS_LOG_INFO(
            "Index rebuild needed: could not retrieve stored file information "
            "from "
            "%s",
            idx_path_.c_str());
        return true;
    }

    DFTRACER_UTILS_LOG_DEBUG(
        "Index rebuild not needed: file content unchanged");
    return false;
}

int Indexer::Impl::init_schema(sqlite3 *db) const {
    char *errmsg = NULL;
    int rc = sqlite3_exec(db, constants::SQL_SCHEMA, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        std::string error =
            "Failed to initialize database schema: " + std::string(errmsg);
        sqlite3_free(errmsg);
        throw Indexer::Error(Indexer::Error::DATABASE_ERROR, error);
    }
    DFTRACER_UTILS_LOG_DEBUG("Schema init succeeded");
    return rc;
}

int Indexer::Impl::create_checkpoint(Inflater &inflater,
                                     CheckpointData *checkpoint,
                                     size_t uc_offset) const {
    checkpoint->uc_offset = uc_offset;

    // Get precise compressed position: file position minus unprocessed input
    size_t file_pos = static_cast<size_t>(ftello(inflater.file));
    size_t absolute_c_offset = file_pos - inflater.stream.avail_in;

    // Store absolute file position (as in original zran)
    checkpoint->c_offset = absolute_c_offset;

    // Get bit offset from zlib state (following zran approach)
    checkpoint->bits = inflater.stream.data_type & 7;

    // Try to get the sliding window dictionary from zlib
    // This contains the last 32KB of uncompressed data
    // Only attempt this when the zlib state is stable
    unsigned have = 0;
    if ((inflater.stream.data_type & 0xc0) == 0x80 &&
        inflater.stream.avail_out == 0 &&
        inflateGetDictionary(&inflater.stream, checkpoint->window, &have) ==
            Z_OK &&
        have > 0) {
        // Got dictionary successfully
        if (have < indexer::constants::ZLIB_WINDOW_SIZE) {
            // If less than 32KB available, right-align and pad with zeros
            memmove(checkpoint->window +
                        (indexer::constants::ZLIB_WINDOW_SIZE - have),
                    checkpoint->window, have);
            memset(checkpoint->window, 0,
                   indexer::constants::ZLIB_WINDOW_SIZE - have);
        }

        DFTRACER_UTILS_LOG_DEBUG(
            "Created checkpoint: uc_offset=%zu, c_offset=%zu, bits=%d, "
            "dict_size=%u",
            uc_offset, checkpoint->c_offset, checkpoint->bits, have);
        return 0;
    }

    // If we can't get dictionary from zlib, this checkpoint won't work
    DFTRACER_UTILS_LOG_DEBUG(
        "Could not get dictionary for checkpoint at offset %zu", uc_offset);
    return -1;
}

int Indexer::Impl::compress_window(const unsigned char *window,
                                   size_t window_size,
                                   unsigned char **compressed,
                                   size_t *compressed_size) const {
    z_stream zs;
    memset(&zs, 0, sizeof(zs));

    if (deflateInit(&zs, Z_BEST_COMPRESSION) != Z_OK) {
        return -1;
    }

    size_t max_compressed = deflateBound(&zs, window_size);
    *compressed = static_cast<unsigned char *>(malloc(max_compressed));
    if (!*compressed) {
        deflateEnd(&zs);
        return -1;
    }

    zs.next_in = const_cast<unsigned char *>(window);
    zs.avail_in = static_cast<uInt>(window_size);
    zs.next_out = *compressed;
    zs.avail_out = static_cast<uInt>(max_compressed);

    int ret = deflate(&zs, Z_FINISH);
    if (ret != Z_STREAM_END) {
        free(*compressed);
        deflateEnd(&zs);
        return -1;
    }

    *compressed_size = max_compressed - zs.avail_out;
    deflateEnd(&zs);
    return 0;
}

int Indexer::Impl::save_checkpoint(sqlite3 *db, int file_id,
                                   const CheckpointData *checkpoint) const {
    unsigned char *compressed_window;
    size_t compressed_size;

    if (compress_window(checkpoint->window, 32768, &compressed_window,
                        &compressed_size) != 0) {
        DFTRACER_UTILS_LOG_DEBUG("Failed to compress window for checkpoint");
        return -1;
    }

    sqlite3_stmt *stmt;
    const char *sql =
        "INSERT INTO checkpoints(file_id, uc_offset, c_offset, bits, "
        "dict_compressed) VALUES(?, ?, ?, ?, ?)";

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        DFTRACER_UTILS_LOG_DEBUG("Failed to prepare checkpoint insert: %s",
                                 sqlite3_errmsg(db));
        free(compressed_window);
        return -1;
    }

    sqlite3_bind_int(stmt, 1, file_id);
    sqlite3_bind_int64(stmt, 2,
                       static_cast<sqlite3_int64>(checkpoint->uc_offset));
    sqlite3_bind_int64(stmt, 3,
                       static_cast<sqlite3_int64>(checkpoint->c_offset));
    sqlite3_bind_int(stmt, 4, checkpoint->bits);
    sqlite3_bind_blob(stmt, 5, compressed_window,
                      static_cast<int>(compressed_size), SQLITE_TRANSIENT);

    int ret = sqlite3_step(stmt);
    if (ret != SQLITE_DONE) {
        DFTRACER_UTILS_LOG_DEBUG("Failed to insert checkpoint: %d - %s", ret,
                                 sqlite3_errmsg(db));
    } else {
        DFTRACER_UTILS_LOG_DEBUG(
            "Successfully inserted checkpoint into database: uc_offset=%zu",
            checkpoint->uc_offset);
    }

    sqlite3_finalize(stmt);
    free(compressed_window);

    return (ret == SQLITE_DONE) ? 0 : -1;
}

int Indexer::Impl::build_index_internal(sqlite3 *db, int file_id,
                                        const std::string &gz_path,
                                        size_t ckpt_size) const {
    FILE *fp = fopen(gz_path.c_str(), "rb");
    if (!fp) return -1;

    sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);

    // Clean up existing data for this file before rebuilding
    if (cleanup_existing_data(db, file_id) != 0) {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }

    // Process chunks and get total line count and uncompressed size
    uint64_t total_lines = 0;
    uint64_t total_uc_size = 0;
    int result =
        process_chunks(fp, db, file_id, ckpt_size, total_lines, total_uc_size);
    fclose(fp);

    if (result != 0) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return result;
    }

    // Insert metadata with total_lines and total_uc_size
    if (insert_metadata(db, file_id, ckpt_size, total_lines, total_uc_size) !=
        0) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }

    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    return 0;
}

uint64_t Indexer::Impl::get_max_bytes() const {
    if (!index_exists_and_valid(idx_path_)) {
        return 0;
    }

    sqlite3 *db;
    if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
        throw Indexer::Error(
            Indexer::Error::DATABASE_ERROR,
            "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
    }

    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT MAX(uc_offset + uc_size) FROM checkpoints WHERE file_id = "
        "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)";
    uint64_t max_bytes = 0;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, gz_path_logical_path_.c_str(), -1,
                          SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            max_bytes = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    // If no checkpoints exist (max_bytes is 0), fall back to metadata table
    if (max_bytes == 0) {
        const char *metadata_sql =
            "SELECT total_uc_size FROM metadata WHERE file_id = "
            "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)";
        if (sqlite3_prepare_v2(db, metadata_sql, -1, &stmt, nullptr) ==
            SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, gz_path_logical_path_.c_str(), -1,
                              SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                max_bytes =
                    static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
                DFTRACER_UTILS_LOG_DEBUG(
                    "No checkpoints found, using metadata total_uc_size: %llu",
                    max_bytes);
            }
            sqlite3_finalize(stmt);
        }
    }

    sqlite3_close(db);
    return max_bytes;
}

uint64_t Indexer::Impl::get_num_lines() const {
    if (!index_exists_and_valid(idx_path_)) {
        return 0;
    }

    sqlite3 *db;
    if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
        throw Indexer::Error(
            Indexer::Error::DATABASE_ERROR,
            "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
    }

    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT total_lines FROM metadata WHERE file_id = "
        "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)";
    uint64_t total_lines = 0;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, gz_path_logical_path_.c_str(), -1,
                          SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            total_lines = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    return total_lines;
}

int Indexer::Impl::find_file_id(const std::string &gz_path) const {
    if (!index_exists_and_valid(idx_path_)) {
        return -1;
    }

    sqlite3 *db;
    if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
        throw Indexer::Error(
            Indexer::Error::DATABASE_ERROR,
            "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
    }

    sqlite3_stmt *stmt;
    const char *sql = "SELECT id FROM files WHERE logical_name = ? LIMIT 1";
    int file_id = -1;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        std::string logical_path = get_logical_path(gz_path);
        sqlite3_bind_text(stmt, 1, logical_path.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            file_id = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    } else {
        sqlite3_close(db);
        throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                             "Failed to prepare find_file_id statement: " +
                                 std::string(sqlite3_errmsg(db)));
    }

    sqlite3_close(db);
    return file_id;
}

bool Indexer::Impl::find_checkpoint(size_t target_offset,
                                    Checkpoint &checkpoint) const {
    if (!index_exists_and_valid(idx_path_)) {
        return false;
    }

    // For target offset 0, always decompress from beginning of file (no
    // checkpoint)
    if (target_offset == 0) {
        return false;
    }

    int file_id = get_file_id();
    if (file_id == -1) {
        return false;
    }

    sqlite3 *db;
    if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
        throw Indexer::Error(
            Indexer::Error::DATABASE_ERROR,
            "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
    }

    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT checkpoint_idx, uc_offset, uc_size, c_offset, c_size, bits, "
        "dict_compressed, num_lines "
        "FROM checkpoints WHERE file_id = ? AND uc_offset <= ? "
        "ORDER BY uc_offset DESC LIMIT 1";
    bool found = false;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, file_id);
        sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(target_offset));

        if (sqlite3_step(stmt) == SQLITE_ROW) {
            checkpoint.checkpoint_idx =
                static_cast<size_t>(sqlite3_column_int64(stmt, 0));
            checkpoint.uc_offset =
                static_cast<size_t>(sqlite3_column_int64(stmt, 1));
            checkpoint.uc_size =
                static_cast<size_t>(sqlite3_column_int64(stmt, 2));
            checkpoint.c_offset =
                static_cast<size_t>(sqlite3_column_int64(stmt, 3));
            checkpoint.c_size =
                static_cast<size_t>(sqlite3_column_int64(stmt, 4));
            checkpoint.bits = sqlite3_column_int(stmt, 5);

            size_t dict_size =
                static_cast<size_t>(sqlite3_column_bytes(stmt, 6));
            checkpoint.dict_compressed.resize(dict_size);
            std::memcpy(checkpoint.dict_compressed.data(),
                        sqlite3_column_blob(stmt, 6), dict_size);

            checkpoint.num_lines =
                static_cast<size_t>(sqlite3_column_int64(stmt, 7));

            found = true;
        }
        sqlite3_finalize(stmt);
    } else {
        sqlite3_close(db);
        throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                             "Failed to prepare find_checkpoint statement: " +
                                 std::string(sqlite3_errmsg(db)));
    }

    sqlite3_close(db);
    return found;
}

std::vector<Checkpoint> Indexer::Impl::get_checkpoints() const {
    std::vector<Checkpoint> checkpoints;

    if (!index_exists_and_valid(idx_path_)) {
        return checkpoints;
    }

    sqlite3 *db;
    if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
        throw Indexer::Error(
            Indexer::Error::DATABASE_ERROR,
            "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
    }

    sqlite3_stmt *stmt;
    // Query unified checkpoints table
    const char *sql =
        "SELECT checkpoint_idx, uc_offset, uc_size, c_offset, c_size, bits, "
        "dict_compressed, num_lines "
        "FROM checkpoints WHERE file_id = ? ORDER BY uc_offset";

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, get_file_id());

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            Checkpoint checkpoint;
            checkpoint.checkpoint_idx =
                static_cast<size_t>(sqlite3_column_int64(stmt, 0));
            checkpoint.uc_offset =
                static_cast<size_t>(sqlite3_column_int64(stmt, 1));
            checkpoint.uc_size =
                static_cast<size_t>(sqlite3_column_int64(stmt, 2));
            checkpoint.c_offset =
                static_cast<size_t>(sqlite3_column_int64(stmt, 3));
            checkpoint.c_size =
                static_cast<size_t>(sqlite3_column_int64(stmt, 4));
            checkpoint.bits = sqlite3_column_int(stmt, 5);

            size_t dict_size =
                static_cast<size_t>(sqlite3_column_bytes(stmt, 6));
            checkpoint.dict_compressed.resize(dict_size);
            std::memcpy(checkpoint.dict_compressed.data(),
                        sqlite3_column_blob(stmt, 6), dict_size);

            checkpoint.num_lines =
                static_cast<size_t>(sqlite3_column_int64(stmt, 7));

            checkpoints.push_back(std::move(checkpoint));
        }

        sqlite3_finalize(stmt);
    } else {
        sqlite3_close(db);
        throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                             "Failed to prepare get_checkpoints statement: " +
                                 std::string(sqlite3_errmsg(db)));
    }

    sqlite3_close(db);
    return checkpoints;
}

int Indexer::Impl::get_file_id() const {
    if (cached_file_id_ != -1) {
        return cached_file_id_;
    }

    cached_file_id_ = find_file_id(gz_path_);
    return cached_file_id_;
}

std::vector<Checkpoint> Indexer::Impl::find_checkpoints_by_line_range(
    size_t start_line, size_t end_line) const {
    std::vector<Checkpoint> checkpoints;

    if (!index_exists_and_valid(idx_path_)) {
        return checkpoints;
    }

    if (start_line == 0 || end_line == 0 || start_line > end_line) {
        throw Indexer::Error(
            Indexer::Error::INVALID_ARGUMENT,
            "Invalid line range: start_line and end_line must be > 0 and "
            "start_line <= end_line");
    }

    // For line-based reading, we need to start from the beginning and
    // decompress sequentially Use the original approach from reader.cpp that
    // handles line counting during decompression

    // For now, return all checkpoints in order - the reader will handle line
    // counting
    return get_checkpoints();
}

void Indexer::Impl::build() {
    DFTRACER_UTILS_LOG_DEBUG(
        "Building index for %s with %zu bytes (%.1f MB) chunks...",
        gz_path_.c_str(), ckpt_size_,
        static_cast<double>(ckpt_size_) / (1024 * 1024));

    // If force rebuild is enabled, delete the existing database file to ensure
    // clean schema
    if (force_rebuild_ && fs::exists(idx_path_)) {
        DFTRACER_UTILS_LOG_DEBUG(
            "Force rebuild enabled, removing existing index file: %s",
            idx_path_.c_str());
        if (!fs::remove(idx_path_)) {
            DFTRACER_UTILS_LOG_WARN("Failed to remove existing index file: %s",
                                    idx_path_.c_str());
        }
    }

    // open database
    if (sqlite3_open(idx_path_.c_str(), &db_) != SQLITE_OK) {
        throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                             "Cannot create/open database " + idx_path_ + ": " +
                                 sqlite3_errmsg(db_));
    }
    db_opened_ = true;

    // initialize schema
    if (init_schema(db_) != SQLITE_OK) {
        throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                             "Failed to initialize database schema");
    }

    // get file info
    uint64_t bytes = file_size_bytes(gz_path_);
    if (bytes == UINT64_MAX) {
        throw Indexer::Error(Indexer::Error::FILE_ERROR,
                             "Cannot stat " + gz_path_);
    }

    // calculate SHA256 and get modification time
    std::string file_sha256 = calculate_file_sha256(gz_path_);
    if (file_sha256.empty()) {
        throw Indexer::Error(Indexer::Error::FILE_ERROR,
                             "Failed to calculate SHA256 for " + gz_path_);
    }

    time_t file_mtime = get_file_mtime(gz_path_);

    DFTRACER_UTILS_LOG_DEBUG(
        "File info: size=%llu bytes, mtime=%ld, sha256=%s...", bytes,
        file_mtime, file_sha256.substr(0, 16).c_str());

    // insert/update file record
    // @todo: change this to insert_file_record
    // sqlite3_stmt *st;
    // if (sqlite3_prepare_v2(db_,
    //                        "INSERT INTO files(logical_name, byte_size, "
    //                        "mtime_unix, sha256_hex) "
    //                        "VALUES(?, ?, ?, ?) "
    //                        "ON CONFLICT(logical_name) DO UPDATE SET "
    //                        "byte_size=excluded.byte_size, "
    //                        "mtime_unix=excluded.mtime_unix, "
    //                        "sha256_hex=excluded.sha256_hex "
    //                        "RETURNING id;",
    //                        -1, &st, NULL) != SQLITE_OK) {
    //     throw Indexer::Error(
    //         Indexer::Error::DATABASE_ERROR,
    //         "Prepare failed: " + std::string(sqlite3_errmsg(db_)));
    // }

    // sqlite3_bind_text(st, 1, gz_path_logical_path_.c_str(), -1,
    //                   SQLITE_TRANSIENT);
    // sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(bytes));
    // sqlite3_bind_int64(st, 3, static_cast<sqlite3_int64>(file_mtime));
    // sqlite3_bind_text(st, 4, file_sha256.c_str(), -1, SQLITE_TRANSIENT);

    // int rc = sqlite3_step(st);
    // if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    //     sqlite3_finalize(st);
    //     throw Indexer::Error(
    //         Indexer::Error::DATABASE_ERROR,
    //         "Insert failed: " + std::string(sqlite3_errmsg(db_)));
    // }

    // int db_file_id = sqlite3_column_int(st, 0);
    // sqlite3_finalize(st);

    DFTRACER_UTILS_LOG_DEBUG("Building index with stride: %zu bytes (%.1f MB)",
                             ckpt_size_,
                             static_cast<double>(ckpt_size_) / (1024 * 1024));

    int ret = build_index_internal(db_, db_file_id, gz_path_, ckpt_size_);
    if (ret != 0) {
        throw Indexer::Error(
            Indexer::Error::BUILD_ERROR,
            "Index build failed with error code: " + std::to_string(ret));
    }

    DFTRACER_UTILS_LOG_DEBUG("Index built successfully for %s",
                             gz_path_.c_str());
}

}  // namespace dftracer::utils

// ==============================================================================
// C++ Public Interface Implementation
// ==============================================================================

namespace dftracer::utils::indexer {

Indexer::Indexer(const std::string &gz_path, const std::string &idx_path,
                 size_t ckpt_size, bool force_rebuild)
    : pImpl_(new Impl(gz_path, idx_path, ckpt_size, force_rebuild)) {}

Indexer::~Indexer() = default;

Indexer::Indexer(Indexer &&other) noexcept : pImpl_(other.pImpl_.release()) {}

Indexer &Indexer::operator=(Indexer &&other) noexcept {
    if (this != &other) {
        pImpl_.reset(other.pImpl_.release());
    }
    return *this;
}

void Indexer::build() { pImpl_->build(); }

bool Indexer::need_rebuild() const { return pImpl_->need_rebuild(); }

bool Indexer::is_valid() const { return pImpl_ && pImpl_->is_valid(); }

const std::string &Indexer::get_gz_path() const {
    return pImpl_->get_gz_path();
}

const std::string &Indexer::get_idx_path() const {
    return pImpl_->get_idx_path();
}

size_t Indexer::get_checkpoint_size() const {
    return pImpl_->get_checkpoint_size();
}

uint64_t Indexer::get_max_bytes() const { return pImpl_->get_max_bytes(); }

uint64_t Indexer::get_num_lines() const { return pImpl_->get_num_lines(); }

int Indexer::find_file_id(const std::string &gz_path) const {
    return pImpl_->find_file_id(gz_path);
}

bool Indexer::find_checkpoint(size_t target_offset,
                              IndexCheckpoint &checkpoint) const {
    return pImpl_->find_checkpoint(target_offset, checkpoint);
}

std::vector<IndexCheckpoint> Indexer::get_checkpoints() const {
    return pImpl_->get_checkpoints();
}

std::vector<IndexCheckpoint> Indexer::find_checkpoints_by_line_range(
    size_t start_line, size_t end_line) const {
    return pImpl_->find_checkpoints_by_line_range(start_line, end_line);
}

// ==============================================================================
// Error Class Implementation
// ==============================================================================

}  // namespace dftracer::utils::indexer
