#include <dftracer/utils/common/checkpointer.h>
#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/checkpoint_size.h>
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/helpers.h>
#include <dftracer/utils/indexer/indexer_impl.h>
#include <dftracer/utils/indexer/inflater.h>
#include <dftracer/utils/indexer/queries/queries.h>

namespace dftracer::utils {

static void init_schema(const SqliteDatabase &db) {
    char *errmsg = NULL;
    int rc = sqlite3_exec(db.get(), constants::indexer::SQL_SCHEMA, NULL, NULL,
                          &errmsg);
    if (rc != SQLITE_OK) {
        std::string error =
            "Failed to initialize database schema: " + std::string(errmsg);
        sqlite3_free(errmsg);
        throw IndexerError(IndexerError::DATABASE_ERROR, error);
    }
    DFTRACER_UTILS_LOG_DEBUG("Schema init succeeded", "");
}

static bool process_chunks(FILE *fp, const SqliteDatabase &db, int file_id,
                           std::size_t checkpoint_size,
                           std::uint64_t &total_lines_out,
                           std::uint64_t &total_uc_size_out) {
    if (fseeko(fp, 0, SEEK_SET) != 0) {
        DFTRACER_UTILS_LOG_INFO("Failed to seek to beginning of file", "");
        return false;
    }

    DFTRACER_UTILS_LOG_DEBUG("Starting to process chunks", "");

    IndexerInflater inflater;
    if (!inflater.initialize(fp, 0,
                             constants::indexer::ZLIB_GZIP_WINDOW_BITS)) {
        DFTRACER_UTILS_LOG_DEBUG("Failed to initialize inflater", "");
        return false;
    }

    std::size_t checkpoint_idx = 0;
    std::size_t current_uc_offset = 0;
    std::uint64_t total_lines = 0;
    std::uint64_t current_line_number = 1;  // 1-based line numbering
    std::size_t last_checkpoint_uc_offset = 0;
    std::uint64_t last_checkpoint_line_number = 1;

    while (true) {
        // Check if we're at proper block boundary using the new API
        bool at_block_boundary = inflater.is_at_checkpoint_boundary();
        bool need_checkpoint =
            (checkpoint_idx == 0) ||
            (current_uc_offset - last_checkpoint_uc_offset >= checkpoint_size);

        if (at_block_boundary && need_checkpoint) {
            std::size_t input_pos = inflater.get_total_input_consumed();

            Checkpointer checkpointer(inflater, current_uc_offset);

            if (checkpointer.create(input_pos)) {
                unsigned char *compressed_dict = nullptr;
                std::size_t compressed_dict_size = 0;

                if (!checkpointer.compress(&compressed_dict,
                                           &compressed_dict_size) ||
                    !compressed_dict) {
                    DFTRACER_UTILS_LOG_DEBUG(
                        "Failed to compress dictionary for checkpoint at "
                        "uc_offset=%zu",
                        current_uc_offset);
                    continue;  // Skip this checkpoint if compression fails
                }

                InsertCheckpointData ckpt_data;
                ckpt_data.idx = checkpoint_idx;
                ckpt_data.uc_offset = current_uc_offset;
                ckpt_data.uc_size = 0;  // Will be calculated later
                ckpt_data.c_offset = input_pos;
                ckpt_data.c_size = 0;  // Will be calculated later
                ckpt_data.bits = checkpointer.bits;
                ckpt_data.compressed_dict = compressed_dict;
                ckpt_data.compressed_dict_size = compressed_dict_size;
                ckpt_data.first_line_num = last_checkpoint_line_number;
                ckpt_data.last_line_num =
                    current_line_number -
                    1;  // Current line is the end of this checkpoint
                ckpt_data.num_lines = (current_line_number - 1) -
                                      last_checkpoint_line_number +
                                      1;  // Number of lines in this checkpoint

                insert_checkpoint_record(db, file_id, ckpt_data);

                if (compressed_dict) free(compressed_dict);
                last_checkpoint_uc_offset = current_uc_offset;
                last_checkpoint_line_number = current_line_number;
                checkpoint_idx++;

                DFTRACER_UTILS_LOG_DEBUG(
                    "Checkpoint %zu created at uc_offset=%zu, c_offset=%zu, "
                    "bits=%d",
                    checkpoint_idx - 1, current_uc_offset, input_pos,
                    checkpointer.bits);
            }
        }

        // Now read and process data using the new IndexerInflater API
        IndexerInflaterResult result;
        if (!inflater.read(fp, result)) {
            DFTRACER_UTILS_LOG_DEBUG("Inflater read failed", "");
            break;
        }

        if (result.bytes_read == 0) {
            DFTRACER_UTILS_LOG_DEBUG("End of file reached", "");
            break;
        }

        total_lines += result.lines_found;
        current_line_number += result.lines_found;
        current_uc_offset += result.bytes_read;
    }

    // Create final checkpoint if needed (following zran approach)
    if (current_uc_offset > last_checkpoint_uc_offset) {
        std::size_t input_pos = inflater.get_total_input_consumed();

        // Create proper final checkpoint with dictionary (like zran does)
        Checkpointer final_checkpointer(inflater, current_uc_offset);

        if (final_checkpointer.create(input_pos)) {
            unsigned char *compressed_dict = nullptr;
            std::size_t compressed_dict_size = 0;

            if (final_checkpointer.compress(&compressed_dict,
                                            &compressed_dict_size) &&
                compressed_dict) {
                InsertCheckpointData ckpt_data;
                ckpt_data.idx = checkpoint_idx;
                ckpt_data.uc_offset =
                    current_uc_offset;  // Use current_uc_offset for final
                                        // checkpoint
                ckpt_data.uc_size = 0;
                ckpt_data.c_offset = input_pos;
                ckpt_data.c_size = 0;
                ckpt_data.bits = final_checkpointer.bits;
                ckpt_data.compressed_dict = compressed_dict;
                ckpt_data.compressed_dict_size = compressed_dict_size;
                ckpt_data.first_line_num = last_checkpoint_line_number;
                ckpt_data.last_line_num =
                    current_line_number - 1;  // -1 because current_line_number
                                              // is next line to be processed
                ckpt_data.num_lines =
                    (current_line_number - 1) - last_checkpoint_line_number +
                    1;  // Number of lines in this final checkpoint

                insert_checkpoint_record(db, file_id, ckpt_data);

                if (compressed_dict) free(compressed_dict);
                checkpoint_idx++;

                DFTRACER_UTILS_LOG_DEBUG(
                    "Final checkpoint %zu created at uc_offset=%zu, "
                    "c_offset=%zu",
                    checkpoint_idx - 1, current_uc_offset, input_pos);
            } else {
                DFTRACER_UTILS_LOG_DEBUG(
                    "Failed to create final checkpoint - dictionary "
                    "compression failed",
                    "");
            }
        } else {
            DFTRACER_UTILS_LOG_DEBUG(
                "Failed to create final checkpoint - not at valid block "
                "boundary",
                "");
        }
    }

    total_lines_out = total_lines;
    total_uc_size_out = current_uc_offset;

    DFTRACER_UTILS_LOG_DEBUG(
        "Indexing complete: created %zu checkpoints, %llu total lines, %zu "
        "total "
        "UC bytes",
        checkpoint_idx, total_lines, current_uc_offset);

    return true;
}

static bool build_index(const SqliteDatabase &db, int file_id,
                        const std::string &gz_path, size_t ckpt_size) {
    FILE *fp = fopen(gz_path.c_str(), "rb");
    if (!fp) return false;

    sqlite3_exec(db.get(), "BEGIN IMMEDIATE;", NULL, NULL, NULL);

    try {
        if (!delete_file_record(db, file_id)) {
            DFTRACER_UTILS_LOG_DEBUG("Failed to delete existing file record",
                                     "");
            goto failure;
        }

        // Process chunks and get total line count and uncompressed size
        uint64_t total_lines = 0;
        uint64_t total_uc_size = 0;
        bool result = process_chunks(fp, db, file_id, ckpt_size, total_lines,
                                     total_uc_size);
        if (!result) goto failure;
        insert_file_metadata_record(db, file_id, ckpt_size, total_lines,
                                    total_uc_size);
    } catch (const std::exception &e) {
        DFTRACER_UTILS_LOG_DEBUG("[BUILD_INDEX] Exception occurred: %s",
                                 e.what());
        goto failure;
    }

    fclose(fp);
    sqlite3_exec(db.get(), "COMMIT;", NULL, NULL, NULL);
    return true;

failure:
    fclose(fp);
    sqlite3_exec(db.get(), "ROLLBACK;", NULL, NULL, NULL);
    return false;
}

IndexerImplementor::IndexerImplementor(const std::string &gz_path_,
                                       const std::string &idx_path_,
                                       std::size_t ckpt_size_, bool force_)
    : gz_path(gz_path_),
      gz_path_logical_path(get_logical_path(gz_path)),
      idx_path(idx_path_),
      ckpt_size(ckpt_size_),
      force_rebuild(force_),
      cached_is_valid(false),
      cached_file_id(-1) {
    if (gz_path.empty()) {
        throw IndexerError(IndexerError::Type::INVALID_ARGUMENT,
                           "gz_path must not be empty");
    }

    if (!fs::exists(gz_path)) {
        throw IndexerError(IndexerError::Type::FILE_ERROR,
                           "gz_path does not exist: " + gz_path);
    }

    if (ckpt_size == 0) {
        throw IndexerError(IndexerError::Type::INVALID_ARGUMENT,
                           "ckpt_size must be greater than 0");
    }

    open();
}

void IndexerImplementor::open() {
    if (db.is_open()) {
        return;
    }
    if (!db.open(idx_path)) {
        throw IndexerError(IndexerError::Type::DATABASE_ERROR,
                           "Failed to open database: " + idx_path);
    }
}

void IndexerImplementor::close() { db.close(); }

void IndexerImplementor::build() {
    if (!force_rebuild && index_exists_and_valid(idx_path)) {
        open();
        if (query_schema_validity(db)) {
            DFTRACER_UTILS_LOG_DEBUG(
                "Index is already valid, skipping rebuild.", "");
            cached_is_valid = true;
            return;
        } else {
            DFTRACER_UTILS_LOG_DEBUG(
                "Index file exists but schema is invalid, rebuilding.", "");
        }
    }

    // Now calculate optimal checkpoint size for building the index
    auto new_ckpt_size = determine_checkpoint_size(ckpt_size, gz_path);
    if (new_ckpt_size != ckpt_size) {
        DFTRACER_UTILS_LOG_DEBUG("Adjusted checkpoint size from %zu to %zu",
                                 ckpt_size, new_ckpt_size);
        ckpt_size = new_ckpt_size;
    }

    DFTRACER_UTILS_LOG_DEBUG(
        "Building index for %s with %zu bytes (%.1f MB) chunks...",
        gz_path.c_str(), ckpt_size,
        static_cast<double>(ckpt_size) / (1024 * 1024));

    if (force_rebuild && fs::exists(idx_path)) {
        DFTRACER_UTILS_LOG_DEBUG(
            "Force rebuild enabled, removing existing index file: %s",
            idx_path.c_str());
        close();  // Ensure database is closed before removing file
        if (!fs::remove(idx_path)) {
            DFTRACER_UTILS_LOG_WARN("Failed to remove existing index file: %s",
                                    idx_path.c_str());
        }
    }

    open();
    init_schema(db);

    const auto bytes = file_size_bytes(gz_path);
    if (bytes == 0) {
        throw IndexerError(
            IndexerError::Type::FILE_ERROR,
            "Failed to get file size for: " + gz_path + ", got size: 0");
    }

    std::string hash = calculate_file_hash(gz_path);
    if (hash.empty()) {
        throw IndexerError(IndexerError::Type::FILE_ERROR,
                           "Failed to calculate hash for: " + gz_path);
    }

    time_t mod_time = get_file_modification_time(gz_path);
    insert_file_record(db, gz_path_logical_path, bytes, mod_time, hash,
                       cached_file_id);

    if (cached_file_id == -1) {
        throw IndexerError(IndexerError::Type::DATABASE_ERROR,
                           "Failed to retrieve file ID after insertion");
    }

    if (!build_index(db, cached_file_id, gz_path, ckpt_size)) {
        throw IndexerError(IndexerError::Type::BUILD_ERROR,
                           "Index build failed for: " + gz_path);
    }

    if (!index_exists_and_valid(idx_path)) {
        throw IndexerError(
            IndexerError::Type::BUILD_ERROR,
            "Index build completed but index is invalid: " + idx_path);
    }
    cached_is_valid = true;
    DFTRACER_UTILS_LOG_DEBUG("Index build completed successfully: %s",
                             idx_path.c_str());
}

bool IndexerImplementor::is_valid() const { return cached_is_valid; }

bool IndexerImplementor::exists() const { return fs::exists(idx_path); }

bool IndexerImplementor::need_rebuild() const {
    if (is_valid()) return false;
    if (!query_schema_validity(db)) {
        DFTRACER_UTILS_LOG_WARN("Index schema is invalid, rebuilding index.");
        return true;
    }

    std::string stored_hash;
    time_t stored_mtime;
    if (query_stored_file_info(db, gz_path_logical_path, stored_hash,
                               stored_mtime)) {
        // quick check using modification time as optimization
        // time_t current_mtime = get_file_mtime(indexer->gz_path);
        // if (current_mtime != stored_mtime && current_mtime > 0 &&
        // stored_mtime > 0)
        // {
        //     DFTRACER_UTILS_LOG_DEBUG("Index rebuild needed: file modification
        //     time changed"); return 1;
        // }

        std::string current_hash = calculate_file_hash(gz_path);
        if (current_hash.empty()) {
            throw IndexerError(IndexerError::FILE_ERROR,
                               "Failed to calculate hash for " + gz_path);
        }

        if (current_hash != stored_hash) {
            DFTRACER_UTILS_LOG_DEBUG(
                "Index rebuild needed: file hash changed (%s vs %s)",
                (current_hash.substr(0, 16) + "...").c_str(),
                (stored_hash.substr(0, 16) + "...").c_str());
            return true;
        }
    }

    DFTRACER_UTILS_LOG_DEBUG("Index rebuild not needed: file content unchanged",
                             "");
    return false;
}

std::uint64_t IndexerImplementor::get_max_bytes() const {
    if (cached_max_bytes == 0) {
        cached_max_bytes = query_max_bytes(db, gz_path_logical_path);
    }
    return cached_max_bytes;
}

std::size_t IndexerImplementor::get_checkpoint_size() const {
    if (cached_checkpoint_size == 0) {
        cached_checkpoint_size = query_checkpoint_size(db, cached_file_id);
    }
    return cached_checkpoint_size;
}

std::uint64_t IndexerImplementor::get_num_lines() const {
    if (cached_num_lines == 0) {
        cached_num_lines = query_num_lines(db, gz_path_logical_path);
    }
    return cached_num_lines;
}

int IndexerImplementor::get_file_id() const {
    DFTRACER_UTILS_LOG_DEBUG("get_file_id called: cached_file_id=%d",
                             cached_file_id);
    if (cached_file_id != -1) {
        return cached_file_id;
    }

    cached_file_id = find_file_id(gz_path);
    DFTRACER_UTILS_LOG_DEBUG("get_file_id: found file_id=%d for path=%s",
                             cached_file_id, gz_path.c_str());
    return cached_file_id;
}

int IndexerImplementor::find_file_id(const std::string &path) const {
    return query_file_id(db, get_logical_path(path));
}

bool IndexerImplementor::find_checkpoint(std::size_t target_offset,
                                         IndexCheckpoint &checkpoint) const {
    // Ensure file_id is populated before querying checkpoints
    int file_id = get_file_id();
    return query_checkpoint(db, target_offset, file_id, checkpoint);
}

std::vector<IndexCheckpoint> IndexerImplementor::get_checkpoints() const {
    if (cached_checkpoints.empty()) {
        int file_id = get_file_id();
        cached_checkpoints = query_checkpoints(db, file_id);
    }
    return cached_checkpoints;
}

std::vector<IndexCheckpoint> IndexerImplementor::get_checkpoints_for_line_range(
    std::uint64_t start_line, std::uint64_t end_line) const {
    // Ensure file_id is populated before querying checkpoints
    int file_id = get_file_id();
    DFTRACER_UTILS_LOG_DEBUG(
        "get_checkpoints_for_line_range: file_id=%d, start_line=%llu, "
        "end_line=%llu",
        file_id, start_line, end_line);

    auto checkpoints =
        query_checkpoints_for_line_range(db, file_id, start_line, end_line);
    DFTRACER_UTILS_LOG_DEBUG(
        "get_checkpoints_for_line_range: found %zu checkpoints",
        checkpoints.size());

    return checkpoints;
}

}  // namespace dftracer::utils
