#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/checkpointer.h>
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/helpers.h>
#include <dftracer/utils/indexer/indexer_impl.h>
#include <dftracer/utils/indexer/inflater.h>
#include <dftracer/utils/indexer/queries/queries.h>

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
    DFTRACER_UTILS_LOG_DEBUG("Schema init succeeded");
}

static bool process_chunks(FILE *fp, const SqliteDatabase &db, int file_id,
                           std::size_t checkpoint_size,
                           std::uint64_t &total_lines_out,
                           std::uint64_t &total_uc_size_out) {
    if (fseeko(fp, 0, SEEK_SET) != 0) {
        DFTRACER_UTILS_LOG_DEBUG("Failed to seek to beginning of file");
        return false;
    }

    Inflater inflater(fp);
    std::size_t checkpoint_idx = 0;
    std::size_t current_uc_offset = 0;  // Current uncompressed offset
    std::size_t checkpoint_start_uc_offset =
        0;  // Start of current checkpoint (uncompressed)
    std::uint64_t total_lines = 0;  // Total lines processed

    unsigned char buffer[constants::indexer::PROCESS_BUFFER_SIZE];

    while (true) {
        std::size_t bytes_read;
        std::size_t c_off;

        if (!inflater.process(buffer, sizeof(buffer), &bytes_read, &c_off)) {
            break;
        }

        if (bytes_read == 0) {
            // End of file reached - handle any remaining data
            break;
        }
        std::uint64_t local_total_lines = 0;

        // Count lines in this buffer
        for (std::size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                local_total_lines++;
                total_lines += local_total_lines;
            }
        }

        current_uc_offset += bytes_read;

        std::size_t current_checkpoint_size =
            current_uc_offset - checkpoint_start_uc_offset;
        if (current_checkpoint_size >= checkpoint_size &&
            (inflater.stream.data_type & 0xc0) ==
                0x80 &&                        // At deflate block boundary
            inflater.stream.avail_out == 0) {  // Output buffer is consumed
            // Create checkpoint at current position
            size_t checkpoint_uc_size =
                current_uc_offset - checkpoint_start_uc_offset;
            Checkpointer checkpointer(inflater);
            if (!checkpointer.create()) {
                DFTRACER_UTILS_LOG_DEBUG("Failed to create checkpoint");
                continue;
            }
            unsigned char *compressed_dict = nullptr;
            std::size_t compressed_dict_size = 0;

            if (!checkpointer.compress(&compressed_dict,
                                       &compressed_dict_size)) {
                DFTRACER_UTILS_LOG_DEBUG("Failed to get compressed dictionary");
                continue;
            }

            InsertCheckpointData ckpt_data;
            ckpt_data.idx = checkpoint_idx;
            ckpt_data.uc_offset = current_uc_offset;
            ckpt_data.uc_size = checkpoint_uc_size;
            ckpt_data.c_offset = checkpointer.c_offset;
            ckpt_data.c_size = checkpointer.c_offset;
            ckpt_data.bits = checkpointer.bits;
            ckpt_data.compressed_dict = compressed_dict;
            ckpt_data.compressed_dict_size = compressed_dict_size;
            ckpt_data.num_lines = local_total_lines;

            insert_checkpoint_record(db, file_id, ckpt_data);

            free(compressed_dict);
            checkpoint_idx++;
            checkpoint_start_uc_offset = current_uc_offset;
        }
    }

    // edge case: always create a final checkpoint for remaining data
    if (current_uc_offset > checkpoint_start_uc_offset) {
        size_t checkpoint_uc_size =
            current_uc_offset - checkpoint_start_uc_offset;

        // Create a special checkpoint for remaining data (might be without
        // deflate boundary)
        if (checkpoint_start_uc_offset == 0) {
            InsertCheckpointData ckpt_data;
            ckpt_data.idx = checkpoint_idx;
            ckpt_data.uc_offset = checkpoint_start_uc_offset;
            ckpt_data.uc_size = checkpoint_uc_size;
            ckpt_data.c_offset = 0;
            ckpt_data.c_size = 0;
            ckpt_data.bits = 0;
            ckpt_data.compressed_dict = nullptr;
            ckpt_data.compressed_dict_size = 0;
            ckpt_data.num_lines = total_lines;
            insert_checkpoint_record(db, file_id, ckpt_data);
        } else {
            // create regular checkpoint with dictionary if possible
            Checkpointer checkpointer(inflater);
            if (!checkpointer.create()) {
                DFTRACER_UTILS_LOG_DEBUG("Failed to create final checkpoint");
                return false;
            }

            unsigned char *compressed_dict = nullptr;
            std::size_t compressed_dict_size = 0;

            if (!checkpointer.compress(&compressed_dict,
                                       &compressed_dict_size)) {
                DFTRACER_UTILS_LOG_DEBUG("Failed to get compressed dictionary");
                return false;
            }

            InsertCheckpointData ckpt_data;
            ckpt_data.idx = checkpoint_idx;
            ckpt_data.uc_offset = checkpoint_start_uc_offset;
            ckpt_data.uc_size = checkpoint_uc_size;
            ckpt_data.c_offset = checkpointer.c_offset;
            ckpt_data.c_size = checkpointer.c_offset;
            ckpt_data.bits = checkpointer.bits;
            ckpt_data.compressed_dict = compressed_dict;
            ckpt_data.compressed_dict_size = compressed_dict_size;
            ckpt_data.num_lines = total_lines;
            insert_checkpoint_record(db, file_id, ckpt_data);

            free(compressed_dict);
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
}

static bool build_index(const SqliteDatabase &db, int file_id,
                        const std::string &gz_path, size_t ckpt_size) {
    FILE *fp = fopen(gz_path.c_str(), "rb");
    if (!fp) return false;

    sqlite3_exec(db.get(), "BEGIN IMMEDIATE;", NULL, NULL, NULL);

    try {
        if (delete_file_record(db, file_id) != 0) {
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

namespace dftracer::utils {
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
    DFTRACER_UTILS_LOG_DEBUG("Created DFT indexer for gz: %s and index: %s",
                             gz_path.c_str(), idx_path.c_str());
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

void IndexerImplementor::build() {
    if (!force_rebuild && index_exists_and_valid(idx_path)) {
        DFTRACER_UTILS_LOG_INFO("Index is already valid, skipping rebuild.");
        cached_is_valid = true;
        return;
    }

    DFTRACER_UTILS_LOG_DEBUG(
        "Building index for %s with %zu bytes (%.1f MB) chunks...",
        gz_path_.c_str(), ckpt_size_,
        static_cast<double>(ckpt_size_) / (1024 * 1024));

    if (force_rebuild && fs::exists(idx_path)) {
        DFTRACER_UTILS_LOG_DEBUG(
            "Force rebuild enabled, removing existing index file: %s",
            idx_path.c_str());
        if (!fs::remove(idx_path)) {
            DFTRACER_UTILS_LOG_WARN("Failed to remove existing index file: %s",
                                    idx_path.c_str());
        }
    }

    open();
    init_schema(db);

    std::size_t bytes = file_size_bytes(gz_path);
    if (bytes == static_cast<std::size_t>(-1)) {
        throw IndexerError(IndexerError::Type::FILE_ERROR,
                           "Failed to get file size for: " + gz_path);
    }

    std::string sha256 = calculate_file_sha256(gz_path);
    if (sha256.empty()) {
        throw IndexerError(IndexerError::Type::FILE_ERROR,
                           "Failed to calculate SHA256 for: " + gz_path);
    }

    time_t mod_time = get_file_modification_time(gz_path);
    insert_file_record(db, gz_path_logical_path, bytes, mod_time, sha256,
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

bool IndexerImplementor::need_rebuild() const {
    if (is_valid()) return false;
    if (!query_schema_validity(db)) {
        DFTRACER_UTILS_LOG_WARN("Index schema is invalid, rebuilding index.");
        return true;
    }

    std::string stored_sha256;
    time_t stored_mtime;
    if (query_stored_file_info(idx_path, gz_path_logical_path, stored_sha256,
                               stored_mtime)) {
        // quick check using modification time as optimization
        // time_t current_mtime = get_file_mtime(indexer->gz_path);
        // if (current_mtime != stored_mtime && current_mtime > 0 &&
        // stored_mtime > 0)
        // {
        //     DFTRACER_UTILS_LOG_DEBUG("Index rebuild needed: file modification
        //     time changed"); return 1;
        // }

        std::string current_sha256 = calculate_file_sha256(gz_path);
        if (current_sha256.empty()) {
            throw IndexerError(IndexerError::FILE_ERROR,
                               "Failed to calculate SHA256 for " + gz_path);
        }

        if (current_sha256 != stored_sha256) {
            DFTRACER_UTILS_LOG_INFO(
                "Index rebuild needed: file SHA256 changed (%s vs %s)",
                (current_sha256.substr(0, 16) + "...").c_str(),
                (stored_sha256.substr(0, 16) + "...").c_str());
            return true;
        }
    }

    DFTRACER_UTILS_LOG_DEBUG(
        "Index rebuild not needed: file content unchanged");
    return false;
}

std::uint64_t IndexerImplementor::get_max_bytes() const {
    return query_max_bytes(db, gz_path_logical_path);
}

std::size_t IndexerImplementor::get_checkpoint_size() const {
    return ckpt_size;
}

std::uint64_t IndexerImplementor::get_num_lines() const {
    return query_num_lines(db, gz_path_logical_path);
}

int IndexerImplementor::get_file_id() const {
    if (cached_file_id != -1) {
        return cached_file_id;
    }

    cached_file_id = find_file_id(gz_path);
    return cached_file_id;
}

int IndexerImplementor::find_file_id(const std::string &gz_path) const {
    return query_file_id(db, get_logical_path(gz_path));
}

bool IndexerImplementor::find_checkpoint(std::size_t target_offset,
                                         IndexCheckpoint &checkpoint) const {
    return query_checkpoint(db, target_offset, cached_file_id, checkpoint);
}

std::vector<IndexCheckpoint> IndexerImplementor::get_checkpoints() const {
    return query_checkpoints(db, cached_file_id);
}
}  // namespace dftracer::utils
