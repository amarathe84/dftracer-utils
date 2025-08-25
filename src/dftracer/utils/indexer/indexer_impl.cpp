#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/constants.h>
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/helpers.h>
#include <dftracer/utils/indexer/indexer_impl.h>
#include <dftracer/utils/indexer/queries.h>

static void init_schema(SqliteDatabase &db) {
    char *errmsg = NULL;
    int rc = sqlite3_exec(db.get(), constants::SQL_SCHEMA, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        std::string error =
            "Failed to initialize database schema: " + std::string(errmsg);
        sqlite3_free(errmsg);
        throw IndexerError(IndexerError::DATABASE_ERROR, error);
    }
    DFTRACER_UTILS_LOG_DEBUG("Schema init succeeded");
}

static bool build_index(SqliteDatabase &db, int file_id,
                        const std::string &gz_path, const std::string &idx_path,
                        size_t ckpt_size) {
    FILE *fp = fopen(gz_path.c_str(), "rb");
    if (!fp) return false;

    sqlite3_exec(db.get(), "BEGIN IMMEDIATE;", NULL, NULL, NULL);

    try {
        if (cleanup_existing_data(db, file_id) != 0) {
            goto failure;
        }

        // Process chunks and get total line count and uncompressed size
        uint64_t total_lines = 0;
        uint64_t total_uc_size = 0;
        int result = process_chunks(fp, db, file_id, ckpt_size, total_lines,
                                    total_uc_size);
        fclose(fp);

        if (result != 0) goto failure;
        insert_file_metadata_record(db, file_id, ckpt_size, total_lines,
                                    total_uc_size);
    } catch (...) {
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
IndexerImplementor::IndexerImplementor(const std::string &gz_path,
                                       const std::string &idx_path,
                                       size_t ckpt_size, bool force_rebuild)
    : gz_path(gz_path),
      gz_path_logical_path(get_logical_path(gz_path)),
      idx_path(idx_path),
      ckpt_size(ckpt_size),
      is_valid_cache(false),
      force_rebuild(force_rebuild),
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
        is_valid_cache = true;
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

    if (!build_index(db, cached_file_id, gz_path, idx_path, ckpt_size)) {
        throw IndexerError(IndexerError::Type::BUILD_ERROR,
                           "Index build failed for: " + gz_path);
    }

    if (!index_exists_and_valid(idx_path)) {
        throw IndexerError(
            IndexerError::Type::BUILD_ERROR,
            "Index build completed but index is invalid: " + idx_path);
    }
    is_valid_cache = true;
}

bool IndexerImplementor::is_valid() const { return is_valid_cache; }

bool IndexerImplementor::need_rebuild() const {
    if (is_valid()) return false;
    return is_index_schema_valid(db);
}

std::uint64_t IndexerImplementor::get_max_bytes() const {
    return query_max_bytes(db, gz_path_logical_path);
}

std::uint64_t IndexerImplementor::get_num_lines() const {
    return 0;  // Placeholder implementation
}

int IndexerImplementor::get_file_id() const {
    if (cached_file_id != -1) {
        return cached_file_id;
    }

    cached_file_id = find_file_id(gz_path);
    return cached_file_id;
}

int IndexerImplementor::find_file_id(const std::string &gz_path) const {
    return -1;  // Placeholder implementation
}

bool IndexerImplementor::find_checkpoint(size_t target_offset,
                                         IndexCheckpoint &checkpoint) const {
    return false;  // Placeholder implementation
}

std::vector<IndexCheckpoint> IndexerImplementor::get_checkpoints() const {
    return {};  // Placeholder implementation
}
}  // namespace dftracer::utils
