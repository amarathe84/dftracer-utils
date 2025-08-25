#ifndef DFT_UTILS_INDEXER_SQLITE_QUERIES_H
#define DFT_UTILS_INDEXER_SQLITE_QUERIES_H

#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/sqlite/database.h>
#include <dftracer/utils/indexer/sqlite/statement.h>

#include <cstdint>

using namespace dftracer::utils;

void insert_file_record(const SqliteDatabase &db,
                        const std::string &gz_path_logical_path,
                        std::size_t bytes, std::time_t file_mtime,
                        const std::string &file_sha256, int &db_file_id) {
    SqliteStmt stmt(db,
                    "INSERT INTO files(logical_name, byte_size, "
                    "mtime_unix, sha256_hex) "
                    "VALUES(?, ?, ?, ?) "
                    "ON CONFLICT(logical_name) DO UPDATE SET "
                    "byte_size=excluded.byte_size, "
                    "mtime_unix=excluded.mtime_unix, "
                    "sha256_hex=excluded.sha256_hex "
                    "RETURNING id;");

    sqlite3_bind_text(stmt, 1, gz_path_logical_path.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(bytes));
    sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(file_mtime));
    sqlite3_bind_text(stmt, 4, file_sha256.c_str(), -1, SQLITE_TRANSIENT);

    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
        throw IndexerError(
            IndexerError::Type::DATABASE_ERROR,
            "Insert failed: " + std::string(sqlite3_errmsg(db.get())));
    }

    db_file_id = sqlite3_column_int(stmt, 0);
}

void insert_file_metadata_record(const SqliteDatabase &db, int file_id,
                                 std::size_t ckpt_size,
                                 std::uint64_t total_lines,
                                 std::uint64_t total_uc_size) {
    SqliteStmt stmt(db,
                    "INSERT INTO metadata(file_id, checkpoint_size, "
                    "total_lines, total_uc_size) VALUES(?, ?, ?, ?);");

    sqlite3_bind_int(stmt, 1, file_id);
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(ckpt_size));
    sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(total_lines));
    sqlite3_bind_int64(stmt, 4, static_cast<sqlite3_int64>(total_uc_size));

    int result = sqlite3_step(stmt);
    if (result != SQLITE_DONE) {
        throw IndexerError(
            IndexerError::Type::DATABASE_ERROR,
            "Insert failed: " + std::string(sqlite3_errmsg(db.get())));
    }
    DFTRACER_UTILS_LOG_DEBUG(
        "Successfully inserted metadata for file_id %d: "
        "checkpoint_size=%zu, "
        "total_lines=%llu, total_uc_size=%llu",
        file_id, ckpt_size, total_lines, total_uc_size);
}

bool get_stored_file_info(const SqliteDatabase &db, const std::string &gz_path,
                          std::string &stored_sha256, time_t &stored_mtime) {
    SqliteStmt stmt(db,
                    "SELECT sha256_hex, mtime_unix FROM files WHERE "
                    "logical_name = ? LIMIT 1");

    sqlite3_bind_text(stmt, 1, gz_path.c_str(), -1, SQLITE_STATIC);

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *sha256_text =
            reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
        if (sha256_text) {
            stored_sha256 = sha256_text;
        }
        stored_mtime = static_cast<time_t>(sqlite3_column_int64(stmt, 1));
        return true;
    }

    return false;
}

struct InsertCheckpointData {
    std::uint64_t checkpoint_idx;
    std::uint64_t current_uc_offset;
    std::uint64_t checkpoint_uc_size;
    std::uint64_t checkpoint_c_size;
    std::uint64_t checkpoint_c_offset;
    int checkpoint_bits;
    const void *compressed_dict;
    std::size_t compressed_dict_size;
    uint64_t num_lines;
};

void insert_checkpoint(const SqliteDatabase &db, int file_id,
                       const InsertCheckpointData &data) {
    SqliteStmt stmt(
        db,
        "INSERT INTO checkpoints(file_id, checkpoint_idx, uc_offset, "
        "uc_size, c_offset, c_size, bits, dict_compressed, num_lines) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");

    sqlite3_bind_int(stmt, 1, file_id);
    sqlite3_bind_int64(stmt, 2, static_cast<int64_t>(data.checkpoint_idx));
    sqlite3_bind_int64(stmt, 3, static_cast<int64_t>(data.current_uc_offset));
    sqlite3_bind_int64(stmt, 4, static_cast<int64_t>(data.checkpoint_uc_size));
    sqlite3_bind_int64(stmt, 5, static_cast<int64_t>(data.checkpoint_c_offset));
    sqlite3_bind_int64(stmt, 6, static_cast<int64_t>(data.checkpoint_c_size));
    sqlite3_bind_int(stmt, 7, data.checkpoint_bits);
    sqlite3_bind_blob(stmt, 8, data.compressed_dict,
                      static_cast<int>(data.compressed_dict_size),
                      SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 9, static_cast<int64_t>(data.num_lines));

    int result = sqlite3_step(stmt);
    if (result != SQLITE_DONE) {
        throw IndexerError(IndexerError::Type::DATABASE_ERROR,
                           "Failed to insert checkpoint: " +
                               std::string(sqlite3_errmsg(db.get())));
    }
}

bool is_index_schema_valid(const SqliteDatabase &db) {
    SqliteStmt stmt(db,
                    "SELECT name FROM sqlite_master WHERE type='table' AND "
                    "name IN ('checkpoints', 'metadata', 'files')");
    int table_count = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        table_count++;
    }

    return table_count >= 3;
}

bool cleanup_existing_data(const SqliteDatabase &db, int file_id) {
    const char *cleanup_queries[] = {
        "DELETE FROM checkpoints WHERE file_id = ?;",
        "DELETE FROM metadata WHERE file_id = ?;"};

    for (const char *query : cleanup_queries) {
        try {
            SqliteStmt stmt(db, query);
            sqlite3_bind_int(stmt, 1, file_id);
            int result = sqlite3_step(stmt);
            if (result != SQLITE_DONE) {
                DFTRACER_UTILS_LOG_ERROR(
                    "Failed to execute cleanup statement '%s' for file_id %d: "
                    "%d - "
                    "%s",
                    query, file_id, result, sqlite3_errmsg(db.get()));
                return false;
            }
        } catch (const IndexerError &e) {
            DFTRACER_UTILS_LOG_ERROR(
                "Failed to prepare cleanup statement '%s': %s", query,
                e.what());
            return false;
        }
    }
    DFTRACER_UTILS_LOG_DEBUG(
        "Successfully cleaned up existing data for file_id %d", file_id);
    return true;
}

std::size_t query_max_bytes(const SqliteDatabase &db,
                            const std::string &gz_path_logical_path) {
    SqliteStmt stmt(
        db,
        "SELECT MAX(uc_offset + uc_size) FROM checkpoints WHERE file_id = "
        "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)");
    uint64_t max_bytes = 0;

    sqlite3_bind_text(stmt, 1, gz_path_logical_path.c_str(), -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        max_bytes = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
    }

    // If no checkpoints exist (max_bytes is 0), fall back to metadata table
    if (max_bytes == 0) {
        SqliteStmt metadata_stmt(
            db,
            "SELECT total_uc_size FROM metadata WHERE file_id = "
            "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)");
        sqlite3_bind_text(metadata_stmt, 1, gz_path_logical_path.c_str(), -1,
                          SQLITE_STATIC);
        if (sqlite3_step(metadata_stmt) == SQLITE_ROW) {
            max_bytes =
                static_cast<uint64_t>(sqlite3_column_int64(metadata_stmt, 0));
            DFTRACER_UTILS_LOG_DEBUG(
                "No checkpoints found, using metadata total_uc_size: %llu",
                max_bytes);
        }
    }

    return max_bytes;
}

#endif  // DFT_UTILS_INDEXER_SQLITE_QUERIES_H
