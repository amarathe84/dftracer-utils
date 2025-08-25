#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/queries/queries.h>

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
