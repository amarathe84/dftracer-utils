#include <dftracer/utils/indexer/queries/queries.h>

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

    stmt.bind_text(1, gz_path_logical_path);
    stmt.bind_int64(2, static_cast<int64_t>(bytes));
    stmt.bind_int64(3, static_cast<int64_t>(file_mtime));
    stmt.bind_text(4, file_sha256);

    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
        throw IndexerError(
            IndexerError::Type::DATABASE_ERROR,
            "Insert failed: " + std::string(sqlite3_errmsg(db.get())));
    }

    db_file_id = sqlite3_column_int(stmt, 0);
}
