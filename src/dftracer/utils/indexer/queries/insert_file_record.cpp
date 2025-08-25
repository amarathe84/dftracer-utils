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
