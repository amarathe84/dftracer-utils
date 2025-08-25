#include <dftracer/utils/indexer/queries/queries.h>

bool query_stored_file_info(const SqliteDatabase &db,
                            const std::string &gz_path,
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
