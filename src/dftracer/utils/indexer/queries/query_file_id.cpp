#include <dftracer/utils/indexer/queries/queries.h>

int query_file_id(const SqliteDatabase &db,
                  const std::string &gz_path_logical_path) {
    SqliteStmt stmt(db, "SELECT id FROM files WHERE logical_name = ? LIMIT 1");
    int file_id = -1;

    sqlite3_bind_text(stmt, 1, gz_path_logical_path.c_str(), -1,
                      SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        file_id = sqlite3_column_int(stmt, 0);
    }

    return file_id;
}
