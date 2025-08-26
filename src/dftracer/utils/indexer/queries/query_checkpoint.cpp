#include <dftracer/utils/indexer/queries/queries.h>

#include <cstring>

bool query_checkpoint(const SqliteDatabase& db, size_t target_offset,
                      int file_id, IndexCheckpoint& checkpoint) {
    // For target offset 0, always decompress from beginning of file (no
    // checkpoint)
    if (target_offset == 0) {
        return false;
    }

    if (file_id == -1) {
        return false;
    }

    SqliteStmt stmt(
        db,
        "SELECT checkpoint_idx, uc_offset, uc_size, c_offset, c_size, bits, "
        "dict_compressed, num_lines "
        "FROM checkpoints WHERE file_id = ? AND uc_offset <= ? "
        "ORDER BY uc_offset DESC LIMIT 1");
    bool found = false;

    stmt.bind_int(1, file_id);
    stmt.bind_int64(2, static_cast<int64_t>(target_offset));

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        checkpoint.checkpoint_idx =
            static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 0));
        checkpoint.uc_offset =
            static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 1));
        checkpoint.uc_size =
            static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 2));
        checkpoint.c_offset =
            static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 3));
        checkpoint.c_size =
            static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 4));
        checkpoint.bits = sqlite3_column_int(stmt, 5);
        std::size_t dict_size =
            static_cast<std::size_t>(sqlite3_column_bytes(stmt, 6));
        checkpoint.dict_compressed.resize(dict_size);
        std::memcpy(checkpoint.dict_compressed.data(),
                    sqlite3_column_blob(stmt, 6), dict_size);
        checkpoint.num_lines =
            static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 7));
        found = true;
    }

    return found;
}
