#include <dftracer/utils/indexer/queries/queries.h>

#include <dftracer/utils/indexer/sqlite/statement.h>

#include <cstring>

std::vector<IndexCheckpoint> query_checkpoints(const SqliteDatabase &db,
                                               int file_id) {
    std::vector<dftracer::utils::IndexCheckpoint> checkpoints;

    SqliteStmt stmt(
        db,
        "SELECT checkpoint_idx, uc_offset, uc_size, c_offset, c_size, bits, "
        "dict_compressed, num_lines "
        "FROM checkpoints WHERE file_id = ? ORDER BY uc_offset");

    stmt.bind_int(1, file_id);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        dftracer::utils::IndexCheckpoint checkpoint;
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

        size_t dict_size = static_cast<size_t>(sqlite3_column_bytes(stmt, 6));
        checkpoint.dict_compressed.resize(dict_size);
        std::memcpy(checkpoint.dict_compressed.data(),
                    sqlite3_column_blob(stmt, 6), dict_size);

        checkpoint.num_lines =
            static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 7));

        checkpoints.push_back(std::move(checkpoint));
    }

    return checkpoints;
}
