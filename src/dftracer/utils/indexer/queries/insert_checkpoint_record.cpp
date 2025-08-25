#include <dftracer/utils/indexer/queries/queries.h>

void insert_checkpoint_record(const SqliteDatabase &db, int file_id,
                              const InsertCheckpointData &data) {
    SqliteStmt stmt(
        db,
        "INSERT INTO checkpoints(file_id, checkpoint_idx, uc_offset, "
        "uc_size, c_offset, c_size, bits, dict_compressed, num_lines) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");

    sqlite3_bind_int(stmt, 1, file_id);
    sqlite3_bind_int64(stmt, 2, static_cast<int64_t>(data.idx));
    sqlite3_bind_int64(stmt, 3, static_cast<int64_t>(data.uc_offset));
    sqlite3_bind_int64(stmt, 4, static_cast<int64_t>(data.uc_size));
    sqlite3_bind_int64(stmt, 5, static_cast<int64_t>(data.c_offset));
    sqlite3_bind_int64(stmt, 6, static_cast<int64_t>(data.c_size));
    sqlite3_bind_int(stmt, 7, data.bits);
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
