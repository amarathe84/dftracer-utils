#ifndef DFT_UTILS_INDEXER_SQLITE_STATEMENT_H
#define DFT_UTILS_INDEXER_SQLITE_STATEMENT_H

#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/sqlite/database.h>
#include <sqlite3.h>

#include <string>

using namespace dftracer::utils;

class SqliteStmt {
   public:
    SqliteStmt(const SqliteDatabase &db, const char *sql) {
        sqlite3 *raw_db = db.get();
        if (sqlite3_prepare_v2(raw_db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            stmt_ = nullptr;
            throw IndexerError(IndexerError::Type::DATABASE_ERROR,
                               "Failed to prepare SQL statement: " +
                                   std::string(sqlite3_errmsg(raw_db)));
        }
    }

    SqliteStmt(sqlite3 *db, const char *sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            stmt_ = nullptr;
            throw IndexerError(IndexerError::Type::DATABASE_ERROR,
                               "Failed to prepare SQL statement: " +
                                   std::string(sqlite3_errmsg(db)));
        }
    }

    ~SqliteStmt() {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    SqliteStmt(const SqliteStmt &) = delete;
    SqliteStmt &operator=(const SqliteStmt &) = delete;

    operator sqlite3_stmt *() { return stmt_; }
    sqlite3_stmt *get() { return stmt_; }

    void reset() { sqlite3_reset(stmt_); }

   private:
    sqlite3_stmt *stmt_;
};

#endif  // DFT_UTILS_INDEXER_SQLITE_STATEMENT_H
