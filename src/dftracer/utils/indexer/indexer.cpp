#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/utils/file.h>
#include <dftracer/utils/utils/filesystem.h>
#include <dftracer/utils/utils/platform_compat.h>
#include <picosha2.h>
#include <spdlog/spdlog.h>
#include <sqlite3.h>
#include <zlib.h>

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace dftracer {
namespace utils {
namespace indexer {

// ==============================================================================
// Constants and Configuration
// ==============================================================================

namespace constants {
static constexpr size_t INFLATE_BUFFER_SIZE = 16384;
static constexpr size_t PROCESS_BUFFER_SIZE = 65536;
static constexpr size_t ZLIB_WINDOW_SIZE = 32768;
static constexpr int ZLIB_GZIP_WINDOW_BITS = 31;  // 15 + 16 for gzip format

static const char *SQL_SCHEMA = R"(
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER PRIMARY KEY,
      logical_name TEXT UNIQUE NOT NULL,
      byte_size INTEGER NOT NULL,
      mtime_unix INTEGER NOT NULL,
      sha256_hex TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS checkpoints (
      id INTEGER PRIMARY KEY,
      file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
      checkpoint_idx INTEGER NOT NULL,
      uc_offset INTEGER NOT NULL,
      uc_size INTEGER NOT NULL,
      c_offset INTEGER NOT NULL,
      c_size INTEGER NOT NULL,
      bits INTEGER NOT NULL,
      dict_compressed BLOB NOT NULL,
      num_lines INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS checkpoints_file_idx ON checkpoints(file_id, checkpoint_idx);
    CREATE INDEX IF NOT EXISTS checkpoints_file_uc_off_idx ON checkpoints(file_id, uc_offset);

    CREATE TABLE IF NOT EXISTS metadata (
      file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
      checkpoint_size INTEGER NOT NULL,
      total_lines INTEGER NOT NULL DEFAULT 0,
      total_uc_size INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(file_id)
    );
  )";
}  // namespace constants

// ==============================================================================
// Helper Structures
// ==============================================================================

struct InflateState {
  z_stream zs;
  FILE *file;
  unsigned char in[constants::INFLATE_BUFFER_SIZE];

  InflateState() : file(nullptr) {
    memset(&zs, 0, sizeof(zs));
    memset(in, 0, sizeof(in));
  }
};

struct CheckpointData {
  size_t uc_offset;
  size_t c_offset;
  int bits;
  unsigned char window[constants::ZLIB_WINDOW_SIZE];

  CheckpointData() : uc_offset(0), c_offset(0), bits(0) {
    memset(window, 0, sizeof(window));
  }
};

class SqliteStmt {
 public:
  SqliteStmt(sqlite3 *db, const char *sql) {
    if (sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
      stmt_ = nullptr;
      throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                           "Failed to prepare SQL statement: " +
                               std::string(sqlite3_errmsg(db)));
    }
  }

  ~SqliteStmt() {
    if (stmt_) {
      sqlite3_finalize(stmt_);
    }
  }

  // No copy
  SqliteStmt(const SqliteStmt &) = delete;
  SqliteStmt &operator=(const SqliteStmt &) = delete;

  operator sqlite3_stmt *() { return stmt_; }
  sqlite3_stmt *get() { return stmt_; }

  void reset() { sqlite3_reset(stmt_); }

 private:
  sqlite3_stmt *stmt_;
};

// ==============================================================================
// Error Handling Helper
// ==============================================================================

class ErrorHandler {
 public:
  static void validate_parameters(size_t ckpt_size) {
    if (ckpt_size == 0) {
      throw Indexer::Error(Indexer::Error::INVALID_ARGUMENT,
                           "ckpt_size must be greater than 0");
    }
  }

  static void check_indexer_state(sqlite3 *db) {
    if (!db) {
      throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                           "Database connection is not open");
    }
  }

  static void validate_line_range(size_t start_line, size_t end_line) {
    if (start_line == 0 || end_line == 0 || start_line > end_line) {
      throw Indexer::Error(
          Indexer::Error::INVALID_ARGUMENT,
          "Invalid line range: start_line and end_line must be > 0 and "
          "start_line <= end_line");
    }
  }
};

class Indexer::Impl {
 public:
  Impl(const std::string &gz_path, const std::string &idx_path,
       size_t ckpt_size, bool force_rebuild)
      : gz_path_(gz_path),
        gz_path_logical_path_(get_logical_path(gz_path)),
        idx_path_(idx_path),
        ckpt_size_(ckpt_size),
        force_rebuild_(force_rebuild),
        db_(nullptr),
        db_opened_(false),
        cached_file_id_(-1) {
    ErrorHandler::validate_parameters(ckpt_size_);
    spdlog::debug("Created DFT indexer for gz: {} and index: {}", gz_path,
                  idx_path);
  }

  ~Impl() {
    if (db_opened_ && db_) {
      sqlite3_close(db_);
    }
    spdlog::debug("Successfully destroyed DFT indexer");
  }

  // Disable copy
  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;

  // Enable move
  Impl(Impl &&other) noexcept
      : gz_path_(std::move(other.gz_path_)),
        gz_path_logical_path_(get_logical_path(gz_path_)),
        idx_path_(std::move(other.idx_path_)),
        ckpt_size_(other.ckpt_size_),
        force_rebuild_(other.force_rebuild_),
        db_(other.db_),
        db_opened_(other.db_opened_),
        cached_file_id_(other.cached_file_id_) {
    other.db_ = nullptr;
    other.db_opened_ = false;
  }

  Impl &operator=(Impl &&other) noexcept {
    if (this != &other) {
      if (db_opened_ && db_) {
        sqlite3_close(db_);
      }
      gz_path_ = std::move(other.gz_path_);
      gz_path_logical_path_ = get_logical_path(gz_path_);
      idx_path_ = std::move(other.idx_path_);
      ckpt_size_ = other.ckpt_size_;
      force_rebuild_ = other.force_rebuild_;
      db_ = other.db_;
      db_opened_ = other.db_opened_;
      cached_file_id_ = other.cached_file_id_;
      other.db_ = nullptr;
      other.db_opened_ = false;
    }
    return *this;
  }

  bool need_rebuild() const;
  void build();

  bool is_valid() const { return true; }

  const std::string &get_gz_path() const { return gz_path_; }

  const std::string &get_idx_path() const { return idx_path_; }

  size_t get_checkpoint_size() const { return ckpt_size_; }

  uint64_t get_max_bytes() const;
  uint64_t get_num_lines() const;
  int find_file_id(const std::string &gz_path) const;
  bool find_checkpoint(size_t target_offset, CheckpointInfo &checkpoint) const;
  std::vector<CheckpointInfo> get_checkpoints() const;
  std::vector<CheckpointInfo> find_checkpoints_by_line_range(
      size_t start_line, size_t end_line) const;
  int get_file_id() const;

 private:
  // Helper methods
  std::string calculate_file_sha256(const std::string &file_path) const;
  time_t get_file_mtime(const std::string &file_path) const;
  bool index_exists_and_valid(const std::string &idx_path) const;
  size_t get_existing_ckpt_size(const std::string &idx_path) const;
  bool get_stored_file_info(const std::string &idx_path,
                            const std::string &gz_path,
                            std::string &stored_sha256,
                            time_t &stored_mtime) const;
  uint64_t file_size_bytes(const std::string &path) const;
  int init_schema(sqlite3 *db) const;
  int build_index_internal(sqlite3 *db, int file_id, const std::string &gz_path,
                           size_t ckpt_size) const;

  // Database cleanup helpers
  int cleanup_existing_data(sqlite3 *db, int file_id) const;
  int insert_metadata(sqlite3 *db, int file_id, size_t ckpt_size,
                      uint64_t total_lines, uint64_t total_uc_size) const;
  int process_chunks(FILE *fp, sqlite3 *db, int file_id, size_t ckpt_size,
                     uint64_t &total_lines_out,
                     uint64_t &total_uc_size_out) const;
  void save_chunk(sqlite3_stmt *stmt, int file_id, size_t chunk_idx,
                  size_t chunk_start_c_off, size_t chunk_c_size,
                  size_t chunk_start_uc_off, size_t chunk_uc_size,
                  size_t events) const;

  int inflate_init_simple(InflateState *state, FILE *f) const;
  void inflate_cleanup_simple(InflateState *state) const;
  int inflate_process_chunk(InflateState *state, unsigned char *out,
                            size_t out_size, size_t *bytes_out,
                            size_t *c_off) const;

  int create_checkpoint(InflateState *state, CheckpointData *checkpoint,
                        size_t uc_offset) const;
  int compress_window(const unsigned char *window, size_t window_size,
                      unsigned char **compressed,
                      size_t *compressed_size) const;
  int save_checkpoint(sqlite3 *db, int file_id,
                      const CheckpointData *checkpoint) const;

  std::string get_logical_path(const std::string &path) const;

  std::string gz_path_;
  std::string gz_path_logical_path_;
  std::string idx_path_;
  size_t ckpt_size_;
  bool force_rebuild_;
  sqlite3 *db_;
  bool db_opened_;
  mutable int cached_file_id_;
};

std::string Indexer::Impl::get_logical_path(const std::string &path) const {
  auto fs_path = fs::path(path);
  return fs_path.filename().string();
}

std::string Indexer::Impl::calculate_file_sha256(
    const std::string &file_path) const {
  std::ifstream file(file_path, std::ios::binary);
  if (!file.is_open()) {
    spdlog::error("Cannot open file for SHA256 calculation: {}", file_path);
    return "";
  }

  std::vector<unsigned char> buffer(8192);
  picosha2::hash256_one_by_one hasher;
  hasher.init();

  while (file.read(reinterpret_cast<char *>(buffer.data()),
                   static_cast<std::streamsize>(buffer.size())) ||
         file.gcount() > 0) {
    hasher.process(buffer.begin(), buffer.begin() + file.gcount());
  }

  hasher.finish();
  std::string hex_str;
  picosha2::get_hash_hex_string(hasher, hex_str);
  return hex_str;
}

time_t Indexer::Impl::get_file_mtime(const std::string &file_path) const {
  return dftracer::utils::utils::get_file_modification_time(file_path);
}

bool Indexer::Impl::index_exists_and_valid(const std::string &idx_path) const {
  FILE *f = fopen(idx_path.c_str(), "rb");
  if (!f) return false;
  fclose(f);

  sqlite3 *db;
  if (sqlite3_open(idx_path.c_str(), &db) != SQLITE_OK) {
    return false;
  }

  sqlite3_stmt *stmt;
  const char *sql =
      "SELECT name FROM sqlite_master WHERE type='table' AND "
      "name IN ('checkpoints', 'metadata', 'files')";
  int table_count = 0;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      table_count++;
    }
    sqlite3_finalize(stmt);
  }

  sqlite3_close(db);
  return table_count >= 3;
}

size_t Indexer::Impl::get_existing_ckpt_size(
    const std::string &idx_path) const {
  sqlite3 *db;
  if (sqlite3_open(idx_path.c_str(), &db) != SQLITE_OK) {
    return -1;
  }

  sqlite3_stmt *stmt;
  const char *sql = "SELECT checkpoint_size FROM metadata LIMIT 1";
  size_t ckpt_size = 0;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      ckpt_size = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
    }
    sqlite3_finalize(stmt);
  }

  sqlite3_close(db);
  return ckpt_size;
}

int Indexer::Impl::cleanup_existing_data(sqlite3 *db, int file_id) const {
  const char *cleanup_queries[] = {"DELETE FROM checkpoints WHERE file_id = ?;",
                                   "DELETE FROM metadata WHERE file_id = ?;"};

  for (const char *query : cleanup_queries) {
    sqlite3_stmt *stmt = nullptr;
    if (sqlite3_prepare_v2(db, query, -1, &stmt, nullptr) != SQLITE_OK) {
      spdlog::error("Failed to prepare cleanup statement '{}': {}", query,
                    sqlite3_errmsg(db));
      return -1;
    }
    sqlite3_bind_int(stmt, 1, file_id);
    int result = sqlite3_step(stmt);
    if (result != SQLITE_DONE) {
      spdlog::error(
          "Failed to execute cleanup statement '{}' for file_id {}: {} - {}",
          query, file_id, result, sqlite3_errmsg(db));
      sqlite3_finalize(stmt);
      return -1;
    }
    sqlite3_finalize(stmt);
  }
  spdlog::debug("Successfully cleaned up existing data for file_id {}",
                file_id);
  return 0;
}

int Indexer::Impl::insert_metadata(sqlite3 *db, int file_id, size_t ckpt_size,
                                   uint64_t total_lines,
                                   uint64_t total_uc_size) const {
  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db,
                         "INSERT INTO metadata(file_id, checkpoint_size, "
                         "total_lines, total_uc_size) VALUES(?, ?, ?, ?);",
                         -1, &stmt, nullptr) != SQLITE_OK) {
    spdlog::error("Failed to prepare metadata insert statement: {}",
                  sqlite3_errmsg(db));
    return -1;
  }

  sqlite3_bind_int(stmt, 1, file_id);
  sqlite3_bind_int64(stmt, 2, static_cast<int64_t>(ckpt_size));
  sqlite3_bind_int64(stmt, 3, static_cast<int64_t>(total_lines));
  sqlite3_bind_int64(stmt, 4, static_cast<int64_t>(total_uc_size));

  int result = sqlite3_step(stmt);
  if (result != SQLITE_DONE) {
    spdlog::error("Failed to insert metadata for file_id {}: {} - {}", file_id,
                  result, sqlite3_errmsg(db));
  } else {
    spdlog::debug(
        "Successfully inserted metadata for file_id {}: checkpoint_size={}, "
        "total_lines={}, total_uc_size={}",
        file_id, ckpt_size, total_lines, total_uc_size);
  }
  sqlite3_finalize(stmt);
  return (result == SQLITE_DONE) ? 0 : -1;
}

void Indexer::Impl::save_chunk(sqlite3_stmt *stmt, int file_id,
                               size_t chunk_idx, size_t chunk_start_c_off,
                               size_t chunk_c_size, size_t chunk_start_uc_off,
                               size_t chunk_uc_size, size_t events) const {
  sqlite3_bind_int(stmt, 1, file_id);
  sqlite3_bind_int64(stmt, 2, static_cast<int64_t>(chunk_idx));
  sqlite3_bind_int64(stmt, 3, static_cast<int64_t>(chunk_start_c_off));
  sqlite3_bind_int64(stmt, 4, static_cast<int64_t>(chunk_c_size));
  sqlite3_bind_int64(stmt, 5, static_cast<int64_t>(chunk_start_uc_off));
  sqlite3_bind_int64(stmt, 6, static_cast<int64_t>(chunk_uc_size));
  sqlite3_bind_int64(stmt, 7, static_cast<int64_t>(events));
  sqlite3_step(stmt);
  sqlite3_reset(stmt);
}

int Indexer::Impl::process_chunks(FILE *fp, sqlite3 *db, int file_id,
                                  size_t checkpoint_size,
                                  uint64_t &total_lines_out,
                                  uint64_t &total_uc_size_out) const {
  // Reset file pointer to beginning for gzip decompression
  if (fseeko(fp, 0, SEEK_SET) != 0) {
    return -4;
  }

  try {
    SqliteStmt st_checkpoint(
        db,
        "INSERT INTO checkpoints(file_id, checkpoint_idx, uc_offset, uc_size, "
        "c_offset, c_size, bits, dict_compressed, num_lines) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);");

    InflateState inflate_state;
    if (inflate_init_simple(&inflate_state, fp) != 0) {
      return -4;
    }

    spdlog::debug(
        "Starting sequential checkpoint creation with checkpoint_size={} bytes",
        checkpoint_size);

    size_t checkpoint_idx = 0;
    size_t current_uc_offset = 0;  // Current uncompressed offset
    size_t checkpoint_start_uc_offset =
        0;  // Start of current checkpoint (uncompressed)

    unsigned char buffer[constants::PROCESS_BUFFER_SIZE];
    uint64_t total_lines = 0;

    while (true) {
      size_t bytes_read;
      size_t c_off;

      if (inflate_process_chunk(&inflate_state, buffer, sizeof(buffer),
                                &bytes_read, &c_off) != 0) {
        break;
      }

      if (bytes_read == 0) {
        // End of file reached - handle any remaining data
        break;
      }

      // Count lines in this buffer
      for (size_t i = 0; i < bytes_read; i++) {
        if (buffer[i] == '\n') {
          total_lines++;
        }
      }

      current_uc_offset += bytes_read;

      // Check if current checkpoint has reached the target size
      size_t current_checkpoint_size =
          current_uc_offset - checkpoint_start_uc_offset;
      if (current_checkpoint_size >= checkpoint_size &&
          (inflate_state.zs.data_type & 0xc0) ==
              0x80 &&                         // At deflate block boundary
          inflate_state.zs.avail_out == 0) {  // Output buffer is consumed

        // Create checkpoint at current position
        size_t checkpoint_uc_size =
            current_uc_offset - checkpoint_start_uc_offset;

        // Create checkpoint for random access
        CheckpointData checkpoint_data;
        if (create_checkpoint(&inflate_state, &checkpoint_data,
                              current_uc_offset) == 0) {
          unsigned char *compressed_dict = nullptr;
          size_t compressed_dict_size = 0;

          if (compress_window(checkpoint_data.window,
                              constants::ZLIB_WINDOW_SIZE, &compressed_dict,
                              &compressed_dict_size) == 0) {
            // Insert checkpoint into database
            sqlite3_bind_int(st_checkpoint, 1, file_id);
            sqlite3_bind_int64(st_checkpoint, 2,
                               static_cast<int64_t>(checkpoint_idx));
            sqlite3_bind_int64(st_checkpoint, 3,
                               static_cast<int64_t>(current_uc_offset));
            sqlite3_bind_int64(st_checkpoint, 4,
                               static_cast<int64_t>(checkpoint_uc_size));
            sqlite3_bind_int64(st_checkpoint, 5,
                               static_cast<int64_t>(checkpoint_data.c_offset));
            sqlite3_bind_int64(
                st_checkpoint, 6,
                static_cast<int64_t>(
                    checkpoint_data
                        .c_offset));  // c_size same as c_offset for simplicity
            sqlite3_bind_int(st_checkpoint, 7, checkpoint_data.bits);
            sqlite3_bind_blob(st_checkpoint, 8, compressed_dict,
                              static_cast<int>(compressed_dict_size),
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(
                st_checkpoint, 9,
                static_cast<int64_t>(
                    0));  // Approximate line count - not tracked precisely
            sqlite3_step(st_checkpoint);
            sqlite3_reset(st_checkpoint);

            spdlog::debug("Created checkpoint {}: uc_offset={}, size={} bytes",
                          checkpoint_idx, checkpoint_start_uc_offset,
                          checkpoint_uc_size);

            free(compressed_dict);

            // Setup for next checkpoint
            checkpoint_idx++;
            checkpoint_start_uc_offset = current_uc_offset;
          }
        }
      }
    }

    // Always create a final checkpoint if we have any remaining data
    if (current_uc_offset > checkpoint_start_uc_offset) {
      size_t checkpoint_uc_size =
          current_uc_offset - checkpoint_start_uc_offset;

      // Create a special checkpoint for remaining data (might be without
      // deflate boundary)
      if (checkpoint_start_uc_offset == 0) {
        // This is a checkpoint starting from the beginning - no dictionary
        // needed
        sqlite3_bind_int(st_checkpoint, 1, file_id);
        sqlite3_bind_int64(st_checkpoint, 2,
                           static_cast<int64_t>(checkpoint_idx));
        sqlite3_bind_int64(st_checkpoint, 3,
                           static_cast<int64_t>(checkpoint_start_uc_offset));
        sqlite3_bind_int64(st_checkpoint, 4,
                           static_cast<int64_t>(checkpoint_uc_size));
        sqlite3_bind_int64(st_checkpoint, 5,
                           static_cast<int64_t>(0));  // c_offset
        sqlite3_bind_int64(st_checkpoint, 6,
                           static_cast<int64_t>(0));  // c_size
        sqlite3_bind_int(st_checkpoint, 7, 0);        // bits
        sqlite3_bind_blob(st_checkpoint, 8, nullptr, 0,
                          SQLITE_TRANSIENT);  // No dictionary
        sqlite3_bind_int64(
            st_checkpoint, 9,
            static_cast<int64_t>(total_lines));  // Total lines in file
        sqlite3_step(st_checkpoint);
        sqlite3_reset(st_checkpoint);

        spdlog::debug(
            "Created start checkpoint {}: uc_offset={}, size={} bytes (no "
            "dictionary)",
            checkpoint_idx, checkpoint_start_uc_offset, checkpoint_uc_size);
      } else {
        // Try to create a regular checkpoint with dictionary if possible
        CheckpointData checkpoint_data;
        if (create_checkpoint(&inflate_state, &checkpoint_data,
                              current_uc_offset) == 0) {
          unsigned char *compressed_dict = nullptr;
          size_t compressed_dict_size = 0;

          if (compress_window(checkpoint_data.window,
                              constants::ZLIB_WINDOW_SIZE, &compressed_dict,
                              &compressed_dict_size) == 0) {
            sqlite3_bind_int(st_checkpoint, 1, file_id);
            sqlite3_bind_int64(st_checkpoint, 2,
                               static_cast<int64_t>(checkpoint_idx));
            sqlite3_bind_int64(
                st_checkpoint, 3,
                static_cast<int64_t>(checkpoint_start_uc_offset));
            sqlite3_bind_int64(st_checkpoint, 4,
                               static_cast<int64_t>(checkpoint_uc_size));
            sqlite3_bind_int64(st_checkpoint, 5,
                               static_cast<int64_t>(checkpoint_data.c_offset));
            sqlite3_bind_int64(st_checkpoint, 6,
                               static_cast<int64_t>(checkpoint_data.c_offset));
            sqlite3_bind_int(st_checkpoint, 7, checkpoint_data.bits);
            sqlite3_bind_blob(st_checkpoint, 8, compressed_dict,
                              static_cast<int>(compressed_dict_size),
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(
                st_checkpoint, 9,
                static_cast<int64_t>(0));  // Approximate line count
            sqlite3_step(st_checkpoint);
            sqlite3_reset(st_checkpoint);

            spdlog::debug(
                "Created final checkpoint {}: uc_offset={}, size={} bytes",
                checkpoint_idx, checkpoint_start_uc_offset, checkpoint_uc_size);

            free(compressed_dict);
          }
        }
      }
      checkpoint_idx++;
    }

    inflate_cleanup_simple(&inflate_state);
    total_lines_out = total_lines;
    total_uc_size_out = current_uc_offset;
    spdlog::debug(
        "Indexing complete: created {} checkpoints, {} total lines, {} total "
        "UC bytes",
        checkpoint_idx, total_lines, current_uc_offset);
    return 0;
  } catch (const std::exception &e) {
    spdlog::error("Error during checkpoint processing: {}", e.what());
    return -3;
  }
}

bool Indexer::Impl::get_stored_file_info(const std::string &idx_path,
                                         const std::string &gz_path,
                                         std::string &stored_sha256,
                                         time_t &stored_mtime) const {
  sqlite3 *db;
  if (sqlite3_open(idx_path.c_str(), &db) != SQLITE_OK) {
    return false;
  }

  sqlite3_stmt *stmt;
  const char *sql =
      "SELECT sha256_hex, mtime_unix FROM files WHERE logical_name = ? LIMIT 1";
  bool found = false;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, gz_path.c_str(), -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      const char *sha256_text =
          reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
      if (sha256_text) {
        stored_sha256 = sha256_text;
      }
      stored_mtime = static_cast<time_t>(sqlite3_column_int64(stmt, 1));
      found = true;
    }
    sqlite3_finalize(stmt);
  }

  sqlite3_close(db);
  return found;
}

uint64_t Indexer::Impl::file_size_bytes(const std::string &path) const {
  FILE *fp = fopen(path.c_str(), "rb");
  if (!fp) return UINT64_MAX;
  fseeko(fp, 0, SEEK_END);
  auto sz = static_cast<uint64_t>(ftello(fp));
  fclose(fp);
  return sz;
}

bool Indexer::Impl::need_rebuild() const {
  // Check if index exists and is valid
  if (!index_exists_and_valid(idx_path_)) {
    spdlog::info(
        "Index rebuild needed: index does not exist or is invalid in {}",
        idx_path_);
    return true;
  }

  // Check if checkpoint size differs
  // size_t existing_ckpt_size = get_existing_ckpt_size(idx_path_);
  // if (existing_ckpt_size > 0)
  // {
  //     size_t diff = std::abs(existing_ckpt_size - ckpt_size_);
  //     if (diff > 0.1)
  //     {
  //         // Allow small floating point differences
  //         spdlog::debug("Index rebuild needed: checkpoint size differs ({}
  //         bytes vs {} bytes)",
  //                       existing_ckpt_size, ckpt_size_);
  //         return true;
  //     }
  // }

  // Check if file content has changed using SHA256
  std::string stored_sha256;
  time_t stored_mtime;
  if (get_stored_file_info(idx_path_, gz_path_logical_path_, stored_sha256,
                           stored_mtime)) {
    // quick check using modification time as optimization
    // time_t current_mtime = get_file_mtime(indexer->gz_path);
    // if (current_mtime != stored_mtime && current_mtime > 0 && stored_mtime >
    // 0)
    // {
    //     spdlog::debug("Index rebuild needed: file modification time
    //     changed"); return 1;
    // }

    // If we have a stored SHA256, calculate current SHA256 and compare
    if (!stored_sha256.empty()) {
      std::string current_sha256 = calculate_file_sha256(gz_path_);
      if (current_sha256.empty()) {
        throw Indexer::Error(Indexer::Error::FILE_ERROR,
                             "Failed to calculate SHA256 for " + gz_path_);
      }

      if (current_sha256 != stored_sha256) {
        spdlog::info("Index rebuild needed: file SHA256 changed ({} vs {})",
                     current_sha256.substr(0, 16) + "...",
                     stored_sha256.substr(0, 16) + "...");
        return true;
      }
    } else {
      // No stored SHA256, this might be an old index format
      spdlog::info(
          "Index rebuild needed: no SHA256 stored in index (old format)");
      return true;
    }
  } else {
    // Could not get stored file info, assume rebuild needed
    spdlog::info(
        "Index rebuild needed: could not retrieve stored file information from "
        "{}",
        idx_path_);
    return true;
  }

  spdlog::debug("Index rebuild not needed: file content unchanged");
  return false;
}

int Indexer::Impl::init_schema(sqlite3 *db) const {
  char *errmsg = NULL;
  int rc = sqlite3_exec(db, constants::SQL_SCHEMA, NULL, NULL, &errmsg);
  if (rc != SQLITE_OK) {
    std::string error =
        "Failed to initialize database schema: " + std::string(errmsg);
    sqlite3_free(errmsg);
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR, error);
  }
  spdlog::debug("Schema init succeeded");
  return rc;
}

int Indexer::Impl::inflate_init_simple(InflateState *state, FILE *f) const {
  memset(state, 0, sizeof(*state));
  state->file = f;

  if (inflateInit2(&state->zs, constants::ZLIB_GZIP_WINDOW_BITS) != Z_OK) {
    return -1;
  }
  return 0;
}

void Indexer::Impl::inflate_cleanup_simple(InflateState *state) const {
  inflateEnd(&state->zs);
}

int Indexer::Impl::inflate_process_chunk(InflateState *state,
                                         unsigned char *out, size_t out_size,
                                         size_t *bytes_out,
                                         size_t *c_off) const {
  state->zs.next_out = out;
  state->zs.avail_out = static_cast<uInt>(out_size);
  *bytes_out = 0;

  while (state->zs.avail_out > 0) {
    if (state->zs.avail_in == 0) {
      size_t n = fread(state->in, 1, sizeof(state->in), state->file);
      if (n == 0) {
        break;
      }
      state->zs.next_in = state->in;
      state->zs.avail_in = static_cast<uInt>(n);
    }

    size_t c_pos_before =
        static_cast<size_t>(ftello(state->file)) - state->zs.avail_in;
    // Use Z_BLOCK to process one deflate block at a time (following zran
    // approach)
    int ret = inflate(&state->zs, Z_BLOCK);

    if (ret == Z_STREAM_END) {
      break;
    }
    if (ret != Z_OK) {
      return -1;
    }

    *c_off = c_pos_before;

    // Break early if we've processed at least some data and hit a block
    // boundary This allows us to check for checkpoint opportunities after each
    // block
    if (*bytes_out > 0 && (state->zs.data_type & 0xc0) == 0x80) {
      break;
    }
  }

  *bytes_out = out_size - state->zs.avail_out;
  return 0;
}

int Indexer::Impl::create_checkpoint(InflateState *state,
                                     CheckpointData *checkpoint,
                                     size_t uc_offset) const {
  checkpoint->uc_offset = uc_offset;

  // Get precise compressed position: file position minus unprocessed input
  size_t file_pos = static_cast<size_t>(ftello(state->file));
  size_t absolute_c_offset = file_pos - state->zs.avail_in;

  // Store absolute file position (as in original zran)
  checkpoint->c_offset = absolute_c_offset;

  // Get bit offset from zlib state (following zran approach)
  checkpoint->bits = state->zs.data_type & 7;

  // Try to get the sliding window dictionary from zlib
  // This contains the last 32KB of uncompressed data
  // Only attempt this when the zlib state is stable
  unsigned have = 0;
  if ((state->zs.data_type & 0xc0) == 0x80 && state->zs.avail_out == 0 &&
      inflateGetDictionary(&state->zs, checkpoint->window, &have) == Z_OK &&
      have > 0) {
    // Got dictionary successfully
    if (have < constants::ZLIB_WINDOW_SIZE) {
      // If less than 32KB available, right-align and pad with zeros
      memmove(checkpoint->window + (constants::ZLIB_WINDOW_SIZE - have),
              checkpoint->window, have);
      memset(checkpoint->window, 0, constants::ZLIB_WINDOW_SIZE - have);
    }

    spdlog::debug(
        "Created checkpoint: uc_offset={}, c_offset={}, bits={}, dict_size={}",
        uc_offset, checkpoint->c_offset, checkpoint->bits, have);
    return 0;
  }

  // If we can't get dictionary from zlib, this checkpoint won't work
  spdlog::debug("Could not get dictionary for checkpoint at offset {}",
                uc_offset);
  return -1;
}

int Indexer::Impl::compress_window(const unsigned char *window,
                                   size_t window_size,
                                   unsigned char **compressed,
                                   size_t *compressed_size) const {
  z_stream zs;
  memset(&zs, 0, sizeof(zs));

  if (deflateInit(&zs, Z_BEST_COMPRESSION) != Z_OK) {
    return -1;
  }

  size_t max_compressed = deflateBound(&zs, window_size);
  *compressed = static_cast<unsigned char *>(malloc(max_compressed));
  if (!*compressed) {
    deflateEnd(&zs);
    return -1;
  }

  zs.next_in = const_cast<unsigned char *>(window);
  zs.avail_in = static_cast<uInt>(window_size);
  zs.next_out = *compressed;
  zs.avail_out = static_cast<uInt>(max_compressed);

  int ret = deflate(&zs, Z_FINISH);
  if (ret != Z_STREAM_END) {
    free(*compressed);
    deflateEnd(&zs);
    return -1;
  }

  *compressed_size = max_compressed - zs.avail_out;
  deflateEnd(&zs);
  return 0;
}

int Indexer::Impl::save_checkpoint(sqlite3 *db, int file_id,
                                   const CheckpointData *checkpoint) const {
  unsigned char *compressed_window;
  size_t compressed_size;

  if (compress_window(checkpoint->window, 32768, &compressed_window,
                      &compressed_size) != 0) {
    spdlog::debug("Failed to compress window for checkpoint");
    return -1;
  }

  sqlite3_stmt *stmt;
  const char *sql =
      "INSERT INTO checkpoints(file_id, uc_offset, c_offset, bits, "
      "dict_compressed) VALUES(?, ?, ?, ?, ?)";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    spdlog::debug("Failed to prepare checkpoint insert: {}",
                  sqlite3_errmsg(db));
    free(compressed_window);
    return -1;
  }

  sqlite3_bind_int(stmt, 1, file_id);
  sqlite3_bind_int64(stmt, 2,
                     static_cast<sqlite3_int64>(checkpoint->uc_offset));
  sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(checkpoint->c_offset));
  sqlite3_bind_int(stmt, 4, checkpoint->bits);
  sqlite3_bind_blob(stmt, 5, compressed_window,
                    static_cast<int>(compressed_size), SQLITE_TRANSIENT);

  int ret = sqlite3_step(stmt);
  if (ret != SQLITE_DONE) {
    spdlog::debug("Failed to insert checkpoint: {} - {}", ret,
                  sqlite3_errmsg(db));
  } else {
    spdlog::debug(
        "Successfully inserted checkpoint into database: uc_offset={}",
        checkpoint->uc_offset);
  }

  sqlite3_finalize(stmt);
  free(compressed_window);

  return (ret == SQLITE_DONE) ? 0 : -1;
}

int Indexer::Impl::build_index_internal(sqlite3 *db, int file_id,
                                        const std::string &gz_path,
                                        size_t ckpt_size) const {
  FILE *fp = fopen(gz_path.c_str(), "rb");
  if (!fp) return -1;

  sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);

  // Clean up existing data for this file before rebuilding
  if (cleanup_existing_data(db, file_id) != 0) {
    fclose(fp);
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return -2;
  }

  // Process chunks and get total line count and uncompressed size
  uint64_t total_lines = 0;
  uint64_t total_uc_size = 0;
  int result =
      process_chunks(fp, db, file_id, ckpt_size, total_lines, total_uc_size);
  fclose(fp);

  if (result != 0) {
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return result;
  }

  // Insert metadata with total_lines and total_uc_size
  if (insert_metadata(db, file_id, ckpt_size, total_lines, total_uc_size) !=
      0) {
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return -2;
  }

  sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
  return 0;
}

uint64_t Indexer::Impl::get_max_bytes() const {
  if (!index_exists_and_valid(idx_path_)) {
    return 0;
  }

  sqlite3 *db;
  if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
    throw Indexer::Error(
        Indexer::Error::DATABASE_ERROR,
        "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
  }

  sqlite3_stmt *stmt;
  const char *sql =
      "SELECT MAX(uc_offset + uc_size) FROM checkpoints WHERE file_id = "
      "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)";
  uint64_t max_bytes = 0;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, gz_path_logical_path_.c_str(), -1,
                      SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      max_bytes = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
    }
    sqlite3_finalize(stmt);
  }

  // If no checkpoints exist (max_bytes is 0), fall back to metadata table
  if (max_bytes == 0) {
    const char *metadata_sql =
        "SELECT total_uc_size FROM metadata WHERE file_id = "
        "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)";
    if (sqlite3_prepare_v2(db, metadata_sql, -1, &stmt, nullptr) == SQLITE_OK) {
      sqlite3_bind_text(stmt, 1, gz_path_logical_path_.c_str(), -1,
                        SQLITE_STATIC);
      if (sqlite3_step(stmt) == SQLITE_ROW) {
        max_bytes = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
        spdlog::debug("No checkpoints found, using metadata total_uc_size: {}",
                      max_bytes);
      }
      sqlite3_finalize(stmt);
    }
  }

  sqlite3_close(db);
  return max_bytes;
}

uint64_t Indexer::Impl::get_num_lines() const {
  if (!index_exists_and_valid(idx_path_)) {
    return 0;
  }

  sqlite3 *db;
  if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
    throw Indexer::Error(
        Indexer::Error::DATABASE_ERROR,
        "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
  }

  sqlite3_stmt *stmt;
  const char *sql =
      "SELECT total_lines FROM metadata WHERE file_id = "
      "(SELECT id FROM files WHERE logical_name = ? LIMIT 1)";
  uint64_t total_lines = 0;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, gz_path_logical_path_.c_str(), -1,
                      SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      total_lines = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
    }
    sqlite3_finalize(stmt);
  }

  sqlite3_close(db);
  return total_lines;
}

int Indexer::Impl::find_file_id(const std::string &gz_path) const {
  if (!index_exists_and_valid(idx_path_)) {
    return -1;
  }

  sqlite3 *db;
  if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
    throw Indexer::Error(
        Indexer::Error::DATABASE_ERROR,
        "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
  }

  sqlite3_stmt *stmt;
  const char *sql = "SELECT id FROM files WHERE logical_name = ? LIMIT 1";
  int file_id = -1;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
    std::string logical_path = get_logical_path(gz_path);
    sqlite3_bind_text(stmt, 1, logical_path.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      file_id = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
  } else {
    sqlite3_close(db);
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                         "Failed to prepare find_file_id statement: " +
                             std::string(sqlite3_errmsg(db)));
  }

  sqlite3_close(db);
  return file_id;
}

bool Indexer::Impl::find_checkpoint(size_t target_offset,
                                    CheckpointInfo &checkpoint) const {
  if (!index_exists_and_valid(idx_path_)) {
    return false;
  }

  // For target offset 0, always decompress from beginning of file (no
  // checkpoint)
  if (target_offset == 0) {
    return false;
  }

  int file_id = get_file_id();
  if (file_id == -1) {
    return false;
  }

  sqlite3 *db;
  if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
    throw Indexer::Error(
        Indexer::Error::DATABASE_ERROR,
        "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
  }

  sqlite3_stmt *stmt;
  const char *sql =
      "SELECT checkpoint_idx, uc_offset, uc_size, c_offset, c_size, bits, "
      "dict_compressed, num_lines "
      "FROM checkpoints WHERE file_id = ? AND uc_offset <= ? "
      "ORDER BY uc_offset DESC LIMIT 1";
  bool found = false;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
    sqlite3_bind_int(stmt, 1, file_id);
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(target_offset));

    if (sqlite3_step(stmt) == SQLITE_ROW) {
      checkpoint.checkpoint_idx =
          static_cast<size_t>(sqlite3_column_int64(stmt, 0));
      checkpoint.uc_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 1));
      checkpoint.uc_size = static_cast<size_t>(sqlite3_column_int64(stmt, 2));
      checkpoint.c_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 3));
      checkpoint.c_size = static_cast<size_t>(sqlite3_column_int64(stmt, 4));
      checkpoint.bits = sqlite3_column_int(stmt, 5);

      size_t dict_size = static_cast<size_t>(sqlite3_column_bytes(stmt, 6));
      checkpoint.dict_compressed.resize(dict_size);
      std::memcpy(checkpoint.dict_compressed.data(),
                  sqlite3_column_blob(stmt, 6), dict_size);

      checkpoint.num_lines = static_cast<size_t>(sqlite3_column_int64(stmt, 7));

      found = true;
    }
    sqlite3_finalize(stmt);
  } else {
    sqlite3_close(db);
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                         "Failed to prepare find_checkpoint statement: " +
                             std::string(sqlite3_errmsg(db)));
  }

  sqlite3_close(db);
  return found;
}

std::vector<CheckpointInfo> Indexer::Impl::get_checkpoints() const {
  std::vector<CheckpointInfo> checkpoints;

  if (!index_exists_and_valid(idx_path_)) {
    return checkpoints;
  }

  sqlite3 *db;
  if (sqlite3_open(idx_path_.c_str(), &db) != SQLITE_OK) {
    throw Indexer::Error(
        Indexer::Error::DATABASE_ERROR,
        "Cannot open index database: " + std::string(sqlite3_errmsg(db)));
  }

  sqlite3_stmt *stmt;
  // Query unified checkpoints table
  const char *sql =
      "SELECT checkpoint_idx, uc_offset, uc_size, c_offset, c_size, bits, "
      "dict_compressed, num_lines "
      "FROM checkpoints WHERE file_id = ? ORDER BY uc_offset";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
    sqlite3_bind_int(stmt, 1, get_file_id());

    while (sqlite3_step(stmt) == SQLITE_ROW) {
      CheckpointInfo checkpoint;
      checkpoint.checkpoint_idx =
          static_cast<size_t>(sqlite3_column_int64(stmt, 0));
      checkpoint.uc_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 1));
      checkpoint.uc_size = static_cast<size_t>(sqlite3_column_int64(stmt, 2));
      checkpoint.c_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 3));
      checkpoint.c_size = static_cast<size_t>(sqlite3_column_int64(stmt, 4));
      checkpoint.bits = sqlite3_column_int(stmt, 5);

      size_t dict_size = static_cast<size_t>(sqlite3_column_bytes(stmt, 6));
      checkpoint.dict_compressed.resize(dict_size);
      std::memcpy(checkpoint.dict_compressed.data(),
                  sqlite3_column_blob(stmt, 6), dict_size);

      checkpoint.num_lines = static_cast<size_t>(sqlite3_column_int64(stmt, 7));

      checkpoints.push_back(std::move(checkpoint));
    }

    sqlite3_finalize(stmt);
  } else {
    sqlite3_close(db);
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                         "Failed to prepare get_checkpoints statement: " +
                             std::string(sqlite3_errmsg(db)));
  }

  sqlite3_close(db);
  return checkpoints;
}

int Indexer::Impl::get_file_id() const {
  if (cached_file_id_ != -1) {
    return cached_file_id_;
  }

  cached_file_id_ = find_file_id(gz_path_);
  return cached_file_id_;
}

std::vector<CheckpointInfo> Indexer::Impl::find_checkpoints_by_line_range(
    size_t start_line, size_t end_line) const {
  std::vector<CheckpointInfo> checkpoints;

  if (!index_exists_and_valid(idx_path_)) {
    return checkpoints;
  }

  if (start_line == 0 || end_line == 0 || start_line > end_line) {
    throw Indexer::Error(
        Indexer::Error::INVALID_ARGUMENT,
        "Invalid line range: start_line and end_line must be > 0 and "
        "start_line <= end_line");
  }

  // For line-based reading, we need to start from the beginning and decompress
  // sequentially Use the original approach from reader.cpp that handles line
  // counting during decompression

  // For now, return all checkpoints in order - the reader will handle line
  // counting
  return get_checkpoints();
}

void Indexer::Impl::build() {
  spdlog::debug("Building index for {} with {} bytes ({:.1f} MB) chunks...",
                gz_path_, ckpt_size_,
                static_cast<double>(ckpt_size_) / (1024 * 1024));

  // If force rebuild is enabled, delete the existing database file to ensure
  // clean schema
  if (force_rebuild_ && fs::exists(idx_path_)) {
    spdlog::debug("Force rebuild enabled, removing existing index file: {}",
                  idx_path_);
    if (!fs::remove(idx_path_)) {
      spdlog::warn("Failed to remove existing index file: {}", idx_path_);
    }
  }

  // open database
  if (sqlite3_open(idx_path_.c_str(), &db_) != SQLITE_OK) {
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                         "Cannot create/open database " + idx_path_ + ": " +
                             sqlite3_errmsg(db_));
  }
  db_opened_ = true;

  // initialize schema
  if (init_schema(db_) != SQLITE_OK) {
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                         "Failed to initialize database schema");
  }

  // get file info
  uint64_t bytes = file_size_bytes(gz_path_);
  if (bytes == UINT64_MAX) {
    throw Indexer::Error(Indexer::Error::FILE_ERROR, "Cannot stat " + gz_path_);
  }

  // calculate SHA256 and get modification time
  std::string file_sha256 = calculate_file_sha256(gz_path_);
  if (file_sha256.empty()) {
    throw Indexer::Error(Indexer::Error::FILE_ERROR,
                         "Failed to calculate SHA256 for " + gz_path_);
  }

  time_t file_mtime = get_file_mtime(gz_path_);

  spdlog::debug("File info: size={} bytes, mtime={}, sha256={}...", bytes,
                file_mtime, file_sha256.substr(0, 16));

  // insert/update file record
  sqlite3_stmt *st;
  if (sqlite3_prepare_v2(
          db_,
          "INSERT INTO files(logical_name, byte_size, mtime_unix, sha256_hex) "
          "VALUES(?, ?, ?, ?) "
          "ON CONFLICT(logical_name) DO UPDATE SET "
          "byte_size=excluded.byte_size, "
          "mtime_unix=excluded.mtime_unix, "
          "sha256_hex=excluded.sha256_hex "
          "RETURNING id;",
          -1, &st, NULL) != SQLITE_OK) {
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                         "Prepare failed: " + std::string(sqlite3_errmsg(db_)));
  }

  sqlite3_bind_text(st, 1, gz_path_logical_path_.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(bytes));
  sqlite3_bind_int64(st, 3, static_cast<sqlite3_int64>(file_mtime));
  sqlite3_bind_text(st, 4, file_sha256.c_str(), -1, SQLITE_TRANSIENT);

  int rc = sqlite3_step(st);
  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    sqlite3_finalize(st);
    throw Indexer::Error(Indexer::Error::DATABASE_ERROR,
                         "Insert failed: " + std::string(sqlite3_errmsg(db_)));
  }

  int db_file_id = sqlite3_column_int(st, 0);
  sqlite3_finalize(st);

  spdlog::debug("Building index with stride: {} bytes ({:.1f} MB)", ckpt_size_,
                static_cast<double>(ckpt_size_) / (1024 * 1024));

  int ret = build_index_internal(db_, db_file_id, gz_path_, ckpt_size_);
  if (ret != 0) {
    throw Indexer::Error(
        Indexer::Error::BUILD_ERROR,
        "Index build failed with error code: " + std::to_string(ret));
  }

  spdlog::debug("Index built successfully for {}", gz_path_);
}

}  // namespace indexer
}  // namespace utils
}  // namespace dftracer

// ==============================================================================
// C++ Public Interface Implementation
// ==============================================================================

namespace dftracer {
namespace utils {
namespace indexer {

Indexer::Indexer(const std::string &gz_path, const std::string &idx_path,
                 size_t ckpt_size, bool force_rebuild)
    : pImpl_(new Impl(gz_path, idx_path, ckpt_size, force_rebuild)) {}

Indexer::~Indexer() = default;

Indexer::Indexer(Indexer &&other) noexcept : pImpl_(other.pImpl_.release()) {}

Indexer &Indexer::operator=(Indexer &&other) noexcept {
  if (this != &other) {
    pImpl_.reset(other.pImpl_.release());
  }
  return *this;
}

void Indexer::build() { pImpl_->build(); }

bool Indexer::need_rebuild() const { return pImpl_->need_rebuild(); }

bool Indexer::is_valid() const { return pImpl_ && pImpl_->is_valid(); }

const std::string &Indexer::get_gz_path() const {
  return pImpl_->get_gz_path();
}

const std::string &Indexer::get_idx_path() const {
  return pImpl_->get_idx_path();
}

size_t Indexer::get_checkpoint_size() const {
  return pImpl_->get_checkpoint_size();
}

uint64_t Indexer::get_max_bytes() const { return pImpl_->get_max_bytes(); }

uint64_t Indexer::get_num_lines() const { return pImpl_->get_num_lines(); }

int Indexer::find_file_id(const std::string &gz_path) const {
  return pImpl_->find_file_id(gz_path);
}

bool Indexer::find_checkpoint(size_t target_offset,
                              CheckpointInfo &checkpoint) const {
  return pImpl_->find_checkpoint(target_offset, checkpoint);
}

std::vector<CheckpointInfo> Indexer::get_checkpoints() const {
  return pImpl_->get_checkpoints();
}

std::vector<CheckpointInfo> Indexer::find_checkpoints_by_line_range(
    size_t start_line, size_t end_line) const {
  return pImpl_->find_checkpoints_by_line_range(start_line, end_line);
}

// ==============================================================================
// Error Class Implementation
// ==============================================================================

std::string Indexer::Error::format_message(Type type,
                                           const std::string &message) {
  const char *prefix = "";
  switch (type) {
    case DATABASE_ERROR:
      prefix = "Database error";
      break;
    case FILE_ERROR:
      prefix = "File error";
      break;
    case COMPRESSION_ERROR:
      prefix = "Compression error";
      break;
    case INVALID_ARGUMENT:
      prefix = "Invalid argument";
      break;
    case BUILD_ERROR:
      prefix = "Build error";
      break;
    case UNKNOWN_ERROR:
      prefix = "Unknown error";
      break;
  }
  return std::string(prefix) + ": " + message;
}

}  // namespace indexer
}  // namespace utils
}  // namespace dftracer

// ==============================================================================
// C API Implementation (wraps C++ implementation)
// ==============================================================================

extern "C" {

// Helper functions for C API
static int validate_handle(dft_indexer_handle_t indexer) {
  return indexer ? 0 : -1;
}

static dftracer::utils::indexer::Indexer *cast_indexer(
    dft_indexer_handle_t indexer) {
  return static_cast<dftracer::utils::indexer::Indexer *>(indexer);
}

dft_indexer_handle_t dft_indexer_create(const char *gz_path,
                                        const char *idx_path,
                                        size_t checkpoint_size,
                                        int force_rebuild) {
  if (!gz_path || !idx_path || checkpoint_size == 0) {
    spdlog::error("Invalid parameters for indexer creation");
    return nullptr;
  }

  try {
    auto *indexer = new dftracer::utils::indexer::Indexer(
        gz_path, idx_path, checkpoint_size, force_rebuild != 0);
    return static_cast<dft_indexer_handle_t>(indexer);
  } catch (const std::exception &e) {
    spdlog::error("Failed to create DFT indexer: {}", e.what());
    return nullptr;
  }
}

int dft_indexer_build(dft_indexer_handle_t indexer) {
  if (validate_handle(indexer)) {
    return -1;
  }

  try {
    cast_indexer(indexer)->build();
    return 0;
  } catch (const std::exception &e) {
    spdlog::error("Failed to build index: {}", e.what());
    return -1;
  }
}

int dft_indexer_need_rebuild(dft_indexer_handle_t indexer) {
  if (validate_handle(indexer)) {
    return -1;
  }

  try {
    return cast_indexer(indexer)->need_rebuild() ? 1 : 0;
  } catch (const std::exception &e) {
    spdlog::error("Failed to check if rebuild is needed: {}", e.what());
    return -1;
  }
}

uint64_t dft_indexer_get_max_bytes(dft_indexer_handle_t indexer) {
  if (validate_handle(indexer)) {
    return 0;
  }

  try {
    return cast_indexer(indexer)->get_max_bytes();
  } catch (const std::exception &e) {
    spdlog::error("Failed to get max bytes: {}", e.what());
    return 0;
  }
}

uint64_t dft_indexer_get_num_lines(dft_indexer_handle_t indexer) {
  if (validate_handle(indexer)) {
    return 0;
  }

  try {
    return cast_indexer(indexer)->get_num_lines();
  } catch (const std::exception &e) {
    spdlog::error("Failed to get number of lines: {}", e.what());
    return 0;
  }
}

int dft_indexer_find_file_id(dft_indexer_handle_t indexer,
                             const char *gz_path) {
  if (validate_handle(indexer) || !gz_path) {
    return -1;
  }

  try {
    return cast_indexer(indexer)->find_file_id(gz_path);
  } catch (const std::exception &e) {
    spdlog::error("Failed to find file ID: {}", e.what());
    return -1;
  }
}

int dft_indexer_find_checkpoint(dft_indexer_handle_t indexer,
                                size_t target_offset,
                                dft_indexer_checkpoint_info_t *checkpoint) {
  if (validate_handle(indexer) || !checkpoint) {
    return -1;
  }

  try {
    dftracer::utils::indexer::CheckpointInfo temp_ckpt;

    if (cast_indexer(indexer)->find_checkpoint(
            static_cast<size_t>(target_offset), temp_ckpt)) {
      checkpoint->uc_offset = static_cast<uint64_t>(temp_ckpt.uc_offset);
      checkpoint->c_offset = static_cast<uint64_t>(temp_ckpt.c_offset);
      checkpoint->bits = temp_ckpt.bits;
      checkpoint->dict_size = temp_ckpt.dict_compressed.size();

      checkpoint->dict_compressed =
          static_cast<unsigned char *>(malloc(checkpoint->dict_size));
      if (checkpoint->dict_compressed) {
        std::memcpy(checkpoint->dict_compressed,
                    temp_ckpt.dict_compressed.data(), checkpoint->dict_size);
        return 1;
      }
      return -1;
    }
    return 0;
  } catch (const std::exception &e) {
    spdlog::error("Failed to find checkpoint: {}", e.what());
    return -1;
  }
}

int dft_indexer_get_checkpoints(dft_indexer_handle_t indexer,
                                dft_indexer_checkpoint_info_t **checkpoints,
                                size_t *count) {
  if (validate_handle(indexer) || !count || !checkpoints) {
    return -1;
  }

  try {
    auto ckpts = cast_indexer(indexer)->get_checkpoints();

    auto temp_count = ckpts.size();
    if (temp_count == 0) {
      return 0;
    }

    auto temp_ckpts = static_cast<dft_indexer_checkpoint_info_t *>(
        malloc(temp_count * sizeof(dft_indexer_checkpoint_info_t)));
    if (!temp_ckpts) {
      return -1;
    }

    *checkpoints = temp_ckpts;

    // Initialize checkpoint information
    for (size_t i = 0; i < temp_count; i++) {
      const auto &checkpoint = ckpts[i];

      temp_ckpts[i].checkpoint_idx =
          static_cast<uint64_t>(checkpoint.checkpoint_idx);
      temp_ckpts[i].uc_offset = static_cast<uint64_t>(checkpoint.uc_offset);
      temp_ckpts[i].uc_size = static_cast<uint64_t>(checkpoint.uc_size);
      temp_ckpts[i].c_offset = static_cast<uint64_t>(checkpoint.c_offset);
      temp_ckpts[i].c_size = static_cast<uint64_t>(checkpoint.c_size);
      temp_ckpts[i].bits = checkpoint.bits;
      temp_ckpts[i].dict_size = checkpoint.dict_compressed.size();
      temp_ckpts[i].num_lines = static_cast<uint64_t>(checkpoint.num_lines);

      // Allocate and copy dictionary data
      temp_ckpts[i].dict_compressed =
          static_cast<unsigned char *>(malloc(temp_ckpts[i].dict_size));
      if (temp_ckpts[i].dict_compressed) {
        std::memcpy(temp_ckpts[i].dict_compressed,
                    checkpoint.dict_compressed.data(), temp_ckpts[i].dict_size);
      } else {
        // Clean up on allocation failure
        for (size_t j = 0; j < i; j++) {
          free(temp_ckpts[j].dict_compressed);
        }
        free(temp_ckpts);
        spdlog::error(
            "Failed to allocate memory for checkpoint dictionary data");
        *checkpoints = nullptr;
        return -1;
      }
    }

    *count = temp_count;
    *checkpoints = temp_ckpts;
    return 0;
  } catch (const std::exception &e) {
    spdlog::error("Failed to get checkpoints: {}", e.what());
    return -1;
  }
}

void dft_indexer_free_checkpoint(dft_indexer_checkpoint_info_t *checkpoint) {
  if (checkpoint) {
    free(checkpoint->dict_compressed);
    free(checkpoint);
  }
}

void dft_indexer_free_checkpoints(dft_indexer_checkpoint_info_t *checkpoints,
                                  size_t count) {
  if (checkpoints) {
    for (size_t i = 0; i < count; i++) {
      free(checkpoints[i].dict_compressed);
    }
    free(checkpoints);
  }
}

void dft_indexer_destroy(dft_indexer_handle_t indexer) {
  if (indexer) {
    delete static_cast<dftracer::utils::indexer::Indexer *>(indexer);
  }
}

}  // extern "C"
