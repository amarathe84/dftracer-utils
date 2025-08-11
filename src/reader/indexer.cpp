#include "indexer.h"
#include "platform_compat.h"

#include <cstdlib>
#include <cstring>
#include <cmath>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <sqlite3.h>
#include <zlib.h>
#include <sys/stat.h>
#include <picosha2.h>
#include <fstream>
#include <vector>

namespace dft {
namespace indexer {

class Indexer::Impl {
public:
    Impl(const std::string& gz_path, const std::string& idx_path, double chunk_size_mb, bool force_rebuild)
        : gz_path_(gz_path), idx_path_(idx_path), chunk_size_mb_(chunk_size_mb), force_rebuild_(force_rebuild), db_(nullptr), db_opened_(false)
    {
        if (chunk_size_mb <= 0) {
            throw std::invalid_argument("chunk_size_mb must be greater than 0");
        }
        spdlog::debug("Created DFT indexer for gz: {} and index: {}", gz_path, idx_path);
    }

    ~Impl() {
        if (db_opened_ && db_) {
            sqlite3_close(db_);
        }
        spdlog::debug("Successfully destroyed DFT indexer");
    }

    // Disable copy
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    // Enable move
    Impl(Impl&& other) noexcept
        : gz_path_(std::move(other.gz_path_))
        , idx_path_(std::move(other.idx_path_))
        , chunk_size_mb_(other.chunk_size_mb_)
        , force_rebuild_(other.force_rebuild_)
        , db_(other.db_)
        , db_opened_(other.db_opened_)
    {
        other.db_ = nullptr;
        other.db_opened_ = false;
    }

    Impl& operator=(Impl&& other) noexcept {
        if (this != &other) {
            if (db_opened_ && db_) {
                sqlite3_close(db_);
            }
            gz_path_ = std::move(other.gz_path_);
            idx_path_ = std::move(other.idx_path_);
            chunk_size_mb_ = other.chunk_size_mb_;
            force_rebuild_ = other.force_rebuild_;
            db_ = other.db_;
            db_opened_ = other.db_opened_;
            other.db_ = nullptr;
            other.db_opened_ = false;
        }
        return *this;
    }

    bool need_rebuild() const;
    void build();

    bool is_valid() const { return true; } // Always valid after construction
    const std::string& get_gz_path() const { return gz_path_; }
    const std::string& get_idx_path() const { return idx_path_; }
    double get_chunk_size_mb() const { return chunk_size_mb_; }

private:
    static const char* SQL_SCHEMA;

    struct InflateState {
        z_stream zs;
        FILE *file;
        unsigned char in[16384];
    };

    // Helper methods
    std::string calculate_file_sha256(const std::string& file_path) const;
    time_t get_file_mtime(const std::string& file_path) const;
    bool index_exists_and_valid(const std::string& idx_path) const;
    double get_existing_chunk_size_mb(const std::string& idx_path) const;
    bool get_stored_file_info(const std::string& idx_path, const std::string& gz_path, std::string& stored_sha256, time_t& stored_mtime) const;
    uint64_t file_size_bytes(const std::string& path) const;
    int init_schema(sqlite3* db) const;
    int build_index_internal(sqlite3* db, int file_id, const std::string& gz_path, size_t chunk_size) const;
    
    int inflate_init_simple(InflateState* state, FILE* f) const;
    void inflate_cleanup_simple(InflateState* state) const;
    int inflate_process_chunk(InflateState* state, unsigned char* out, size_t out_size, size_t* bytes_out, size_t* c_off) const;

    std::string gz_path_;
    std::string idx_path_;
    double chunk_size_mb_;
    bool force_rebuild_;
    sqlite3* db_;
    bool db_opened_;
};

const char* Indexer::Impl::SQL_SCHEMA = 
    "CREATE TABLE IF NOT EXISTS files ("
    "  id INTEGER PRIMARY KEY,"
    "  logical_name TEXT UNIQUE NOT NULL,"
    "  byte_size INTEGER NOT NULL,"
    "  mtime_unix INTEGER NOT NULL,"
    "  sha256_hex TEXT NOT NULL"
    ");"

    "CREATE TABLE IF NOT EXISTS chunks ("
    "  id INTEGER PRIMARY KEY,"
    "  file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,"
    "  chunk_idx INTEGER NOT NULL,"
    "  compressed_offset INTEGER NOT NULL,"
    "  compressed_size INTEGER NOT NULL,"
    "  uncompressed_offset INTEGER NOT NULL,"
    "  uncompressed_size INTEGER NOT NULL,"
    "  num_events INTEGER NOT NULL"
    ");"

    "CREATE INDEX IF NOT EXISTS chunks_file_idx ON "
    "chunks(file_id, chunk_idx);"
    "CREATE INDEX IF NOT EXISTS chunks_file_uc_off_idx ON "
    "chunks(file_id, uncompressed_offset);"

    "CREATE TABLE IF NOT EXISTS metadata ("
    "  file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,"
    "  chunk_size INTEGER NOT NULL,"
    "  PRIMARY KEY(file_id)"
    ");";

std::string Indexer::Impl::calculate_file_sha256(const std::string& file_path) const {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        spdlog::error("Cannot open file for SHA256 calculation: {}", file_path);
        return "";
    }

    std::vector<unsigned char> buffer(8192);
    picosha2::hash256_one_by_one hasher;
    hasher.init();

    while (file.read(reinterpret_cast<char*>(buffer.data()), buffer.size()) || file.gcount() > 0) {
        hasher.process(buffer.begin(), buffer.begin() + file.gcount());
    }

    hasher.finish();
    std::string hex_str;
    picosha2::get_hash_hex_string(hasher, hex_str);
    return hex_str;
}

time_t Indexer::Impl::get_file_mtime(const std::string& file_path) const {
    struct stat st;
    if (stat(file_path.c_str(), &st) == 0) {
        return st.st_mtime;
    }
    return 0;
}

bool Indexer::Impl::index_exists_and_valid(const std::string& idx_path) const {
    FILE *f = fopen(idx_path.c_str(), "rb");
    if (!f) return false;
    fclose(f);

    sqlite3 *db;
    if (sqlite3_open(idx_path.c_str(), &db) != SQLITE_OK) {
        return false;
    }

    sqlite3_stmt *stmt;
    const char *sql = "SELECT name FROM sqlite_master WHERE type='table' AND "
                      "name IN ('chunks', 'metadata', 'files')";
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

double Indexer::Impl::get_existing_chunk_size_mb(const std::string& idx_path) const {
    sqlite3 *db;
    if (sqlite3_open(idx_path.c_str(), &db) != SQLITE_OK) {
        return -1;
    }

    sqlite3_stmt *stmt;
    const char *sql = "SELECT chunk_size FROM metadata LIMIT 1";
    double chunk_size_mb = -1;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            auto chunk_size_bytes = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
            chunk_size_mb = static_cast<double>(chunk_size_bytes) / (1024.0 * 1024.0);
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    return chunk_size_mb;
}

bool Indexer::Impl::get_stored_file_info(const std::string& idx_path, const std::string& gz_path, std::string& stored_sha256, time_t& stored_mtime) const {
    sqlite3 *db;
    if (sqlite3_open(idx_path.c_str(), &db) != SQLITE_OK) {
        return false;
    }

    sqlite3_stmt *stmt;
    const char *sql = "SELECT sha256_hex, mtime_unix FROM files WHERE logical_name = ? LIMIT 1";
    bool found = false;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, gz_path.c_str(), -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *sha256_text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
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

uint64_t Indexer::Impl::file_size_bytes(const std::string& path) const {
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
        spdlog::debug("Index rebuild needed: index does not exist or is invalid");
        return true;
    }

    // If force rebuild is set, always rebuild
    if (force_rebuild_) {
        spdlog::debug("Index rebuild needed: force rebuild is enabled");
        return true;
    }

    // Check if chunk size differs
    double existing_chunk_size = get_existing_chunk_size_mb(idx_path_);
    if (existing_chunk_size > 0) {
        double diff = std::abs(existing_chunk_size - chunk_size_mb_);
        if (diff > 0.1) { // Allow small floating point differences
            spdlog::debug("Index rebuild needed: chunk size differs ({:.1f} MB vs {:.1f} MB)", 
                         existing_chunk_size, chunk_size_mb_);
            return true;
        }
    }

    // Check if file content has changed using SHA256
    std::string stored_sha256;
    time_t stored_mtime;
    if (get_stored_file_info(idx_path_, gz_path_, stored_sha256, stored_mtime)) {
        // If we have a stored SHA256, calculate current SHA256 and compare
        if (!stored_sha256.empty()) {
            std::string current_sha256 = calculate_file_sha256(gz_path_);
            if (current_sha256.empty()) {
                throw std::runtime_error("Failed to calculate SHA256 for " + gz_path_);
            }

            if (current_sha256 != stored_sha256) {
                spdlog::debug("Index rebuild needed: file SHA256 changed ({} vs {})", 
                             current_sha256.substr(0, 16) + "...", 
                             stored_sha256.substr(0, 16) + "...");
                return true;
            }
        } else {
            // No stored SHA256, this might be an old index format
            spdlog::debug("Index rebuild needed: no SHA256 stored in index (old format)");
            return true;
        }
    } else {
        // Could not get stored file info, assume rebuild needed
        spdlog::debug("Index rebuild needed: could not retrieve stored file information");
        return true;
    }

    spdlog::debug("Index rebuild not needed: file content unchanged");
    return false;
}

int Indexer::Impl::init_schema(sqlite3 *db) const {
    char *errmsg = NULL;
    int rc = sqlite3_exec(db, SQL_SCHEMA, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        std::string error = "Failed to initialize database schema: " + std::string(errmsg);
        sqlite3_free(errmsg);
        throw std::runtime_error(error);
    }
    spdlog::debug("Schema init succeeded");
    return rc;
}

int Indexer::Impl::inflate_init_simple(InflateState *state, FILE *f) const {
    memset(state, 0, sizeof(*state));
    state->file = f;

    if (inflateInit2(&state->zs, 31) != Z_OK) {
        return -1;
    }
    return 0;
}

void Indexer::Impl::inflate_cleanup_simple(InflateState *state) const {
    inflateEnd(&state->zs);
}

int Indexer::Impl::inflate_process_chunk(InflateState *state, unsigned char *out, size_t out_size, size_t *bytes_out, size_t *c_off) const {
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

        size_t c_pos_before = static_cast<size_t>(ftello(state->file)) - state->zs.avail_in;
        int ret = inflate(&state->zs, Z_NO_FLUSH);

        if (ret == Z_STREAM_END) {
            break;
        }
        if (ret != Z_OK) {
            return -1;
        }

        *c_off = c_pos_before;
    }

    *bytes_out = out_size - state->zs.avail_out;
    return 0;
}

int Indexer::Impl::build_index_internal(sqlite3 *db, int file_id, const std::string& gz_path, size_t chunk_size) const {
    FILE *fp = fopen(gz_path.c_str(), "rb");
    if (!fp) return -1;

    sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);

    // Clean up existing data for this file before rebuilding
    sqlite3_stmt *st_cleanup_chunks = NULL;
    if (sqlite3_prepare_v2(db, "DELETE FROM chunks WHERE file_id = ?;", -1, &st_cleanup_chunks, NULL)) {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }
    sqlite3_bind_int(st_cleanup_chunks, 1, file_id);
    sqlite3_step(st_cleanup_chunks);
    sqlite3_finalize(st_cleanup_chunks);

    sqlite3_stmt *st_cleanup_metadata = NULL;
    if (sqlite3_prepare_v2(db, "DELETE FROM metadata WHERE file_id = ?;", -1, &st_cleanup_metadata, NULL)) {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }
    sqlite3_bind_int(st_cleanup_metadata, 1, file_id);
    sqlite3_step(st_cleanup_metadata);
    sqlite3_finalize(st_cleanup_metadata);

    sqlite3_stmt *st_meta = NULL;
    if (sqlite3_prepare_v2(db, "INSERT INTO metadata(file_id, chunk_size) VALUES(?, ?);", -1, &st_meta, NULL)) {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }

    sqlite3_bind_int(st_meta, 1, file_id);
    sqlite3_bind_int64(st_meta, 2, static_cast<int64_t>(chunk_size));
    sqlite3_step(st_meta);
    sqlite3_finalize(st_meta);

    sqlite3_stmt *st_chunk = NULL;
    if (sqlite3_prepare_v2(db,
                        "INSERT INTO chunks(file_id, chunk_idx, compressed_offset, "
                        "compressed_size, uncompressed_offset, uncompressed_size, num_events) "
                        "VALUES(?, ?, ?, ?, ?, ?, ?);",
                        -1, &st_chunk, NULL)) {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -3;
    }

    InflateState inflate_state;
    if (inflate_init_simple(&inflate_state, fp) != 0) {
        sqlite3_finalize(st_chunk);
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -4;
    }

    size_t chunk_idx = 0;
    size_t chunk_start_uc_off = 0;
    size_t chunk_start_c_off = 0;
    size_t current_uc_off = 0;
    size_t current_events = 0;
    unsigned char buffer[65536];
    int chunk_has_complete_event = 0;

    spdlog::info("Building chunk index with chunk_size={} bytes", chunk_size);

    while (1) {
        size_t bytes_read;
        size_t c_off;

        if (inflate_process_chunk(&inflate_state, buffer, sizeof(buffer), &bytes_read, &c_off) != 0) {
            break;
        }

        if (bytes_read == 0) {
            if (current_uc_off > chunk_start_uc_off) {
                size_t chunk_uc_size = current_uc_off - chunk_start_uc_off;
                size_t chunk_c_size = c_off - chunk_start_c_off;

                sqlite3_bind_int(st_chunk, 1, file_id);
                sqlite3_bind_int64(st_chunk, 2, static_cast<int64_t>(chunk_idx));
                sqlite3_bind_int64(st_chunk, 3, static_cast<int64_t>(chunk_start_c_off));
                sqlite3_bind_int64(st_chunk, 4, static_cast<int64_t>(chunk_c_size));
                sqlite3_bind_int64(st_chunk, 5, static_cast<int64_t>(chunk_start_uc_off));
                sqlite3_bind_int64(st_chunk, 6, static_cast<int64_t>(chunk_uc_size));
                sqlite3_bind_int64(st_chunk, 7, static_cast<int64_t>(current_events));

                if (sqlite3_step(st_chunk) != SQLITE_DONE) {
                    spdlog::error("Error inserting final chunk");
                    break;
                }
                sqlite3_reset(st_chunk);

                spdlog::debug("Final chunk {}: uc_off={}-{} ({} bytes), events={}",
                              chunk_idx, chunk_start_uc_off, current_uc_off, chunk_uc_size, current_events);
            }
            break;
        }

        size_t last_newline_pos = SIZE_MAX;
        for (size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                current_events++;
                chunk_has_complete_event = 1;
                last_newline_pos = i;
            }
        }

        current_uc_off += bytes_read;

        if ((current_uc_off - chunk_start_uc_off) >= chunk_size && chunk_has_complete_event &&
            last_newline_pos != SIZE_MAX) {
            // end current chunk at the last complete line boundary
            auto chunk_end_uc_off = current_uc_off - bytes_read + last_newline_pos + 1;
            size_t chunk_uc_size = chunk_end_uc_off - chunk_start_uc_off;
            size_t chunk_c_size = c_off - chunk_start_c_off;

            size_t chunk_events = current_events;

            sqlite3_bind_int(st_chunk, 1, file_id);
            sqlite3_bind_int64(st_chunk, 2, static_cast<int64_t>(chunk_idx));
            sqlite3_bind_int64(st_chunk, 3, static_cast<int64_t>(chunk_start_c_off));
            sqlite3_bind_int64(st_chunk, 4, static_cast<int64_t>(chunk_c_size));
            sqlite3_bind_int64(st_chunk, 5, static_cast<int64_t>(chunk_start_uc_off));
            sqlite3_bind_int64(st_chunk, 6, static_cast<int64_t>(chunk_uc_size));
            sqlite3_bind_int64(st_chunk, 7, static_cast<int64_t>(chunk_events));

            if (sqlite3_step(st_chunk) != SQLITE_DONE) {
                spdlog::error("Error inserting chunk {}", chunk_idx);
                break;
            }
            sqlite3_reset(st_chunk);

            spdlog::debug("Chunk {}: uc_off={}-{} ({} bytes), events={} (ended at line boundary)",
                          chunk_idx, chunk_start_uc_off, chunk_end_uc_off, chunk_uc_size, chunk_events);

            // start new chunk after the last complete line
            chunk_idx++;
            chunk_start_uc_off = chunk_end_uc_off;
            chunk_start_c_off = c_off;
            current_events = 0;
            chunk_has_complete_event = 0;

            // count remaining lines in buffer for next chunk
            for (size_t i = last_newline_pos + 1; i < bytes_read; i++) {
                if (buffer[i] == '\n') {
                    current_events++;
                    chunk_has_complete_event = 1;
                }
            }
        }
    }

    inflate_cleanup_simple(&inflate_state);
    sqlite3_finalize(st_chunk);
    fclose(fp);

    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    spdlog::info("Indexing complete: created {} chunks", chunk_idx + 1);
    return 0;
}

void Indexer::Impl::build() {
    // check if rebuild is needed
    bool rebuild_needed = need_rebuild();
    if (!rebuild_needed) {
        spdlog::info("Index is up to date, skipping rebuild");
        return;
    }

    spdlog::info("Building index for {} with {:.1f} MB chunks...", gz_path_, chunk_size_mb_);

    // open database
    if (sqlite3_open(idx_path_.c_str(), &db_) != SQLITE_OK) {
        throw std::runtime_error("Cannot create/open database " + idx_path_ + ": " + sqlite3_errmsg(db_));
    }
    db_opened_ = true;

    // initialize schema
    if (init_schema(db_) != SQLITE_OK) {
        throw std::runtime_error("Failed to initialize database schema");
    }

    // get file info
    uint64_t bytes = file_size_bytes(gz_path_);
    if (bytes == UINT64_MAX) {
        throw std::runtime_error("Cannot stat " + gz_path_);
    }

    // calculate SHA256 and get modification time
    std::string file_sha256 = calculate_file_sha256(gz_path_);
    if (file_sha256.empty()) {
        throw std::runtime_error("Failed to calculate SHA256 for " + gz_path_);
    }
    
    time_t file_mtime = get_file_mtime(gz_path_);

    spdlog::debug("File info: size={} bytes, mtime={}, sha256={}...", 
                 bytes, file_mtime, file_sha256.substr(0, 16));

    // insert/update file record
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db_,
                           "INSERT INTO files(logical_name, byte_size, mtime_unix, sha256_hex) "
                           "VALUES(?, ?, ?, ?) "
                           "ON CONFLICT(logical_name) DO UPDATE SET "
                           "byte_size=excluded.byte_size, "
                           "mtime_unix=excluded.mtime_unix, "
                           "sha256_hex=excluded.sha256_hex "
                           "RETURNING id;",
                           -1, &st, NULL) != SQLITE_OK) {
        throw std::runtime_error("Prepare failed: " + std::string(sqlite3_errmsg(db_)));
    }

    sqlite3_bind_text(st, 1, gz_path_.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(bytes));
    sqlite3_bind_int64(st, 3, static_cast<sqlite3_int64>(file_mtime));
    sqlite3_bind_text(st, 4, file_sha256.c_str(), -1, SQLITE_TRANSIENT);

    int rc = sqlite3_step(st);
    if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
        sqlite3_finalize(st);
        throw std::runtime_error("Insert failed: " + std::string(sqlite3_errmsg(db_)));
    }

    int db_file_id = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    // build the index
    auto stride = static_cast<size_t>(chunk_size_mb_ * 1024 * 1024);
    spdlog::debug("Building index with stride: {} bytes ({:.1f} MB)", stride, chunk_size_mb_);
    
    int ret = build_index_internal(db_, db_file_id, gz_path_, stride);
    if (ret != 0) {
        throw std::runtime_error("Index build failed for " + gz_path_ + " (error code: " + std::to_string(ret) + ")");
    }

    spdlog::info("Index built successfully for {}", gz_path_);
}

} // namespace indexer
} // namespace dft

// ==============================================================================
// C++ Public Interface Implementation
// ==============================================================================

namespace dft {
namespace indexer {

Indexer::Indexer(const std::string& gz_path, const std::string& idx_path, double chunk_size_mb, bool force_rebuild)
    : pImpl_(new Impl(gz_path, idx_path, chunk_size_mb, force_rebuild))
{
}

Indexer::~Indexer() = default;

Indexer::Indexer(Indexer&& other) noexcept : pImpl_(other.pImpl_.release()) {
}

Indexer& Indexer::operator=(Indexer&& other) noexcept {
    if (this != &other) {
        pImpl_.reset(other.pImpl_.release());
    }
    return *this;
}

void Indexer::build() {
    pImpl_->build();
}

bool Indexer::need_rebuild() const {
    return pImpl_->need_rebuild();
}

bool Indexer::is_valid() const {
    return pImpl_ && pImpl_->is_valid();
}

const std::string& Indexer::get_gz_path() const {
    return pImpl_->get_gz_path();
}

const std::string& Indexer::get_idx_path() const {
    return pImpl_->get_idx_path();
}

double Indexer::get_chunk_size_mb() const {
    return pImpl_->get_chunk_size_mb();
}

} // namespace indexer
} // namespace dft

// ==============================================================================
// C API Implementation (wraps C++ implementation)
// ==============================================================================

extern "C" {

dft_indexer_handle_t dft_indexer_create(const char* gz_path, const char* idx_path, double chunk_size_mb, int force_rebuild) {
    if (!gz_path || !idx_path || chunk_size_mb <= 0) {
        spdlog::error("Invalid parameters for indexer creation");
        return nullptr;
    }

    try {
        auto* indexer = new dft::indexer::Indexer(gz_path, idx_path, chunk_size_mb, force_rebuild != 0);
        return static_cast<dft_indexer_handle_t>(indexer);
    } catch (const std::exception& e) {
        spdlog::error("Failed to create DFT indexer: {}", e.what());
        return nullptr;
    }
}

int dft_indexer_build(dft_indexer_handle_t indexer) {
    if (!indexer) {
        return -1;
    }

    try {
        auto* cpp_indexer = static_cast<dft::indexer::Indexer*>(indexer);
        cpp_indexer->build();
        return 0;
    } catch (const std::exception& e) {
        spdlog::error("Failed to build index: {}", e.what());
        return -1;
    }
}

int dft_indexer_need_rebuild(dft_indexer_handle_t indexer) {
    if (!indexer) {
        return -1;
    }

    try {
        auto* cpp_indexer = static_cast<dft::indexer::Indexer*>(indexer);
        return cpp_indexer->need_rebuild() ? 1 : 0;
    } catch (const std::exception& e) {
        spdlog::error("Failed to check if rebuild is needed: {}", e.what());
        return -1;
    }
}

void dft_indexer_destroy(dft_indexer_handle_t indexer) {
    if (indexer) {
        delete static_cast<dft::indexer::Indexer*>(indexer);
    }
}

} // extern "C"
