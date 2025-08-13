#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <spdlog/spdlog.h>
#include <sqlite3.h>
#include <zlib.h>

#include <dft_utils/reader/reader.h>
#include <dft_utils/utils/platform_compat.h>

// Forward declarations  
struct InflateState
{
    z_stream zs;
    FILE *file;
    unsigned char in[16384]; // 16 KB
    int bits;
    size_t c_off;
};

struct CheckpointInfo
{
    size_t uc_offset;
    size_t c_offset;
    int bits;
    std::vector<unsigned char> dict_compressed;
    size_t dict_compressed_size;
};

namespace dft
{
namespace reader
{

class DatabaseHelper
{
public:
    explicit DatabaseHelper(sqlite3* db) : db_(db) {}
    
    int find_file_id(const std::string& gz_path) {
        const char* sql = "SELECT id FROM files WHERE logical_name = ? LIMIT 1";
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt = prepare_statement(sql);
        sqlite3_bind_text(stmt.get(), 1, gz_path.c_str(), -1, SQLITE_STATIC);
        
        if (sqlite3_step(stmt.get()) == SQLITE_ROW) {
            return sqlite3_column_int(stmt.get(), 0);
        }
        return -1;
    }
    
    bool find_checkpoint(int file_id, size_t target_offset, CheckpointInfo* result) {
        const char* sql = "SELECT uc_offset, c_offset, bits, dict_compressed "
                         "FROM checkpoints WHERE file_id = ? AND uc_offset <= ? "
                         "ORDER BY uc_offset DESC LIMIT 1";
        
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt = prepare_statement(sql);
        sqlite3_bind_int(stmt.get(), 1, file_id);
        sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(target_offset));
        
        if (sqlite3_step(stmt.get()) == SQLITE_ROW) {
            result->uc_offset = static_cast<size_t>(sqlite3_column_int64(stmt.get(), 0));
            result->c_offset = static_cast<size_t>(sqlite3_column_int64(stmt.get(), 1));
            result->bits = sqlite3_column_int(stmt.get(), 2);
            
            size_t dict_size = static_cast<size_t>(sqlite3_column_bytes(stmt.get(), 3));
            result->dict_compressed.resize(dict_size);
            memcpy(result->dict_compressed.data(), sqlite3_column_blob(stmt.get(), 3), dict_size);
            
            return true;
        }
        return false;
    }
    
    size_t get_max_bytes() {
        const char* sql = "SELECT MAX(uc_offset + uc_size) FROM chunks";
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt = prepare_statement(sql);
        
        if (sqlite3_step(stmt.get()) == SQLITE_ROW) {
            sqlite3_int64 max_val = sqlite3_column_int64(stmt.get(), 0);
            return (max_val >= 0) ? static_cast<size_t>(max_val) : 0;
        }
        return 0;
    }
    
private:
    sqlite3* db_;
    
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> prepare_statement(const char* sql) {
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, NULL) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(db_)));
        }
        return std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>(stmt, sqlite3_finalize);
    }
};

class ReaderError : public std::runtime_error
{
public:
    enum Type {
        DATABASE_ERROR,
        FILE_IO_ERROR,
        COMPRESSION_ERROR,
        INVALID_ARGUMENT,
        INITIALIZATION_ERROR
    };
    
    ReaderError(Type type, const std::string& message) 
        : std::runtime_error(format_message(type, message)), type_(type) {}
    
    Type get_type() const { return type_; }
    
private:
    Type type_;
    
    static std::string format_message(Type type, const std::string& message) {
        const char* prefix = "";
        switch (type) {
            case DATABASE_ERROR: prefix = "Database error"; break;
            case FILE_IO_ERROR: prefix = "File I/O error"; break;
            case COMPRESSION_ERROR: prefix = "Compression error"; break;
            case INVALID_ARGUMENT: prefix = "Invalid argument"; break;
            case INITIALIZATION_ERROR: prefix = "Initialization error"; break;
        }
        return std::string(prefix) + ": " + message;
    }
};

class CompressionManager
{
public:
    class InflationSession {
    public:
        explicit InflationSession(InflateState* state, bool owns = false) 
            : state_(state), owns_state_(owns) {}
        
        ~InflationSession() {
            if (owns_state_ && state_) {
                inflateEnd(&state_->zs);
            }
        }
        
        int read(unsigned char* output, size_t output_size, size_t* bytes_read) {
            return inflate_read(state_, output, output_size, bytes_read);
        }
        
        void skip(size_t bytes_to_skip, unsigned char* skip_buffer, size_t skip_buffer_size) {
            if (bytes_to_skip == 0) return;
            
            size_t remaining_skip = bytes_to_skip;
            while (remaining_skip > 0) {
                size_t to_skip = (remaining_skip > skip_buffer_size) ? skip_buffer_size : remaining_skip;
                size_t skipped;
                int result = read(skip_buffer, to_skip, &skipped);
                if (result != 0) {
                    throw std::runtime_error("Failed during skip phase");
                }
                if (skipped == 0) {
                    break;
                }
                remaining_skip -= skipped;
            }
        }
        
    private:
        InflateState* state_;
        bool owns_state_;
        
        InflationSession(const InflationSession&); // deleted
        InflationSession& operator=(const InflationSession&); // deleted
    };
    
    static int inflate_init(InflateState *state, FILE *f, size_t c_off, int bits) {
        memset(state, 0, sizeof(*state));
        state->file = f;
        state->c_off = c_off;
        state->bits = bits;

        if (inflateInit2(&state->zs, 15 + 16) != Z_OK) {
            return -1;
        }

        if (fseeko(f, static_cast<off_t>(c_off), SEEK_SET) != 0) {
            spdlog::error("Failed to seek to compressed offset: {}", c_off);
            inflateEnd(&state->zs);
            return -1;
        }

        return 0;
    }
    
    static void inflate_cleanup(InflateState *state) {
        inflateEnd(&state->zs);
    }
    
    static int inflate_read(InflateState *state, unsigned char *out, size_t out_size, size_t *bytes_read) {
        state->zs.next_out = out;
        state->zs.avail_out = static_cast<uInt>(out_size);
        *bytes_read = 0;

        while (state->zs.avail_out > 0) {
            if (state->zs.avail_in == 0) {
                size_t n = fread(state->in, 1, sizeof(state->in), state->file);
                if (n == 0) {
                    if (ferror(state->file)) {
                        spdlog::error("Error reading from file during inflate_read");
                        return -1;
                    }
                    break; // EOF
                }
                state->zs.next_in = state->in;
                state->zs.avail_in = static_cast<uInt>(n);
            }

            int ret = inflate(&state->zs, Z_NO_FLUSH);
            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                spdlog::debug("inflate() failed with error: {} ({})", 
                             ret, state->zs.msg ? state->zs.msg : "no message");
                return -1;
            }
        }

        *bytes_read = out_size - state->zs.avail_out;
        return 0;
    }
};

static void validate_read_parameters(const char* buffer, size_t buffer_size, 
                                   size_t start_bytes, size_t end_bytes) {
    if (!buffer || buffer_size == 0) {
        throw std::invalid_argument("Invalid buffer parameters");
    }
    if (start_bytes >= end_bytes) {
        throw std::invalid_argument("start_bytes must be less than end_bytes");
    }
}

class BaseStreamingSession
{
protected:
    std::string current_gz_path_;
    size_t start_bytes_;
    size_t current_position_;
    size_t target_end_bytes_;
    bool is_active_;
    bool is_finished_;
    
    std::unique_ptr<InflateState> inflate_state_;
    std::unique_ptr<CheckpointInfo> checkpoint_;
    FILE* file_handle_;
    bool decompression_initialized_;
    
    static const size_t SKIP_BUFFER_SIZE = 65536;
    unsigned char skip_buffer_[SKIP_BUFFER_SIZE];
    
public:
    BaseStreamingSession()
        : start_bytes_(0), current_position_(0), target_end_bytes_(0), 
          is_active_(false), is_finished_(false), file_handle_(nullptr),
          decompression_initialized_(false) {}
    
    virtual ~BaseStreamingSession() {
        reset();
    }
    
    bool matches(const std::string& gz_path, size_t start_bytes, size_t end_bytes) const {
        return current_gz_path_ == gz_path && 
               start_bytes_ == start_bytes && 
               target_end_bytes_ == end_bytes;
    }
    
    bool is_finished() const { return is_finished_; }
    
    virtual void initialize(const std::string& gz_path, size_t start_bytes, size_t end_bytes,
                          DatabaseHelper& db_helper) = 0;
    virtual size_t stream_chunk(char* buffer, size_t buffer_size) = 0;
    
    virtual void reset() {
        current_gz_path_.clear();
        start_bytes_ = 0;
        current_position_ = 0;
        target_end_bytes_ = 0;
        is_active_ = false;
        is_finished_ = false;
        if (file_handle_) {
            fclose(file_handle_);
            file_handle_ = nullptr;
        }
        if (decompression_initialized_ && inflate_state_) {
            CompressionManager::inflate_cleanup(inflate_state_.get());
        }
        inflate_state_.reset();
        checkpoint_.reset();
        decompression_initialized_ = false;
    }
    
protected:
    FILE* open_file(const std::string& path) {
        FILE* file = fopen(path.c_str(), "rb");
        if (!file) {
            throw ReaderError(ReaderError::FILE_IO_ERROR, "Failed to open file: " + path);
        }
        return file;
    }
    
    void initialize_compression(const std::string& gz_path, size_t start_bytes, 
                              DatabaseHelper& db_helper) {
        file_handle_ = open_file(gz_path);
        inflate_state_.reset(new InflateState());
        bool use_checkpoint = false;
        
        // Try to find checkpoint
        int file_id = db_helper.find_file_id(gz_path);
        if (file_id != -1) {
            checkpoint_.reset(new CheckpointInfo());
            if (db_helper.find_checkpoint(file_id, start_bytes, checkpoint_.get())) {
                if (inflate_init_from_checkpoint(inflate_state_.get(), file_handle_, checkpoint_.get()) == 0) {
                    use_checkpoint = true;
                    spdlog::debug("Using checkpoint at uncompressed offset {} for target {}", 
                                 checkpoint_->uc_offset, start_bytes);
                }
            }
        }
        
        // Fallback to sequential read
        if (!use_checkpoint) {
            checkpoint_.reset();
            if (CompressionManager::inflate_init(inflate_state_.get(), file_handle_, 0, 0) != 0) {
                throw ReaderError(ReaderError::COMPRESSION_ERROR, "Failed to initialize inflation");
            }
        }
        
        decompression_initialized_ = true;
    }
    
    void skip_to_position(size_t target_position) {
        size_t current_pos = checkpoint_ ? checkpoint_->uc_offset : 0;
        if (target_position > current_pos) {
            CompressionManager::InflationSession session(inflate_state_.get(), false);
            session.skip(target_position - current_pos, skip_buffer_, SKIP_BUFFER_SIZE);
        }
    }
    
    int inflate_init_from_checkpoint(InflateState* state, FILE* f, const CheckpointInfo* checkpoint) const {
        memset(state, 0, sizeof(*state));
        state->file = f;
        state->c_off = checkpoint->c_offset;
        state->bits = checkpoint->bits;

        spdlog::debug("Checkpoint c_offset: {}, bits: {}", checkpoint->c_offset, checkpoint->bits);

        // Position file exactly like zran: seek to point->in - (point->bits ? 1 : 0)
        off_t seek_pos = static_cast<off_t>(checkpoint->c_offset) - (checkpoint->bits ? 1 : 0);
        if (fseeko(f, seek_pos, SEEK_SET) != 0) {
            spdlog::error("Failed to seek to checkpoint position: {}", seek_pos);
            return -1;
        }

        // If we have partial bits, read the extra byte (following zran approach)
        int ch = 0;
        if (checkpoint->bits != 0) {
            ch = fgetc(f);
            if (ch == EOF) {
                spdlog::error("Failed to read byte at checkpoint position");
                return -1;
            }
        }

        // Initialize raw deflate stream (not gzip wrapper) following zran approach
        if (inflateInit2(&state->zs, -15) != Z_OK) {
            return -1;
        }

        // Reset to raw mode (following zran: inflateReset2(&index->strm, RAW))
        state->zs.avail_in = 0;
        if (inflateReset2(&state->zs, -15) != Z_OK) { // RAW mode
            inflateEnd(&state->zs);
            return -1;
        }

        // Prime with partial bits if needed (following zran: INFLATEPRIME)
        if (checkpoint->bits != 0) {
            int prime_value = ch >> (8 - checkpoint->bits);
            spdlog::debug("Applying inflatePrime with {} bits, value: {}", checkpoint->bits, prime_value);
            if (inflatePrime(&state->zs, checkpoint->bits, prime_value) != Z_OK) {
                spdlog::error("inflatePrime failed with {} bits, value: {}", checkpoint->bits, prime_value);
                inflateEnd(&state->zs);
                return -1;
            }
        }

        // Decompress the saved dictionary
        unsigned char window[32768];
        size_t window_size = 32768;
        if (decompress_window(checkpoint->dict_compressed.data(), checkpoint->dict_compressed.size(), 
                             window, &window_size) != 0) {
            inflateEnd(&state->zs);
            return -1;
        }

        // Set dictionary (following zran: inflateSetDictionary)
        if (inflateSetDictionary(&state->zs, window, static_cast<uInt>(window_size)) != Z_OK) {
            spdlog::error("inflateSetDictionary failed");
            inflateEnd(&state->zs);
            return -1;
        }

        // Prime the input buffer for subsequent reads
        size_t n = fread(state->in, 1, sizeof(state->in), state->file);
        if (n > 0) {
            state->zs.next_in = state->in;
            state->zs.avail_in = static_cast<uInt>(n);
        } else if (ferror(state->file)) {
            spdlog::error("Error reading from file during checkpoint initialization");
            return -1;
        }

        return 0;
    }
    
    int decompress_window(const unsigned char *compressed, size_t compressed_size, 
                         unsigned char *window, size_t *window_size) const {
        z_stream zs;
        memset(&zs, 0, sizeof(zs));

        if (inflateInit(&zs) != Z_OK) {
            spdlog::error("Failed to initialize inflate for window decompression");
            return -1;
        }

        zs.next_in = const_cast<unsigned char *>(compressed);
        zs.avail_in = static_cast<uInt>(compressed_size);
        zs.next_out = window;
        zs.avail_out = static_cast<uInt>(*window_size);

        int ret = inflate(&zs, Z_FINISH);
        if (ret != Z_STREAM_END) {
            spdlog::error("inflate failed during window decompression with error: {} ({})", 
                         ret, zs.msg ? zs.msg : "no message");
            inflateEnd(&zs);
            return -1;
        }

        *window_size = *window_size - zs.avail_out;
        inflateEnd(&zs);
        return 0;
    }
};

class JsonStreamingSession : public BaseStreamingSession
{
private:
    static const size_t INCOMPLETE_BUFFER_SIZE = 2 * 1024 * 1024;
    char incomplete_buffer_[INCOMPLETE_BUFFER_SIZE];
    size_t incomplete_buffer_size_;
    size_t actual_start_bytes_;
    
public:
    JsonStreamingSession() : BaseStreamingSession(), incomplete_buffer_size_(0), actual_start_bytes_(0) {
        memset(incomplete_buffer_, 0, sizeof(incomplete_buffer_));
    }
    
    void initialize(const std::string& gz_path, size_t start_bytes, size_t end_bytes,
                   DatabaseHelper& db_helper) override {
        spdlog::debug("Initializing JSON streaming session for range [{}, {}] from {}", 
                     start_bytes, end_bytes, gz_path);
        
        if (is_active_) {
            reset();
        }
        
        current_gz_path_ = gz_path;
        start_bytes_ = start_bytes;
        target_end_bytes_ = end_bytes;
        is_active_ = true;
        is_finished_ = false;
        incomplete_buffer_size_ = 0;
        
        initialize_compression(gz_path, start_bytes, db_helper);
        
        // Find the actual start position (beginning of a complete JSON line)
        actual_start_bytes_ = find_json_line_start(start_bytes);
        current_position_ = actual_start_bytes_;
        
        spdlog::debug("JSON streaming session initialized: actual_start={}, target_end={}", 
                     actual_start_bytes_, end_bytes);
    }
    
    size_t stream_chunk(char* buffer, size_t buffer_size) override {
        if (!decompression_initialized_) {
            throw ReaderError(ReaderError::INITIALIZATION_ERROR, 
                            "Streaming session not properly initialized");
        }
        
        // Fill incomplete buffer if empty
        if (incomplete_buffer_size_ == 0) {
            fill_incomplete_buffer();
        }
        
        // Serve data from incomplete buffer
        if (incomplete_buffer_size_ > 0) {
            return serve_data_from_buffer(buffer, buffer_size);
        }
        
        // Return 0 to indicate end of stream
        return 0;
    }
    
    void reset() override {
        BaseStreamingSession::reset();
        memset(incomplete_buffer_, 0, sizeof(incomplete_buffer_));
        incomplete_buffer_size_ = 0;
        actual_start_bytes_ = 0;
    }
    
private:
    size_t find_json_line_start(size_t target_start) {
        size_t current_pos = checkpoint_ ? checkpoint_->uc_offset : 0;
        size_t actual_start = target_start;
        
        if (target_start <= current_pos) {
            return target_start;
        }
        
        // Search for JSON line start
        size_t search_start = (target_start >= 512) ? target_start - 512 : current_pos;
        
        if (search_start > current_pos) {
            skip_to_position(search_start);
            current_pos = search_start;
        }
        
        // Read data to find start of complete JSON line
        unsigned char search_buffer[2048];
        size_t search_bytes;
        if (CompressionManager::inflate_read(inflate_state_.get(), search_buffer, 
                                           sizeof(search_buffer) - 1, &search_bytes) == 0) {
            size_t relative_target = target_start - current_pos;
            if (relative_target < search_bytes) {
                for (int64_t i = static_cast<int64_t>(relative_target); i >= 0; i--) {
                    if (i == 0 || search_buffer[i - 1] == '\n') {
                        actual_start = current_pos + static_cast<size_t>(i);
                        spdlog::debug("Found JSON line start at position {} (requested {})", 
                                     actual_start, target_start);
                        break;
                    }
                }
            }
        }
        
        // Restart compression and skip to actual start
        restart_compression();
        if (actual_start > (checkpoint_ ? checkpoint_->uc_offset : 0)) {
            skip_to_position(actual_start);
        }
        
        return actual_start;
    }
    
    void fill_incomplete_buffer() {
        char temp_buffer[INCOMPLETE_BUFFER_SIZE];
        size_t total_read = 0;
        
        while (total_read < INCOMPLETE_BUFFER_SIZE) {
            if (current_position_ >= target_end_bytes_) {
                if (total_read > 0 && find_json_boundary_in_buffer(temp_buffer, total_read)) {
                    break;
                }
                is_finished_ = true;
                break;
            }
            
            size_t bytes_read;
            int result = CompressionManager::inflate_read(inflate_state_.get(),
                                                        reinterpret_cast<unsigned char*>(temp_buffer + total_read),
                                                        INCOMPLETE_BUFFER_SIZE - total_read,
                                                        &bytes_read);
            
            if (result != 0 || bytes_read == 0) {
                is_finished_ = true;
                break;
            }
            
            total_read += bytes_read;
            current_position_ += bytes_read;
        }
        
        if (total_read > 0) {
            memcpy(incomplete_buffer_, temp_buffer, total_read);
            incomplete_buffer_size_ = total_read;
        }
    }
    
    size_t serve_data_from_buffer(char* buffer, size_t buffer_size) {
        size_t bytes_to_copy = std::min(buffer_size, incomplete_buffer_size_);
        bytes_to_copy = adjust_to_json_boundary(bytes_to_copy);
        
        memcpy(buffer, incomplete_buffer_, bytes_to_copy);
        
        // Shift remaining data
        if (bytes_to_copy < incomplete_buffer_size_) {
            memmove(incomplete_buffer_, incomplete_buffer_ + bytes_to_copy, 
                   incomplete_buffer_size_ - bytes_to_copy);
        }
        incomplete_buffer_size_ -= bytes_to_copy;
        
        return bytes_to_copy;
    }
    
    size_t adjust_to_json_boundary(size_t buffer_size) {
        // Don't adjust if we don't have any complete JSON objects
        // This ensures we never return 0 when there's data unless truly finished
        for (int64_t i = static_cast<int64_t>(buffer_size) - 1; i > 0; i--) {
            if (incomplete_buffer_[i - 1] == '}' && incomplete_buffer_[i] == '\n') {
                return static_cast<size_t>(i) + 1;
            }
        }
        // If no JSON boundary found but we have data and aren't finished,
        // return all the data (don't cut off incomplete JSON)
        if (!is_finished_ && buffer_size > 0) {
            return buffer_size;
        }
        // Only return partial data if we're at the end of stream
        return buffer_size;
    }
    
    bool find_json_boundary_in_buffer(const char* buffer, size_t buffer_size) {
        for (size_t i = 1; i < buffer_size; i++) {
            if (buffer[i - 1] == '}' && buffer[i] == '\n') {
                return true;
            }
        }
        return false;
    }
    
    void restart_compression() {
        CompressionManager::inflate_cleanup(inflate_state_.get());
        
        bool use_checkpoint = (checkpoint_ != nullptr);
        if (use_checkpoint) {
            if (inflate_init_from_checkpoint(inflate_state_.get(), file_handle_, checkpoint_.get()) != 0) {
                throw ReaderError(ReaderError::COMPRESSION_ERROR, 
                                "Failed to reinitialize from checkpoint");
            }
        } else {
            if (CompressionManager::inflate_init(inflate_state_.get(), file_handle_, 0, 0) != 0) {
                throw ReaderError(ReaderError::COMPRESSION_ERROR, "Failed to reinitialize inflation");
            }
        }
    }
};

class RawStreamingSession : public BaseStreamingSession
{
public:
    RawStreamingSession() : BaseStreamingSession() {}
    
    void initialize(const std::string& gz_path, size_t start_bytes, size_t end_bytes,
                   DatabaseHelper& db_helper) override {
        spdlog::debug("Initializing raw streaming session for range [{}, {}] from {}", 
                     start_bytes, end_bytes, gz_path);
        
        if (is_active_) {
            reset();
        }
        
        current_gz_path_ = gz_path;
        start_bytes_ = start_bytes;
        target_end_bytes_ = end_bytes;
        current_position_ = start_bytes;
        is_active_ = true;
        is_finished_ = false;
        
        initialize_compression(gz_path, start_bytes, db_helper);
        
        // Skip to the start position
        size_t current_pos = checkpoint_ ? checkpoint_->uc_offset : 0;
        if (start_bytes > current_pos) {
            skip_to_position(start_bytes);
        }
        
        spdlog::debug("Raw streaming session initialized: start={}, target_end={}", 
                     start_bytes, end_bytes);
    }
    
    size_t stream_chunk(char* buffer, size_t buffer_size) override {
        if (!decompression_initialized_) {
            throw ReaderError(ReaderError::INITIALIZATION_ERROR,
                            "Raw streaming session not properly initialized");
        }
        
        // Check if we've reached the target end
        if (current_position_ >= target_end_bytes_) {
            is_finished_ = true;
            return 0;
        }
        
        // Calculate how much we can read without exceeding the target
        size_t max_read = target_end_bytes_ - current_position_;
        size_t read_size = std::min(buffer_size, max_read);
        
        size_t bytes_read;
        int result = CompressionManager::inflate_read(inflate_state_.get(),
                                                    reinterpret_cast<unsigned char*>(buffer),
                                                    read_size,
                                                    &bytes_read);
        
        if (result != 0 || bytes_read == 0) {
            is_finished_ = true;
            return 0;
        }
        
        current_position_ += bytes_read;
        
        spdlog::debug("Raw streamed {} bytes (position: {} / {})",
                     bytes_read, current_position_, target_end_bytes_);
        
        return bytes_read;
    }
};

class StreamingSessionFactory
{
private:
    DatabaseHelper& db_helper_;
    
public:
    explicit StreamingSessionFactory(DatabaseHelper& db_helper) 
        : db_helper_(db_helper) {}
    
    std::unique_ptr<JsonStreamingSession> create_json_session(
        const std::string& gz_path, size_t start_bytes, size_t end_bytes) {
        
        std::unique_ptr<JsonStreamingSession> session(new JsonStreamingSession());
        session->initialize(gz_path, start_bytes, end_bytes, db_helper_);
        return session;
    }
    
    std::unique_ptr<RawStreamingSession> create_raw_session(
        const std::string& gz_path, size_t start_bytes, size_t end_bytes) {
        
        std::unique_ptr<RawStreamingSession> session(new RawStreamingSession());
        session->initialize(gz_path, start_bytes, end_bytes, db_helper_);
        return session;
    }
    
    bool needs_new_json_session(const JsonStreamingSession* current,
                               const std::string& gz_path, size_t start_bytes, size_t end_bytes) const {
        return !current || 
               !current->matches(gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }
    
    bool needs_new_raw_session(const RawStreamingSession* current,
                              const std::string& gz_path, size_t start_bytes, size_t end_bytes) const {
        return !current || 
               !current->matches(gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }
};

class Reader::Impl
{
  public:
    Impl(const std::string &gz_path, const std::string &idx_path)
        : gz_path_(gz_path), idx_path_(idx_path), db_(nullptr), is_open_(false)
    {
        if (sqlite3_open(idx_path.c_str(), &db_) != SQLITE_OK)
        {
            throw ReaderError(ReaderError::DATABASE_ERROR, "Failed to open index database: " + std::string(sqlite3_errmsg(db_)));
        }
        is_open_ = true;
        
        // Initialize new architecture components
        db_helper_.reset(new DatabaseHelper(db_));
        session_factory_.reset(new StreamingSessionFactory(*db_helper_));
        
        spdlog::debug("Successfully created DFT reader for gz: {} and index: {}", gz_path, idx_path);
    }

    ~Impl()
    {
        cleanup_streaming_state();
        if (is_open_ && db_)
        {
            sqlite3_close(db_);
        }
        spdlog::debug("Successfully destroyed DFT reader");
    }

    Impl(const Impl &) = delete;
    Impl &operator=(const Impl &) = delete;

    Impl(Impl &&other) noexcept
        : gz_path_(std::move(other.gz_path_)), idx_path_(std::move(other.idx_path_)), db_(other.db_),
          is_open_(other.is_open_)
    {
        other.db_ = nullptr;
        other.is_open_ = false;
    }

    Impl &operator=(Impl &&other) noexcept
    {
        if (this != &other)
        {
            if (is_open_ && db_)
            {
                sqlite3_close(db_);
            }
            gz_path_ = std::move(other.gz_path_);
            idx_path_ = std::move(other.idx_path_);
            db_ = other.db_;
            is_open_ = other.is_open_;
            other.db_ = nullptr;
            other.is_open_ = false;
        }
        return *this;
    }

    size_t get_max_bytes() const
    {
        if (!is_open_ || !db_)
        {
            throw std::runtime_error("Reader is not open");
        }

        DatabaseHelper db_helper(db_);
        size_t max_bytes = db_helper.get_max_bytes();
        
        if (max_bytes > 0) {
            spdlog::debug("Maximum bytes available: {}", max_bytes);
        } else {
            spdlog::debug("No chunks found, maximum bytes: 0");
        }
        
        return max_bytes;
    }

    size_t read(const std::string &gz_path,
                size_t start_bytes,
                size_t end_bytes,
                char *buffer,
                size_t buffer_size)
    {
        if (!is_open_ || !db_)
        {
            throw ReaderError(ReaderError::INITIALIZATION_ERROR, "Reader is not open");
        }

        validate_read_parameters(buffer, buffer_size, start_bytes, end_bytes);
        
        // Create or reuse JSON streaming session
        if (session_factory_->needs_new_json_session(json_session_.get(), gz_path, start_bytes, end_bytes)) {
            json_session_ = session_factory_->create_json_session(gz_path, start_bytes, end_bytes);
        }
        
        if (json_session_->is_finished()) {
            return 0;
        }
        
        return json_session_->stream_chunk(buffer, buffer_size);
    }

    size_t read_raw(const std::string &gz_path,
                    size_t start_bytes,
                    size_t end_bytes,
                    char *buffer,
                    size_t buffer_size)
    {
        if (!is_open_ || !db_)
        {
            throw ReaderError(ReaderError::INITIALIZATION_ERROR, "Reader is not open");
        }

        validate_read_parameters(buffer, buffer_size, start_bytes, end_bytes);
        
        // Create or reuse raw streaming session
        if (session_factory_->needs_new_raw_session(raw_session_.get(), gz_path, start_bytes, end_bytes)) {
            raw_session_ = session_factory_->create_raw_session(gz_path, start_bytes, end_bytes);
        }
        
        if (raw_session_->is_finished()) {
            return 0;
        }
        
        return raw_session_->stream_chunk(buffer, buffer_size);
    }

    void reset()
    {
        if (!is_open_ || !db_)
        {
            throw std::runtime_error("Reader is not open");
        }

        // Reset new session architecture
        if (json_session_) {
            json_session_->reset();
        }
        if (raw_session_) {
            raw_session_->reset();
        }
        
        // Reset legacy sessions
        streaming_state_ = {};
        raw_streaming_state_ = {};
    }

  public:
    bool is_valid() const
    {
        return is_open_ && db_ != nullptr;
    }
    const std::string &get_gz_path() const
    {
        return gz_path_;
    }
    const std::string &get_idx_path() const
    {
        return idx_path_;
    }

  private:
    static constexpr size_t INFLATE_CHUNK_SIZE = 16384; // 16 KB
    static constexpr size_t SKIP_BUFFER_SIZE = 65536;   // 64 KB
#define INCOMPLETE_BUFFER_SIZE 2 * 1024 * 1024          // 2 MB, big enough to hold multiple JSON lines
    unsigned char skip_buffer[SKIP_BUFFER_SIZE];


    int inflate_init(InflateState *state, FILE *f, size_t c_off, int bits) const
    {
        return CompressionManager::inflate_init(state, f, c_off, bits);
    }

    void inflate_cleanup(InflateState *state) const
    {
        CompressionManager::inflate_cleanup(state);
    }

    int inflate_read(InflateState *state, unsigned char *out, size_t out_size, size_t *bytes_read) const
    {
        return CompressionManager::inflate_read(state, out, out_size, bytes_read);
    }

    int find_checkpoint(size_t target_uc_offset, CheckpointInfo *checkpoint) const
    {
        if (!is_open_ || !db_)
        {
            spdlog::debug("Reader not open for checkpoint lookup");
            return -1;
        }

        DatabaseHelper db_helper(db_);
        int file_id = db_helper.find_file_id(gz_path_);
        
        if (file_id == -1)
        {
            spdlog::debug("File not found in database: {}", gz_path_);
            return -1;
        }

        if (db_helper.find_checkpoint(file_id, target_uc_offset, checkpoint))
        {
            if (checkpoint->dict_compressed.size() > 0)
            {
                spdlog::debug("Found checkpoint at uc_offset={} for target={}", 
                             checkpoint->uc_offset, target_uc_offset);
                return 0;
            }
            else
            {
                spdlog::error("Failed to allocate memory for checkpoint dictionary of size {}",
                              checkpoint->dict_compressed.size());
            }
        }
        else
        {
            spdlog::debug("No checkpoint found for target uc_offset={}", target_uc_offset);
        }

        return -1;
    }

    int decompress_window(const unsigned char *compressed,
                          size_t compressed_size,
                          unsigned char *window,
                          size_t *window_size) const
    {
        z_stream zs;
        memset(&zs, 0, sizeof(zs));

        if (inflateInit(&zs) != Z_OK)
        {
            spdlog::error("Failed to initialize inflate for window decompression");
            return -1;
        }

        zs.next_in = const_cast<unsigned char *>(compressed);
        zs.avail_in = static_cast<uInt>(compressed_size);
        zs.next_out = window;
        zs.avail_out = static_cast<uInt>(*window_size);

        int ret = inflate(&zs, Z_FINISH);
        if (ret != Z_STREAM_END)
        {
            spdlog::error(
                "inflate failed during window decompression with error: {} ({})", ret, zs.msg ? zs.msg : "no message");
            inflateEnd(&zs);
            return -1;
        }

        *window_size = *window_size - zs.avail_out;
        inflateEnd(&zs);
        return 0;
    }

    int inflate_init_from_checkpoint(InflateState *state, FILE *f, const CheckpointInfo *checkpoint) const
    {
        memset(state, 0, sizeof(*state));
        state->file = f;
        state->c_off = checkpoint->c_offset;
        state->bits = checkpoint->bits;

        spdlog::debug("Checkpoint c_offset: {}, bits: {}", checkpoint->c_offset, checkpoint->bits);

        // Position file exactly like zran: seek to point->in - (point->bits ? 1 : 0)
        off_t seek_pos = static_cast<off_t>(checkpoint->c_offset) - (checkpoint->bits ? 1 : 0);
        if (fseeko(f, seek_pos, SEEK_SET) != 0)
        {
            spdlog::error("Failed to seek to checkpoint position: {}", seek_pos);
            return -1;
        }

        // If we have partial bits, read the extra byte (following zran approach)
        int ch = 0;
        if (checkpoint->bits != 0)
        {
            ch = fgetc(f);
            if (ch == EOF)
            {
                spdlog::error("Failed to read byte at checkpoint position");
                return -1;
            }
        }

        // Initialize raw deflate stream (not gzip wrapper) following zran approach
        if (inflateInit2(&state->zs, -15) != Z_OK)
        {
            return -1;
        }

        // Reset to raw mode (following zran: inflateReset2(&index->strm, RAW))
        state->zs.avail_in = 0;
        if (inflateReset2(&state->zs, -15) != Z_OK) // RAW mode
        {
            inflateEnd(&state->zs);
            return -1;
        }

        // Prime with partial bits if needed (following zran: INFLATEPRIME)
        if (checkpoint->bits != 0)
        {
            int prime_value = ch >> (8 - checkpoint->bits);
            spdlog::debug("Applying inflatePrime with {} bits, value: {}", checkpoint->bits, prime_value);
            if (inflatePrime(&state->zs, checkpoint->bits, prime_value) != Z_OK)
            {
                spdlog::error("inflatePrime failed with {} bits, value: {}", checkpoint->bits, prime_value);
                inflateEnd(&state->zs);
                return -1;
            }
        }

        // Decompress the saved dictionary
        unsigned char window[32768];
        size_t window_size = 32768;
        if (decompress_window(
                checkpoint->dict_compressed.data(), checkpoint->dict_compressed.size(), window, &window_size) != 0)
        {
            inflateEnd(&state->zs);
            return -1;
        }

        // Set dictionary (following zran: inflateSetDictionary)
        if (inflateSetDictionary(&state->zs, window, static_cast<uInt>(window_size)) != Z_OK)
        {
            spdlog::error("inflateSetDictionary failed");
            inflateEnd(&state->zs);
            return -1;
        }

        // Prime the input buffer for subsequent reads
        size_t n = fread(state->in, 1, sizeof(state->in), state->file);
        if (n > 0)
        {
            state->zs.next_in = state->in;
            state->zs.avail_in = static_cast<uInt>(n);
        }
        else if (ferror(state->file))
        {
            spdlog::error("Error reading from file during checkpoint initialization");
            return -1;
        }

        return 0;
    }

    void free_checkpoint(CheckpointInfo *checkpoint) const
    {
        if (checkpoint)
        {
            checkpoint->dict_compressed = std::vector<unsigned char>();
        }
    }

    void initialize_streaming_session(const std::string &gz_path, size_t start_bytes, size_t end_bytes)
    {
        spdlog::debug("Initializing streaming session for range [{}, {}] from {}", start_bytes, end_bytes, gz_path);

        // Clean up any existing state - SAFELY
        if (streaming_state_.is_active)
        {
            streaming_state_.reset(); // This is safe - only closes file handle and resets pointers
        }

        // Setup new session
        streaming_state_.current_gz_path = gz_path;
        streaming_state_.start_bytes = start_bytes;
        streaming_state_.target_end_bytes = end_bytes;
        streaming_state_.is_active = true;
        streaming_state_.is_finished = false;

        // Open file
        streaming_state_.file_handle = fopen(gz_path.c_str(), "rb");
        if (!streaming_state_.file_handle)
        {
            spdlog::error("Failed to open file: {}", gz_path);
            throw std::runtime_error("Failed to open file: " + gz_path);
        }

        // Initialize decompression state
        streaming_state_.inflate_state.reset(new InflateState());
        bool use_checkpoint = false;

        // Try to find a checkpoint near the start position
        streaming_state_.checkpoint.reset(new CheckpointInfo());
        if (find_checkpoint(start_bytes, streaming_state_.checkpoint.get()) == 0)
        {
            if (inflate_init_from_checkpoint(streaming_state_.inflate_state.get(),
                                             streaming_state_.file_handle,
                                             streaming_state_.checkpoint.get()) == 0)
            {
                use_checkpoint = true;
                spdlog::debug("Using checkpoint at uncompressed offset {} for target {}",
                              streaming_state_.checkpoint->uc_offset,
                              start_bytes);
            }
            else
            {
                spdlog::debug("Failed to initialize from checkpoint, falling back to sequential read");
                free_checkpoint(streaming_state_.checkpoint.get());
                streaming_state_.checkpoint.reset();
            }
        }
        else
        {
            streaming_state_.checkpoint.reset();
        }

        // Fallback to sequential read if no checkpoint or checkpoint failed
        if (!use_checkpoint)
        {
            if (inflate_init(streaming_state_.inflate_state.get(), streaming_state_.file_handle, 0, 0) != 0)
            {
                throw std::runtime_error("Failed to initialize inflation");
            }
        }

        // Find the actual start position (beginning of a complete JSON line)
        streaming_state_.actual_start_bytes = find_json_line_start(start_bytes, use_checkpoint);
        streaming_state_.current_position = streaming_state_.actual_start_bytes;
        streaming_state_.decompression_initialized = true;

        spdlog::debug("Streaming session initialized: actual_start={}, target_end={}",
                      streaming_state_.actual_start_bytes,
                      end_bytes);
    }

    size_t find_json_line_start(size_t target_start, bool use_checkpoint)
    {
        size_t current_pos = use_checkpoint ? streaming_state_.checkpoint->uc_offset : 0;
        size_t actual_start = target_start;

        if (target_start <= current_pos)
        {
            return target_start; // Already at or past the target
        }

        // Seek a bit before the requested start to find the beginning of the JSON line
        size_t search_start = (target_start >= 512) ? target_start - 512 : current_pos;

        // Skip to search start position
        if (search_start > current_pos)
        {
            skip_bytes(search_start - current_pos);
            current_pos = search_start;
        }

        // Read data to find the start of a complete JSON line
        unsigned char search_buffer[2048];
        size_t search_bytes;
        if (inflate_read(
                streaming_state_.inflate_state.get(), search_buffer, sizeof(search_buffer) - 1, &search_bytes) != 0)
        {
            throw std::runtime_error("Failed during search phase");
        }

        // Find the last newline before or at our target start position
        size_t relative_target = target_start - current_pos;
        if (relative_target < search_bytes)
        {
            // Look backwards from the target position to find the start of the line
            for (int64_t i = static_cast<int64_t>(relative_target); i >= 0; i--)
            {
                if (i == 0 || search_buffer[i - 1] == '\n')
                {
                    actual_start = current_pos + static_cast<size_t>(i);
                    spdlog::debug("Found JSON line start at position {} (requested {})", actual_start, target_start);
                    break;
                }
            }
        }

        // If we need to skip forward to actual_start, do that after restart
        if (actual_start > (use_checkpoint ? streaming_state_.checkpoint->uc_offset : 0))
        {
            size_t restart_pos = use_checkpoint ? streaming_state_.checkpoint->uc_offset : 0;
            // Restart decompression first
            restart_decompression();
            // Then skip to the actual start position
            skip_bytes(actual_start - restart_pos);
        }
        else
        {
            // Just restart decompression
            restart_decompression();
        }

        return actual_start;
    }

    void skip_bytes(size_t bytes_to_skip)
    {
        CompressionManager::InflationSession session(streaming_state_.inflate_state.get(), false);
        session.skip(bytes_to_skip, skip_buffer, SKIP_BUFFER_SIZE);
    }

    void restart_decompression()
    {
        inflate_cleanup(streaming_state_.inflate_state.get());

        bool use_checkpoint = (streaming_state_.checkpoint != nullptr);
        if (use_checkpoint)
        {
            if (inflate_init_from_checkpoint(streaming_state_.inflate_state.get(),
                                             streaming_state_.file_handle,
                                             streaming_state_.checkpoint.get()) != 0)
            {
                throw std::runtime_error("Failed to reinitialize from checkpoint");
            }
        }
        else
        {
            if (inflate_init(streaming_state_.inflate_state.get(), streaming_state_.file_handle, 0, 0) != 0)
            {
                throw std::runtime_error("Failed to reinitialize inflation");
            }
        }
    }

    bool stream_data_chunk(char *buffer, size_t buffer_size, size_t *bytes_written)
    {
        if (!streaming_state_.decompression_initialized)
        {
            throw std::runtime_error("Streaming session not properly initialized");
        }

        *bytes_written = 0;

        // Unified algorithm for all buffer sizes to ensure consistency
        // Step 1: Read new data if incomplete buffer is empty
        if (streaming_state_.incomplete_buffer_size == 0)
        {
            char temp_buffer[INCOMPLETE_BUFFER_SIZE];
            size_t total_read = 0;

            while (total_read < INCOMPLETE_BUFFER_SIZE)
            {
                // Check if we've reached the target end
                if (streaming_state_.current_position >= streaming_state_.target_end_bytes)
                {
                    // Look for a complete JSON boundary to finish cleanly
                    if (total_read > 0 && find_json_boundary_in_buffer(temp_buffer, total_read))
                    {
                        break;
                    }
                    // If no boundary found and we're past target, we're done
                    streaming_state_.is_finished = true;
                    break;
                }

                size_t bytes_read;
                int result = inflate_read(streaming_state_.inflate_state.get(),
                                          reinterpret_cast<unsigned char *>(temp_buffer + total_read),
                                          INCOMPLETE_BUFFER_SIZE - total_read,
                                          &bytes_read);

                if (result != 0)
                {
                    throw std::runtime_error("Failed during streaming read phase");
                }

                if (bytes_read == 0)
                {
                    // Check if we've reached EOF or there's an error
                    if (feof(streaming_state_.file_handle))
                    {
                        spdlog::debug("Reached EOF during streaming read phase");
                    }
                    else if (ferror(streaming_state_.file_handle))
                    {
                        throw std::runtime_error("File error during streaming read phase");
                    }
                    // EOF reached
                    streaming_state_.is_finished = true;
                    break;
                }

                total_read += bytes_read;
                streaming_state_.current_position += bytes_read;

                spdlog::debug("Read {} bytes from compressed stream, total: {}", bytes_read, total_read);
            }

            // Add read data to incomplete buffer
            if (total_read > 0)
            {
                std::copy(temp_buffer, temp_buffer + total_read, streaming_state_.incomplete_buffer);
                streaming_state_.incomplete_buffer_size = total_read;

                spdlog::debug("Added {} bytes to incomplete buffer", total_read);
            }
        }

        // Step 2: Serve data from incomplete buffer to user buffer
        size_t bytes_to_copy = 0;
        if (streaming_state_.incomplete_buffer_size > 0)
        {
            bytes_to_copy = std::min(buffer_size, streaming_state_.incomplete_buffer_size);

            // Adjust to JSON boundary
            bytes_to_copy = adjust_to_json_boundary(streaming_state_.incomplete_buffer, bytes_to_copy);

            // Copy data to user buffer
            std::copy(streaming_state_.incomplete_buffer, streaming_state_.incomplete_buffer + bytes_to_copy, buffer);

            // Shift remaining data (queue behavior)
            if (bytes_to_copy < streaming_state_.incomplete_buffer_size)
            {
                std::memmove(streaming_state_.incomplete_buffer,
                             streaming_state_.incomplete_buffer + bytes_to_copy,
                             streaming_state_.incomplete_buffer_size - bytes_to_copy);
            }
            streaming_state_.incomplete_buffer_size -= bytes_to_copy;

            spdlog::debug("Served {} bytes to user buffer, {} bytes remaining in incomplete buffer",
                          bytes_to_copy,
                          streaming_state_.incomplete_buffer_size);
        }

        *bytes_written = bytes_to_copy;

        spdlog::debug("Streamed {} bytes (incomplete buffer: {}) (position: {} / {})",
                      bytes_to_copy,
                      streaming_state_.incomplete_buffer_size,
                      streaming_state_.current_position,
                      streaming_state_.target_end_bytes);

        // Check if we have more data to stream
        bool has_more =
            (streaming_state_.incomplete_buffer_size > 0) ||
            (!streaming_state_.is_finished && streaming_state_.current_position < streaming_state_.target_end_bytes);

        return has_more;
    }

    bool find_json_boundary_in_buffer(const char *buffer, size_t buffer_size)
    {
        for (size_t i = 1; i < buffer_size; i++)
        {
            if (buffer[i - 1] == '}' && buffer[i] == '\n')
            {
                return true;
            }
        }
        return false;
    }

    size_t adjust_to_json_boundary(char *buffer, size_t buffer_size)
    {
        // Find the last complete JSON boundary in the buffer
        for (int64_t i = static_cast<int64_t>(buffer_size) - 1; i > 0; i--)
        {
            if (buffer[i - 1] == '}' && buffer[i] == '\n')
            {
                // Adjust current position based on what we're keeping vs discarding
                // size_t bytes_to_discard = buffer_size - (static_cast<size_t>(i) + 1);
                // streaming_state_.current_position -= bytes_to_discard;
                return static_cast<size_t>(i) + 1; // +1 to include newline
            }
        }

        // If no boundary found, we might need to buffer this data for next call
        // For now, just return the full buffer and let the next call handle it
        return buffer_size;
    }

    void initialize_raw_streaming_session(const std::string &gz_path, size_t start_bytes, size_t end_bytes)
    {
        spdlog::debug("Initializing raw streaming session for range [{}, {}] from {}", start_bytes, end_bytes, gz_path);

        // Clean up any existing raw state
        if (raw_streaming_state_.is_active)
        {
            raw_streaming_state_.reset();
        }

        // Setup new raw session
        raw_streaming_state_.current_gz_path = gz_path;
        raw_streaming_state_.start_bytes = start_bytes;
        raw_streaming_state_.target_end_bytes = end_bytes;
        raw_streaming_state_.current_position = start_bytes;
        raw_streaming_state_.is_active = true;
        raw_streaming_state_.is_finished = false;

        // Open file
        raw_streaming_state_.file_handle = fopen(gz_path.c_str(), "rb");
        if (!raw_streaming_state_.file_handle)
        {
            spdlog::error("Failed to open file: {}", gz_path);
            throw std::runtime_error("Failed to open file: " + gz_path);
        }

        // Initialize decompression state
        raw_streaming_state_.inflate_state.reset(new InflateState());
        bool use_checkpoint = false;

        // Try to find a checkpoint near the start position
        raw_streaming_state_.checkpoint.reset(new CheckpointInfo());
        if (find_checkpoint(start_bytes, raw_streaming_state_.checkpoint.get()) == 0)
        {
            if (inflate_init_from_checkpoint(raw_streaming_state_.inflate_state.get(),
                                             raw_streaming_state_.file_handle,
                                             raw_streaming_state_.checkpoint.get()) == 0)
            {
                use_checkpoint = true;
                spdlog::debug("Using checkpoint at uncompressed offset {} for raw target {}",
                              raw_streaming_state_.checkpoint->uc_offset,
                              start_bytes);
            }
            else
            {
                spdlog::debug("Failed to initialize from checkpoint for raw read, falling back to sequential read");
                free_checkpoint(raw_streaming_state_.checkpoint.get());
                raw_streaming_state_.checkpoint.reset();
            }
        }
        else
        {
            raw_streaming_state_.checkpoint.reset();
        }

        // Fallback to sequential read if no checkpoint or checkpoint failed
        if (!use_checkpoint)
        {
            if (inflate_init(raw_streaming_state_.inflate_state.get(), raw_streaming_state_.file_handle, 0, 0) != 0)
            {
                throw std::runtime_error("Failed to initialize raw inflation");
            }
        }

        // Skip to the start position
        size_t current_pos = use_checkpoint ? raw_streaming_state_.checkpoint->uc_offset : 0;
        if (start_bytes > current_pos)
        {
            skip_raw_bytes(start_bytes - current_pos);
        }

        raw_streaming_state_.decompression_initialized = true;
        spdlog::debug("Raw streaming session initialized: start={}, target_end={}", start_bytes, end_bytes);
    }

    void skip_raw_bytes(size_t bytes_to_skip)
    {
        CompressionManager::InflationSession session(raw_streaming_state_.inflate_state.get(), false);
        session.skip(bytes_to_skip, skip_buffer, SKIP_BUFFER_SIZE);
    }

    bool stream_raw_data_chunk(char *buffer, size_t buffer_size, size_t *bytes_written)
    {
        if (!raw_streaming_state_.decompression_initialized)
        {
            throw std::runtime_error("Raw streaming session not properly initialized");
        }

        *bytes_written = 0;

        // Check if we've reached the target end
        if (raw_streaming_state_.current_position >= raw_streaming_state_.target_end_bytes)
        {
            raw_streaming_state_.is_finished = true;
            return false;
        }

        // Calculate how much we can read without exceeding the target
        size_t max_read = raw_streaming_state_.target_end_bytes - raw_streaming_state_.current_position;
        size_t read_size = std::min(buffer_size, max_read);

        size_t bytes_read;
        int result = inflate_read(raw_streaming_state_.inflate_state.get(),
                                  reinterpret_cast<unsigned char *>(buffer),
                                  read_size,
                                  &bytes_read);

        if (result != 0)
        {
            throw std::runtime_error("Failed during raw streaming read phase");
        }

        if (bytes_read == 0)
        {
            // Check if we've reached EOF or there's an error
            if (feof(raw_streaming_state_.file_handle))
            {
                spdlog::debug("Reached EOF during raw streaming read phase");
            }
            else if (ferror(raw_streaming_state_.file_handle))
            {
                throw std::runtime_error("File error during raw streaming read phase");
            }
            // EOF reached
            raw_streaming_state_.is_finished = true;
            return false;
        }

        raw_streaming_state_.current_position += bytes_read;
        *bytes_written = bytes_read;

        spdlog::debug("Raw streamed {} bytes (position: {} / {})",
                      bytes_read,
                      raw_streaming_state_.current_position,
                      raw_streaming_state_.target_end_bytes);

        // Check if we have more data to stream
        bool has_more = !raw_streaming_state_.is_finished &&
                        raw_streaming_state_.current_position < raw_streaming_state_.target_end_bytes;

        return has_more;
    }

    void cleanup_streaming_state()
    {
        // Only clean up if we actually have initialized state
        if (streaming_state_.decompression_initialized && streaming_state_.inflate_state)
        {
            inflate_cleanup(streaming_state_.inflate_state.get());
        }
        if (streaming_state_.checkpoint)
        {
            free_checkpoint(streaming_state_.checkpoint.get());
        }
        streaming_state_.reset();

        // Clean up raw streaming state too
        if (raw_streaming_state_.decompression_initialized && raw_streaming_state_.inflate_state)
        {
            inflate_cleanup(raw_streaming_state_.inflate_state.get());
        }
        if (raw_streaming_state_.checkpoint)
        {
            free_checkpoint(raw_streaming_state_.checkpoint.get());
        }
        raw_streaming_state_.reset();
    }

    std::string gz_path_;
    std::string idx_path_;
    sqlite3 *db_;
    bool is_open_;
    
    // New architecture components
    std::unique_ptr<DatabaseHelper> db_helper_;
    std::unique_ptr<StreamingSessionFactory> session_factory_;
    std::unique_ptr<JsonStreamingSession> json_session_;
    std::unique_ptr<RawStreamingSession> raw_session_;

    // Legacy streaming state management (to be removed)
    struct StreamingState
    {
        std::string current_gz_path;
        size_t start_bytes;
        size_t current_position;                        // Current byte position in range
        size_t target_end_bytes;                        // End position in bytes
        size_t actual_start_bytes;                      // Adjusted start (beginning of JSON line)
        bool is_active;                                 // True if streaming is in progress
        bool is_finished;                               // True if reached end of range
        char incomplete_buffer[INCOMPLETE_BUFFER_SIZE]; // Buffer for incomplete JSON line
        size_t incomplete_buffer_size;

        std::unique_ptr<InflateState> inflate_state;
        std::unique_ptr<CheckpointInfo> checkpoint;
        FILE *file_handle;
        bool decompression_initialized;

        StreamingState()
            : start_bytes(0), current_position(0), target_end_bytes(0), actual_start_bytes(0), is_active(false),
              is_finished(false), incomplete_buffer_size(0), file_handle(nullptr), decompression_initialized(false)
        {
        }

        void reset()
        {
            current_gz_path.clear();
            start_bytes = 0;
            current_position = 0;
            target_end_bytes = 0;
            actual_start_bytes = 0;
            std::fill(std::begin(incomplete_buffer), std::end(incomplete_buffer), 0);
            incomplete_buffer_size = 0;
            is_active = false;
            is_finished = false;
            if (file_handle)
            {
                fclose(file_handle);
                file_handle = nullptr;
            }
            inflate_state.reset();
            checkpoint.reset();
            decompression_initialized = false;
        }
    } streaming_state_;

    // Raw streaming state management (similar to streaming_state_ but without JSON boundary handling)
    struct RawStreamingState
    {
        std::string current_gz_path;
        size_t start_bytes;
        size_t current_position; // Current byte position in range
        size_t target_end_bytes; // End position in bytes
        bool is_active;          // True if streaming is in progress
        bool is_finished;        // True if reached end of range

        std::unique_ptr<InflateState> inflate_state;
        std::unique_ptr<CheckpointInfo> checkpoint;
        FILE *file_handle;
        bool decompression_initialized;

        RawStreamingState()
            : start_bytes(0), current_position(0), target_end_bytes(0), is_active(false), is_finished(false),
              file_handle(nullptr), decompression_initialized(false)
        {
        }

        void reset()
        {
            current_gz_path.clear();
            start_bytes = 0;
            current_position = 0;
            target_end_bytes = 0;
            is_active = false;
            is_finished = false;
            if (file_handle)
            {
                fclose(file_handle);
                file_handle = nullptr;
            }
            inflate_state.reset();
            checkpoint.reset();
            decompression_initialized = false;
        }
    } raw_streaming_state_;
};

// ==============================================================================
// C++ Public Interface Implementation
// ==============================================================================

Reader::Reader(const std::string &gz_path, const std::string &idx_path) : pImpl_(new Impl(gz_path, idx_path)) {}

Reader::~Reader() = default;

Reader::Reader(Reader &&other) noexcept : pImpl_(other.pImpl_.release()) {}

Reader &Reader::operator=(Reader &&other) noexcept
{
    if (this != &other)
    {
        pImpl_.reset(other.pImpl_.release());
    }
    return *this;
}

size_t Reader::get_max_bytes() const
{
    return pImpl_->get_max_bytes();
}

size_t Reader::read(const std::string &gz_path,
                    size_t start_bytes,
                    size_t end_bytes,
                    char *buffer,
                    size_t buffer_size)
{
    return pImpl_->read(gz_path, start_bytes, end_bytes, buffer, buffer_size);
}

size_t Reader::read(size_t start_bytes, size_t end_bytes, char *buffer, size_t buffer_size)
{
    return pImpl_->read(pImpl_->get_gz_path(), start_bytes, end_bytes, buffer, buffer_size);
}

size_t Reader::read_raw(const std::string &gz_path,
                        size_t start_bytes,
                        size_t end_bytes,
                        char *buffer,
                        size_t buffer_size)
{
    return pImpl_->read_raw(gz_path, start_bytes, end_bytes, buffer, buffer_size);
}

size_t Reader::read_raw(size_t start_bytes, size_t end_bytes, char *buffer, size_t buffer_size)
{
    return pImpl_->read_raw(pImpl_->get_gz_path(), start_bytes, end_bytes, buffer, buffer_size);
}

void Reader::reset()
{
    pImpl_->reset();
}

bool Reader::is_valid() const
{
    return pImpl_ && pImpl_->is_valid();
}

const std::string &Reader::get_gz_path() const
{
    return pImpl_->get_gz_path();
}

const std::string &Reader::get_idx_path() const
{
    return pImpl_->get_idx_path();
}

} // namespace reader
} // namespace dft

// ==============================================================================
// C API Implementation (wraps C++ implementation)
// ==============================================================================

extern "C"
{

    dft_reader_handle_t dft_reader_create(const char *gz_path, const char *idx_path)
    {
        if (!gz_path || !idx_path)
        {
            spdlog::error("Both gz_path and idx_path cannot be null");
            return nullptr;
        }

        try
        {
            auto *reader = new dft::reader::Reader(gz_path, idx_path);
            return static_cast<dft_reader_handle_t>(reader);
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to create DFT reader: {}", e.what());
            return nullptr;
        }
    }

    void dft_reader_destroy(dft_reader_handle_t reader)
    {
        if (reader)
        {
            delete static_cast<dft::reader::Reader *>(reader);
        }
    }

    int dft_reader_get_max_bytes(dft_reader_handle_t reader, size_t *max_bytes)
    {
        if (!reader || !max_bytes)
        {
            return -1;
        }

        try
        {
            auto *cpp_reader = static_cast<dft::reader::Reader *>(reader);
            *max_bytes = cpp_reader->get_max_bytes();
            return 0;
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to get max bytes: {}", e.what());
            return -1;
        }
    }

    int dft_reader_read(dft_reader_handle_t reader,
                        const char *gz_path,
                        size_t start_bytes,
                        size_t end_bytes,
                        char *buffer,
                        size_t buffer_size)
    {
        if (!reader || !gz_path || !buffer || buffer_size == 0)
        {
            return -1;
        }

        try
        {
            auto *cpp_reader = static_cast<dft::reader::Reader *>(reader);
            size_t bytes_read = cpp_reader->read(gz_path, start_bytes, end_bytes, buffer, buffer_size);
            return static_cast<int>(bytes_read);
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to read: {}", e.what());
            return -1;
        }
    }

    int dft_reader_read_raw(dft_reader_handle_t reader,
                            const char *gz_path,
                            size_t start_bytes,
                            size_t end_bytes,
                            char *buffer,
                            size_t buffer_size)
    {
        if (!reader || !gz_path || !buffer || buffer_size == 0)
        {
            return -1;
        }

        try
        {
            auto *cpp_reader = static_cast<dft::reader::Reader *>(reader);
            size_t bytes_read = cpp_reader->read_raw(gz_path, start_bytes, end_bytes, buffer, buffer_size);
            return static_cast<int>(bytes_read);
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to read raw: {}", e.what());
            return -1;
        }
    }

    void dft_reader_reset(dft_reader_handle_t reader)
    {
        if (reader)
        {
            static_cast<dft::reader::Reader *>(reader)->reset();
        }
    }

} // extern "C"
