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

namespace dft
{
namespace reader
{

class Reader::Impl
{
  public:
    Impl(const std::string &gz_path, const std::string &idx_path)
        : gz_path_(gz_path), idx_path_(idx_path), db_(nullptr), is_open_(false)
    {
        if (sqlite3_open(idx_path.c_str(), &db_) != SQLITE_OK)
        {
            throw std::runtime_error("Failed to open index database: " + std::string(sqlite3_errmsg(db_)));
        }
        is_open_ = true;
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

        sqlite3_stmt *stmt;
        const char *sql = "SELECT MAX(uc_offset + uc_size) FROM chunks";

        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, NULL) != SQLITE_OK)
        {
            throw std::runtime_error("Failed to prepare max bytes query: " + std::string(sqlite3_errmsg(db_)));
        }

        size_t max_bytes = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            sqlite3_int64 max_val = sqlite3_column_int64(stmt, 0);
            if (max_val >= 0)
            {
                max_bytes = static_cast<size_t>(max_val);
                spdlog::debug("Maximum bytes available: {}", max_bytes);
            }
            else
            {
                // no chunks found or empty result
                max_bytes = 0;
                spdlog::debug("No chunks found, maximum bytes: 0");
            }
        }
        else
        {
            sqlite3_finalize(stmt);
            throw std::runtime_error("Failed to execute max bytes query: " + std::string(sqlite3_errmsg(db_)));
        }

        sqlite3_finalize(stmt);
        return max_bytes;
    }

    bool read(const std::string &gz_path, size_t start_bytes, size_t end_bytes,
             char *buffer, size_t buffer_size, size_t *bytes_written)
    {
        if (!is_open_ || !db_)
        {
            throw std::runtime_error("Reader is not open");
        }

        if (!buffer || !bytes_written || buffer_size == 0)
        {
            throw std::invalid_argument("Invalid buffer parameters");
        }

        if (start_bytes >= end_bytes)
        {
            throw std::invalid_argument("start_bytes must be less than end_bytes");
        }
        *bytes_written = 0;
        if (!streaming_state_.is_active || 
            streaming_state_.current_gz_path != gz_path ||
            streaming_state_.start_bytes != start_bytes ||
            streaming_state_.target_end_bytes != end_bytes ||
            streaming_state_.is_finished)
        {
            initialize_streaming_session(gz_path, start_bytes, end_bytes);
        }
        if (streaming_state_.is_finished)
        {
            return false;
        }
        return stream_data_chunk(buffer, buffer_size, bytes_written);
    }

    void reset()
    {
        if (!is_open_ || !db_)
        {
            throw std::runtime_error("Reader is not open");
        }

        streaming_state_ = {};
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
    static constexpr size_t SKIP_BUFFER_SIZE = 65536; // 64 KB
    #define INCOMPLETE_BUFFER_SIZE 2 * 1024 * 1024 // 2 MB, big enough to hold multiple JSON lines
    unsigned char skip_buffer[SKIP_BUFFER_SIZE];

    struct InflateState
    {
        z_stream zs;
        FILE *file;
        unsigned char in[INFLATE_CHUNK_SIZE];
        int bits;
        size_t c_off;
    };

    struct CheckpointInfo
    {
        size_t uc_offset;
        size_t c_offset;
        int bits;
        // unsigned char *dict_compressed;
        std::vector<unsigned char> dict_compressed;
        size_t dict_compressed_size;
    };

    int inflate_init(InflateState *state, FILE *f, size_t c_off, int bits) const
    {
        memset(state, 0, sizeof(*state));
        state->file = f;
        state->c_off = c_off;
        state->bits = bits;

        if (inflateInit2(&state->zs, 15 + 16) != Z_OK)
        {
            return -1;
        }
        
        if (fseeko(f, static_cast<off_t>(c_off), SEEK_SET) != 0)
        {
            spdlog::error("Failed to seek to compressed offset: {}", c_off);
            inflateEnd(&state->zs);
            return -1;
        }

        return 0;
    }

    void inflate_cleanup(InflateState *state) const
    {
        inflateEnd(&state->zs);
    }

    int inflate_read(InflateState *state, unsigned char *out, size_t out_size, size_t *bytes_read) const
    {
        state->zs.next_out = out;
        state->zs.avail_out = static_cast<uInt>(out_size);
        *bytes_read = 0;

        while (state->zs.avail_out > 0)
        {
            if (state->zs.avail_in == 0)
            {
                size_t n = fread(state->in, 1, sizeof(state->in), state->file);
                if (n == 0)
                {
                    if (ferror(state->file))
                    {
                        spdlog::error("Error reading from file during inflate_read");
                        return -1;
                    }
                    break; // EOF
                }
                state->zs.next_in = state->in;
                state->zs.avail_in = static_cast<uInt>(n);
            }

            int ret = inflate(&state->zs, Z_NO_FLUSH);
            if (ret == Z_STREAM_END)
            {
                break;
            }
            if (ret != Z_OK)
            {
                spdlog::debug(
                    "inflate() failed with error: {} ({})", ret, state->zs.msg ? state->zs.msg : "no message");
                return -1;
            }
        }

        *bytes_read = out_size - state->zs.avail_out;
        return 0;
    }

    int find_checkpoint(size_t target_uc_offset, CheckpointInfo *checkpoint) const
    {
        if (!is_open_ || !db_)
        {
            spdlog::debug("Reader not open for checkpoint lookup");
            return -1;
        }

        // First get the file_id for the current gz file
        sqlite3_stmt *file_stmt;
        const char *file_sql = "SELECT id FROM files WHERE logical_name = ? LIMIT 1";
        if (sqlite3_prepare_v2(db_, file_sql, -1, &file_stmt, NULL) != SQLITE_OK)
        {
            spdlog::debug("Failed to prepare file lookup query");
            return -1;
        }

        sqlite3_bind_text(file_stmt, 1, gz_path_.c_str(), -1, SQLITE_STATIC);

        int file_id = -1;
        if (sqlite3_step(file_stmt) == SQLITE_ROW)
        {
            file_id = sqlite3_column_int(file_stmt, 0);
        }
        sqlite3_finalize(file_stmt);

        if (file_id == -1)
        {
            spdlog::debug("File not found in database: {}", gz_path_);
            return -1;
        }

        sqlite3_stmt *stmt;
        const char *sql = "SELECT uc_offset, c_offset, bits, dict_compressed "
                          "FROM checkpoints "
                          "WHERE file_id = ? AND uc_offset <= ? "
                          "ORDER BY uc_offset DESC "
                          "LIMIT 1";

        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, NULL) != SQLITE_OK)
        {
            spdlog::debug("Failed to prepare checkpoint query");
            return -1;
        }

        sqlite3_bind_int(stmt, 1, file_id);
        sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(target_uc_offset));

        int ret = -1;
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            checkpoint->uc_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 0));
            checkpoint->c_offset = static_cast<size_t>(sqlite3_column_int64(stmt, 1));
            checkpoint->bits = sqlite3_column_int(stmt, 2);

            checkpoint->dict_compressed_size = static_cast<size_t>(sqlite3_column_bytes(stmt, 3));
            checkpoint->dict_compressed.resize(checkpoint->dict_compressed_size);
            memcpy(checkpoint->dict_compressed.data(), sqlite3_column_blob(stmt, 3), checkpoint->dict_compressed_size);
            if (checkpoint->dict_compressed.size() > 0)
            {
                ret = 0;
                spdlog::debug(
                    "Found checkpoint at uc_offset={} for target={}", checkpoint->uc_offset, target_uc_offset);
            }
            else
            {
                spdlog::error("Failed to allocate memory for checkpoint dictionary of size {}", checkpoint->dict_compressed_size);
            }
        }
        else
        {
            spdlog::debug("No checkpoint found for target uc_offset={}", target_uc_offset);
        }

        sqlite3_finalize(stmt);
        return ret;
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
            spdlog::error("inflate failed during window decompression with error: {} ({})", ret, zs.msg ? zs.msg : "no message");
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
        if (decompress_window(checkpoint->dict_compressed.data(), checkpoint->dict_compressed.size(), window, &window_size) != 0)
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
        spdlog::debug("Initializing streaming session for range [{}, {}] from {}", 
                      start_bytes, end_bytes, gz_path);

        // Clean up any existing state - SAFELY
        if (streaming_state_.is_active) {
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
                             streaming_state_.checkpoint->uc_offset, start_bytes);
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
                     streaming_state_.actual_start_bytes, end_bytes);
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
        if (inflate_read(streaming_state_.inflate_state.get(), search_buffer, 
                        sizeof(search_buffer) - 1, &search_bytes) != 0)
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
                    spdlog::debug("Found JSON line start at position {} (requested {})", 
                                 actual_start, target_start);
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
        if (bytes_to_skip == 0) return;
        size_t remaining_skip = bytes_to_skip;
        while (remaining_skip > 0)
        {
            size_t to_skip = (remaining_skip > SKIP_BUFFER_SIZE) ? SKIP_BUFFER_SIZE : remaining_skip;
            size_t skipped;
            int inflate_result = inflate_read(streaming_state_.inflate_state.get(), skip_buffer, to_skip, &skipped);
            if (inflate_result != 0)
            {
                throw std::runtime_error("Failed during skip phase");
            }
            if (skipped == 0)
            {
                // Check if we've reached EOF or there's an error
                if (feof(streaming_state_.file_handle))
                {
                    spdlog::debug("Reached EOF during skip phase");
                }
                else if (ferror(streaming_state_.file_handle))
                {
                    throw std::runtime_error("File error during skip phase");
                }
                break;
            }
            remaining_skip -= skipped;
        }
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
                                        INCOMPLETE_BUFFER_SIZE - total_read, &bytes_read);

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
            std::copy(streaming_state_.incomplete_buffer,
                     streaming_state_.incomplete_buffer + bytes_to_copy, buffer);
            
            // Shift remaining data (queue behavior)
            if (bytes_to_copy < streaming_state_.incomplete_buffer_size)
            {
                std::memmove(streaming_state_.incomplete_buffer,
                           streaming_state_.incomplete_buffer + bytes_to_copy,
                           streaming_state_.incomplete_buffer_size - bytes_to_copy);
            }
            streaming_state_.incomplete_buffer_size -= bytes_to_copy;
            
            spdlog::debug("Served {} bytes to user buffer, {} bytes remaining in incomplete buffer", 
                         bytes_to_copy, streaming_state_.incomplete_buffer_size);
        }

        *bytes_written = bytes_to_copy;
        
        spdlog::debug("Streamed {} bytes (incomplete buffer: {}) (position: {} / {})",
                     bytes_to_copy, streaming_state_.incomplete_buffer_size, 
                     streaming_state_.current_position, streaming_state_.target_end_bytes);

        // Check if we have more data to stream
        bool has_more = (streaming_state_.incomplete_buffer_size > 0) ||
                       (!streaming_state_.is_finished && 
                        streaming_state_.current_position < streaming_state_.target_end_bytes);

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
    }

    std::string gz_path_;
    std::string idx_path_;
    sqlite3 *db_;
    bool is_open_;

    // Streaming state management
    struct StreamingState {
        std::string current_gz_path;
        size_t start_bytes;
        size_t current_position;  // Current byte position in range
        size_t target_end_bytes;  // End position in bytes
        size_t actual_start_bytes; // Adjusted start (beginning of JSON line)
        bool is_active;  // True if streaming is in progress
        bool is_finished;  // True if reached end of range
        char incomplete_buffer[INCOMPLETE_BUFFER_SIZE];  // Buffer for incomplete JSON line
        size_t incomplete_buffer_size;

        std::unique_ptr<InflateState> inflate_state;
        std::unique_ptr<CheckpointInfo> checkpoint;
        FILE* file_handle;
        bool decompression_initialized;

        StreamingState() : start_bytes(0), current_position(0), 
                          target_end_bytes(0), actual_start_bytes(0),
                          is_active(false), is_finished(false),
                          incomplete_buffer_size(0),
                          file_handle(nullptr), decompression_initialized(false)
                          {}

        void reset() {
            current_gz_path.clear();
            start_bytes = 0;
            current_position = 0;
            target_end_bytes = 0;
            actual_start_bytes = 0;
            std::fill(std::begin(incomplete_buffer), std::end(incomplete_buffer), 0);
            incomplete_buffer_size = 0;
            is_active = false;
            is_finished = false;           
            if (file_handle) {
                fclose(file_handle);
                file_handle = nullptr;
            }
            inflate_state.reset();
            checkpoint.reset();
            decompression_initialized = false;
        }
    } streaming_state_;
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

bool Reader::read(const std::string &gz_path, size_t start_bytes, size_t end_bytes,
                  char *buffer, size_t buffer_size, size_t *bytes_written)
{
    return pImpl_->read(gz_path, start_bytes, end_bytes, buffer, buffer_size, bytes_written);
}

bool Reader::read(size_t start_bytes, size_t end_bytes,
                  char *buffer, size_t buffer_size, size_t *bytes_written)
{
    return pImpl_->read(pImpl_->get_gz_path(), start_bytes, end_bytes, buffer, buffer_size, bytes_written);
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
                       size_t buffer_size,
                       size_t *bytes_written)
    {
        if (!reader || !gz_path || !buffer || !bytes_written || buffer_size == 0)
        {
            return -1;
        }

        try
        {
            auto *cpp_reader = static_cast<dft::reader::Reader *>(reader);
            bool has_more = cpp_reader->read(gz_path, start_bytes, end_bytes, buffer, buffer_size, bytes_written);
            return has_more ? 1 : 0;
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to read: {}", e.what());
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
