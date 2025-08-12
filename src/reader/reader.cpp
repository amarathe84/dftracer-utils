#include "reader.h"
#include "platform_compat.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <spdlog/spdlog.h>
#include <sqlite3.h>
#include <zlib.h>

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

    std::pair<Reader::Buffer, size_t>
    read_range_bytes(const std::string &gz_path, size_t start_bytes, size_t end_bytes) const
    {
        if (!is_open_ || !db_)
        {
            throw std::runtime_error("Reader is not open");
        }

        if (start_bytes >= end_bytes)
        {
            throw std::invalid_argument("start_bytes must be less than end_bytes");
        }

        spdlog::debug("Reading byte range [{}, {}] from {}...", start_bytes, end_bytes, gz_path);

        FILE *f = fopen(gz_path.c_str(), "rb");
        if (!f)
        {
            throw std::runtime_error("Failed to open file: " + gz_path);
        }

        InflateState inflate_state;
        CheckpointInfo checkpoint;
        bool use_checkpoint = false;

        // Try to find a checkpoint near the start position
        if (find_checkpoint(start_bytes, &checkpoint) == 0)
        {
            if (inflate_init_from_checkpoint(&inflate_state, f, &checkpoint) == 0)
            {
                use_checkpoint = true;
                spdlog::debug("Using checkpoint at uncompressed offset {} for target {}", 
                              checkpoint.uc_offset, start_bytes);
            }
            else
            {
                spdlog::debug("Failed to initialize from checkpoint, falling back to sequential read");
                free_checkpoint(&checkpoint);
            }
        }

        // Fallback to sequential read if no checkpoint or checkpoint failed
        if (!use_checkpoint)
        {
            if (inflate_init(&inflate_state, f, 0, 0) != 0)
            {
                fclose(f);
                throw std::runtime_error("Failed to initialize inflation");
            }
        }

        // step 1: find the actual start position (beginning of a complete JSON line)
        size_t actual_start = start_bytes;
        size_t current_pos = use_checkpoint ? checkpoint.uc_offset : 0;
        
        // If using checkpoint, we should already be positioned close to the target
        
        if (start_bytes > current_pos)
        {
            // seek a bit before the requested start to find the beginning of the JSON line
            size_t search_start = (start_bytes >= 512) ? start_bytes - 512 : current_pos;

            // skip to search start position
            if (search_start > current_pos)
            {
                unsigned char *skip_buffer = static_cast<unsigned char *>(malloc(65536));
                if (!skip_buffer)
                {
                    inflate_cleanup(&inflate_state);
                    fclose(f);
                    throw std::runtime_error("Failed to allocate skip buffer");
                }

                size_t remaining_skip = search_start - current_pos;
                while (remaining_skip > 0)
                {
                    size_t to_skip = (remaining_skip > 65536) ? 65536 : remaining_skip;
                    size_t skipped;
                    int inflate_result = inflate_read(&inflate_state, skip_buffer, to_skip, &skipped);
                    if (inflate_result != 0)
                    {
                        spdlog::debug("inflate_read failed during skip phase with error: {}", inflate_result);
                        spdlog::debug("  to_skip: {}, skipped: {}, remaining_skip: {}", to_skip, skipped, remaining_skip);
                        free(skip_buffer);
                        inflate_cleanup(&inflate_state);
                        fclose(f);
                        if (use_checkpoint) free_checkpoint(&checkpoint);
                        throw std::runtime_error("Failed during skip phase");
                    }
                    if (skipped == 0)
                        break;
                    remaining_skip -= skipped;
                    current_pos += skipped;
                }
                free(skip_buffer);
            }

            // read data to find the start of a complete JSON line
            unsigned char search_buffer[2048];
            size_t search_bytes;
            if (inflate_read(&inflate_state, search_buffer, sizeof(search_buffer) - 1, &search_bytes) != 0)
            {
                inflate_cleanup(&inflate_state);
                fclose(f);
                throw std::runtime_error("Failed during search phase");
            }

            // find the last newline before or at our target start position
            size_t relative_target = start_bytes - current_pos;
            if (relative_target < search_bytes)
            {
                // look backwards from the target position to find the start of the line
                for (int64_t i = static_cast<int64_t>(relative_target); i >= 0; i--)
                {
                    if (i == 0 || search_buffer[i - 1] == '\n')
                    {
                        actual_start = current_pos + i;
                        spdlog::debug("Found JSON line start at position {} (requested {})", actual_start, start_bytes);
                        break;
                    }
                }
            }

            // restart decompression (from checkpoint if available)
            inflate_cleanup(&inflate_state);
            size_t restart_pos = 0;
            if (use_checkpoint)
            {
                if (inflate_init_from_checkpoint(&inflate_state, f, &checkpoint) != 0)
                {
                    fclose(f);
                    free_checkpoint(&checkpoint);
                    throw std::runtime_error("Failed to reinitialize from checkpoint");
                }
                restart_pos = checkpoint.uc_offset;
            }
            else
            {
                if (inflate_init(&inflate_state, f, 0, 0) != 0)
                {
                    fclose(f);
                    throw std::runtime_error("Failed to reinitialize inflation");
                }
            }

            // skip to actual start
            if (actual_start > restart_pos)
            {
                unsigned char *skip_buffer = static_cast<unsigned char *>(malloc(65536));
                if (!skip_buffer)
                {
                    inflate_cleanup(&inflate_state);
                    fclose(f);
                    throw std::runtime_error("Failed to allocate skip buffer");
                }

                size_t remaining_skip = actual_start - restart_pos;
                while (remaining_skip > 0)
                {
                    size_t to_skip = (remaining_skip > 65536) ? 65536 : remaining_skip;
                    size_t skipped;
                    int inflate_result = inflate_read(&inflate_state, skip_buffer, to_skip, &skipped);
                    if (inflate_result != 0)
                    {
                        spdlog::debug("inflate_read failed during final skip phase with error: {}", inflate_result);
                        spdlog::debug("  to_skip: {}, skipped: {}, remaining_skip: {}", to_skip, skipped, remaining_skip);
                        free(skip_buffer);
                        inflate_cleanup(&inflate_state);
                        fclose(f);
                        if (use_checkpoint) free_checkpoint(&checkpoint);
                        throw std::runtime_error("Failed during final skip phase");
                    }
                    if (skipped == 0)
                        break;
                    remaining_skip -= skipped;
                }
                free(skip_buffer);
            }
        }

        // step 1: read data until we find a complete JSON line past the requested end
        size_t target_size = end_bytes - start_bytes;
        size_t original_target_size = target_size;   // for debugging
        size_t buffer_capacity = target_size + 8192; // extra space for complete lines
        char *output = static_cast<char *>(malloc(buffer_capacity + 1));
        if (!output)
        {
            inflate_cleanup(&inflate_state);
            fclose(f);
            if (use_checkpoint) free_checkpoint(&checkpoint);
            throw std::runtime_error("Failed to allocate output buffer");
        }

        size_t total_read = 0;
        current_pos = actual_start;
        bool found_end_boundary = false;

        // always read in chunks and look for complete JSON line boundaries
        while (total_read < buffer_capacity && !found_end_boundary)
        {
            // grow buffer if needed
            if (total_read + 4096 > buffer_capacity)
            {
                buffer_capacity *= 2;
                char *new_buffer = static_cast<char *>(realloc(output, buffer_capacity + 1));
                if (!new_buffer)
                {
                    free(output);
                    inflate_cleanup(&inflate_state);
                    fclose(f);
                    if (use_checkpoint) free_checkpoint(&checkpoint);
                    throw std::runtime_error("Failed to grow buffer");
                }
                output = new_buffer;
            }

            size_t chunk_size = std::min(static_cast<size_t>(4096), buffer_capacity - total_read);
            size_t bytes_read;
            if (inflate_read(
                    &inflate_state, reinterpret_cast<unsigned char *>(output + total_read), chunk_size, &bytes_read) !=
                0)
            {
                free(output);
                inflate_cleanup(&inflate_state);
                fclose(f);
                if (use_checkpoint) free_checkpoint(&checkpoint);
                throw std::runtime_error("Failed during read phase");
            }

            if (bytes_read == 0)
            {
                break; // EOF
            }

            total_read += bytes_read;
            current_pos += bytes_read;

            // only look for boundaries if we've read past the requested end_bytes
            // AND we have at least as much data as originally requested
            if (current_pos >= end_bytes && total_read >= original_target_size)
            {
                // find the last complete JSON boundary after the requested end position
                // scan the entire buffer to find all boundaries and pick the one closest to end_bytes
                size_t best_boundary_pos = SIZE_MAX;

                for (size_t i = 1; i < total_read; i++)
                {
                    if (output[i - 1] == '}' && output[i] == '\n')
                    {
                        size_t absolute_pos = actual_start + i + 1; // +1 to include the newline
                        if (absolute_pos >= end_bytes)
                        {
                            // this boundary is at or past our target - candidate for truncation
                            if (best_boundary_pos == SIZE_MAX || i < best_boundary_pos)
                            {
                                best_boundary_pos = i + 1; // +1 to include newline
                            }
                        }
                    }
                }

                if (best_boundary_pos != SIZE_MAX)
                {
                    total_read = best_boundary_pos;
                    found_end_boundary = true;
                    break;
                }
            }
        }

        output[total_read] = '\0';

        spdlog::debug("Read {} bytes from adjusted range [{}, {}) (requested [{}, {}))",
                      total_read,
                      actual_start,
                      actual_start + total_read,
                      start_bytes,
                      end_bytes);

        inflate_cleanup(&inflate_state);
        fclose(f);
        if (use_checkpoint) free_checkpoint(&checkpoint);

        // wrap in smart pointer for automatic cleanup
        Reader::Buffer buffer(output, std::free);
        return std::make_pair(std::move(buffer), total_read);
    }

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
    // size of input buffer for decompression
    static constexpr size_t CHUNK_SIZE = 16384;

    struct InflateState
    {
        z_stream zs;
        FILE *file;
        unsigned char in[CHUNK_SIZE];
        int bits;
        size_t c_off;
    };

    struct CheckpointInfo
    {
        size_t uc_offset;
        size_t c_offset;
        int bits;
        unsigned char *dict_compressed;
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

        // seek to compressed offset
        if (fseeko(f, static_cast<off_t>(c_off), SEEK_SET) != 0)
        {
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
                spdlog::debug("inflate() failed with error: {} ({})", ret, 
                              state->zs.msg ? state->zs.msg : "no message");
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
            checkpoint->dict_compressed = static_cast<unsigned char *>(malloc(checkpoint->dict_compressed_size));
            if (checkpoint->dict_compressed)
            {
                memcpy(checkpoint->dict_compressed, sqlite3_column_blob(stmt, 3), checkpoint->dict_compressed_size);
                ret = 0;
                spdlog::debug("Found checkpoint at uc_offset={} for target={}", checkpoint->uc_offset, target_uc_offset);
            }
            else
            {
                spdlog::debug("Failed to allocate memory for checkpoint dictionary");
            }
        }
        else
        {
            spdlog::debug("No checkpoint found for target uc_offset={}", target_uc_offset);
        }

        sqlite3_finalize(stmt);
        return ret;
    }

    int decompress_window(const unsigned char *compressed, size_t compressed_size, unsigned char *window, size_t *window_size) const
    {
        z_stream zs;
        memset(&zs, 0, sizeof(zs));
        
        if (inflateInit(&zs) != Z_OK)
        {
            return -1;
        }
        
        zs.next_in = const_cast<unsigned char *>(compressed);
        zs.avail_in = static_cast<uInt>(compressed_size);
        zs.next_out = window;
        zs.avail_out = static_cast<uInt>(*window_size);
        
        int ret = inflate(&zs, Z_FINISH);
        if (ret != Z_STREAM_END)
        {
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
            return -1;
        }

        // If we have partial bits, read the extra byte (following zran approach)
        int ch = 0;
        if (checkpoint->bits != 0) {
            ch = fgetc(f);
            if (ch == EOF) {
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
        if (inflateReset2(&state->zs, -15) != Z_OK)  // RAW mode
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
                spdlog::debug("inflatePrime failed");
                inflateEnd(&state->zs);
                return -1;
            }
        }

        // Decompress the saved dictionary
        unsigned char window[32768];
        size_t window_size = 32768;
        if (decompress_window(checkpoint->dict_compressed, checkpoint->dict_compressed_size, window, &window_size) != 0)
        {
            inflateEnd(&state->zs);
            return -1;
        }

        // Set dictionary (following zran: inflateSetDictionary)
        if (inflateSetDictionary(&state->zs, window, static_cast<uInt>(window_size)) != Z_OK)
        {
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
        
        return 0;
    }

    void free_checkpoint(CheckpointInfo *checkpoint) const
    {
        if (checkpoint && checkpoint->dict_compressed)
        {
            free(checkpoint->dict_compressed);
            checkpoint->dict_compressed = nullptr;
        }
    }

    int find_gzip_deflate_start(FILE *f) const
    {
        // Parse gzip header to find where deflate stream starts
        unsigned char header[10];
        if (fread(header, 1, 10, f) != 10)
        {
            return -1;
        }
        
        // Check gzip magic number
        if (header[0] != 0x1f || header[1] != 0x8b)
        {
            return -1; // Not a gzip file
        }
        
        // Skip compression method, flags, mtime, xfl, os
        int deflate_start = 10;
        unsigned char flags = header[3];
        
        // Skip extra fields if present
        if (flags & 0x04) // FEXTRA
        {
            unsigned char extra_len[2];
            if (fread(extra_len, 1, 2, f) != 2)
                return -1;
            int len = extra_len[0] + (extra_len[1] << 8);
            if (fseeko(f, len, SEEK_CUR) != 0)
                return -1;
            deflate_start += 2 + len;
        }
        
        // Skip original filename if present
        if (flags & 0x08) // FNAME
        {
            int c;
            do {
                c = fgetc(f);
                if (c == EOF) return -1;
                deflate_start++;
            } while (c != 0);
        }
        
        // Skip comment if present  
        if (flags & 0x10) // FCOMMENT
        {
            int c;
            do {
                c = fgetc(f);
                if (c == EOF) return -1;
                deflate_start++;
            } while (c != 0);
        }
        
        // Skip header CRC if present
        if (flags & 0x02) // FHCRC
        {
            if (fseeko(f, 2, SEEK_CUR) != 0)
                return -1;
            deflate_start += 2;
        }
        
        return deflate_start;
    }

    std::string gz_path_;
    std::string idx_path_;
    sqlite3 *db_;
    bool is_open_;
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

std::pair<Reader::Buffer, size_t>
Reader::read_range_bytes(const std::string &gz_path, size_t start_bytes, size_t end_bytes) const
{
    return pImpl_->read_range_bytes(gz_path, start_bytes, end_bytes);
}

std::pair<Reader::Buffer, size_t> Reader::read_range_bytes(size_t start_bytes, size_t end_bytes) const
{
    return pImpl_->read_range_bytes(pImpl_->get_gz_path(), start_bytes, end_bytes);
}

std::pair<Reader::Buffer, size_t>
Reader::read_range_megabytes(const std::string &gz_path, double start_mb, double end_mb) const
{
    // Convert MB to bytes
    size_t start_bytes = static_cast<size_t>(start_mb * 1024 * 1024);
    size_t end_bytes = static_cast<size_t>(end_mb * 1024 * 1024);
    return pImpl_->read_range_bytes(gz_path, start_bytes, end_bytes);
}

std::pair<Reader::Buffer, size_t> Reader::read_range_megabytes(double start_mb, double end_mb) const
{
    // Convert MB to bytes
    size_t start_bytes = static_cast<size_t>(start_mb * 1024 * 1024);
    size_t end_bytes = static_cast<size_t>(end_mb * 1024 * 1024);
    return pImpl_->read_range_bytes(pImpl_->get_gz_path(), start_bytes, end_bytes);
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

    int dft_reader_read_range_bytes(dft_reader_handle_t reader,
                                    const char *gz_path,
                                    size_t start_bytes,
                                    size_t end_bytes,
                                    char **output,
                                    size_t *output_size)
    {

        if (!reader || !gz_path || !output || !output_size)
        {
            return -1;
        }

        try
        {
            auto *cpp_reader = static_cast<dft::reader::Reader *>(reader);
            auto result = cpp_reader->read_range_bytes(gz_path, start_bytes, end_bytes);

            // transfer ownership to C caller
            *output = result.first.release();
            *output_size = result.second;
            return 0;
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to read range bytes: {}", e.what());
            return -1;
        }
    }

    int dft_reader_read_range_megabytes(dft_reader_handle_t reader,
                                        const char *gz_path,
                                        double start_mb,
                                        double end_mb,
                                        char **output,
                                        size_t *output_size)
    {

        if (!reader || !gz_path || !output || !output_size)
        {
            return -1;
        }

        // convert MB to bytes
        size_t start_bytes = static_cast<size_t>(start_mb * 1024 * 1024);
        size_t end_bytes = static_cast<size_t>(end_mb * 1024 * 1024);

        return dft_reader_read_range_bytes(reader, gz_path, start_bytes, end_bytes, output, output_size);
    }

} // extern "C"
