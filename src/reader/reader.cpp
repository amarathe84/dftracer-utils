#include "reader.h"
#include "platform_compat.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <spdlog/spdlog.h>
#include <sqlite3.h>
#include <zlib.h>

struct dft_reader
{
    sqlite3 *db;
    char *gz_path;
    char *index_path;
    bool is_open;
};

// size of input buffer for decompression
#define CHUNK_SIZE 16384

typedef struct
{
    z_stream zs;
    FILE *file;
    unsigned char in[CHUNK_SIZE];
    int bits;
    size_t c_off;
} InflateState;

static int inflate_init(InflateState *I, FILE *f, size_t c_off, int bits)
{
    memset(I, 0, sizeof(*I));
    I->file = f;
    I->c_off = c_off;
    I->bits = bits;

    if (inflateInit2(&I->zs, 15 + 16) != Z_OK)
    {
        return -1;
    }

    // seek to compressed offset
    if (fseeko(f, static_cast<off_t>(c_off), SEEK_SET) != 0)
    {
        inflateEnd(&I->zs);
        return -1;
    }

    return 0;
}

static void inflate_cleanup(InflateState *I)
{
    inflateEnd(&I->zs);
}

static int inflate_read(InflateState *I, unsigned char *out, size_t out_size, size_t *bytes_read)
{
    I->zs.next_out = out;
    I->zs.avail_out = static_cast<uInt>(out_size);
    *bytes_read = 0;

    while (I->zs.avail_out > 0)
    {
        if (I->zs.avail_in == 0)
        {
            size_t n = fread(I->in, 1, sizeof(I->in), I->file);
            if (n == 0)
            {
                break; // EOF
            }
            I->zs.next_in = I->in;
            I->zs.avail_in = static_cast<uInt>(n);
        }

        int ret = inflate(&I->zs, Z_NO_FLUSH);
        if (ret == Z_STREAM_END)
        {
            break;
        }
        if (ret != Z_OK)
        {
            return -1;
        }
    }

    *bytes_read = out_size - I->zs.avail_out;
    return 0;
}

extern "C"
{

    dft_reader_t *dft_reader_create(const char *gz_path, const char *idx_path)
    {
        if (!gz_path || !idx_path)
        {
            spdlog::error("Both gz_path and idx_path cannot be null");
            return nullptr;
        }

        dft_reader_t *reader = static_cast<dft_reader_t *>(malloc(sizeof(dft_reader_t)));
        if (!reader)
        {
            spdlog::error("Failed to allocate memory for reader");
            return nullptr;
        }

        // Initialize the structure
        reader->db = nullptr;
        reader->is_open = false;
        reader->gz_path = nullptr;
        reader->index_path = nullptr;
        
        // Copy the gz path
        size_t gz_path_len = strlen(gz_path);
        reader->gz_path = static_cast<char *>(malloc(gz_path_len + 1));
        if (!reader->gz_path)
        {
            spdlog::error("Failed to allocate memory for gz path");
            free(reader);
            return nullptr;
        }
        strcpy(reader->gz_path, gz_path);
        
        // Copy the index path
        size_t idx_path_len = strlen(idx_path);
        reader->index_path = static_cast<char *>(malloc(idx_path_len + 1));
        if (!reader->index_path)
        {
            spdlog::error("Failed to allocate memory for index path");
            free(reader->gz_path);
            free(reader);
            return nullptr;
        }
        strcpy(reader->index_path, idx_path);

        // Open the database
        if (sqlite3_open(idx_path, &reader->db) != SQLITE_OK)
        {
            spdlog::error("Failed to open index database: {}", sqlite3_errmsg(reader->db));
            free(reader->gz_path);
            free(reader->index_path);
            free(reader);
            return nullptr;
        }

        reader->is_open = true;
        spdlog::debug("Successfully created DFT reader for gz: {} and index: {}", gz_path, idx_path);
        return reader;
    }

    void dft_reader_destroy(dft_reader_t *reader)
    {
        if (!reader)
        {
            return;
        }

        if (reader->is_open && reader->db)
        {
            sqlite3_close(reader->db);
        }

        if (reader->gz_path)
        {
            free(reader->gz_path);
        }

        if (reader->index_path)
        {
            free(reader->index_path);
        }

        free(reader);
        spdlog::debug("Successfully destroyed DFT reader");
    }

    int dft_reader_get_max_bytes(dft_reader_t *reader, size_t *max_bytes)
    {
        if (!reader || !max_bytes)
        {
            return -1;
        }

        if (!reader->is_open || !reader->db)
        {
            spdlog::error("Reader is not open");
            return -1;
        }

        sqlite3_stmt *stmt;
        const char *sql = "SELECT MAX(uncompressed_offset + uncompressed_size) FROM chunks";

        if (sqlite3_prepare_v2(reader->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        {
            spdlog::error("Failed to prepare max bytes query: {}", sqlite3_errmsg(reader->db));
            return -1;
        }

        int result = -1;
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            sqlite3_int64 max_val = sqlite3_column_int64(stmt, 0);
            if (max_val >= 0)
            {
                *max_bytes = static_cast<size_t>(max_val);
                result = 0;
                spdlog::debug("Maximum bytes available: {}", *max_bytes);
            }
            else
            {
                // No chunks found or empty result
                *max_bytes = 0;
                result = 0;
                spdlog::debug("No chunks found, maximum bytes: 0");
            }
        }
        else
        {
            spdlog::error("Failed to execute max bytes query: {}", sqlite3_errmsg(reader->db));
        }

        sqlite3_finalize(stmt);
        return result;
    }

    int dft_reader_read_range_bytes(
        dft_reader_t *reader, const char *gz_path, size_t start_bytes, size_t end_bytes, char **output, size_t *output_size)
    {
        if (!reader || !gz_path || !output || !output_size)
        {
            return -1;
        }

        if (!reader->is_open || !reader->db)
        {
            spdlog::error("Reader is not open");
            return -1;
        }

        if (start_bytes >= end_bytes)
        {
            return -1;
        }

        size_t target_size = end_bytes - start_bytes;

        spdlog::debug("Reading byte range [{}, {}] from {} ({}B to {}B)...",
                      start_bytes,
                      end_bytes,
                      gz_path,
                      start_bytes,
                      end_bytes);

        sqlite3_stmt *stmt;
        const char *sql = "SELECT chunk_idx, compressed_offset, "
                          "uncompressed_offset, uncompressed_size "
                          "FROM chunks "
                          "WHERE uncompressed_offset <= ? AND (uncompressed_offset "
                          "+ uncompressed_size) > ? "
                          "ORDER BY chunk_idx LIMIT 1";

        if (sqlite3_prepare_v2(reader->db, sql, -1, &stmt, NULL) != SQLITE_OK)
        {
            spdlog::error("Failed to prepare chunk query: {}", sqlite3_errmsg(reader->db));
            return -1;
        }

        sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(start_bytes));
        sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(start_bytes));

        if (sqlite3_step(stmt) != SQLITE_ROW)
        {
            spdlog::error("No chunk found containing byte offset {}", start_bytes);
            sqlite3_finalize(stmt);
            return -1;
        }

        uint64_t chunk_idx = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
        uint64_t chunk_c_off = static_cast<uint64_t>(sqlite3_column_int64(stmt, 1));
        uint64_t chunk_uc_off = static_cast<uint64_t>(sqlite3_column_int64(stmt, 2));
        uint64_t chunk_uc_size = static_cast<uint64_t>(sqlite3_column_int64(stmt, 3));

        sqlite3_finalize(stmt);

        spdlog::debug("Using chunk {}: uncompressed_offset={}, uncompressed_size={} "
                      "(compressed_offset={})",
                      chunk_idx,
                      chunk_uc_off,
                      chunk_uc_size,
                      chunk_c_off);

        FILE *f = fopen(gz_path, "rb");
        if (!f)
        {
            spdlog::error("Failed to open file: {}", gz_path);
            return -1;
        }

        // @note: since we can't start decompression from the middle without
        // dictionaries, we will start from the beginning but use the
        // chunk info for validation
        InflateState inflate_state;
        if (inflate_init(&inflate_state, f, 0, 0) != 0)
        {
            spdlog::error("Failed to reinitialize inflation");
            fclose(f);
            return -1;
        }

        size_t actual_start = start_bytes;

        // for small ranges (< 1KB), skip JSON line boundary search and read exact bytes
        bool use_exact_bytes = (target_size < 1024);

        // skip to find the start of a complete JSON line
        // at or after the start position (only for larger ranges)
        if (start_bytes > 0 && !use_exact_bytes)
        {
            unsigned char *search_buffer = static_cast<unsigned char *>(malloc(65536));
            if (!search_buffer)
            {
                inflate_cleanup(&inflate_state);
                fclose(f);
                return -1;
            }

            // read and scan for the first complete JSON line
            // at or after start_bytes
            size_t current_pos = 0;
            // we look at most 256 bytes before in case we need to find a complete line
            size_t search_start = start_bytes > 256 ? start_bytes - 256 : 0;

            spdlog::debug("Searching for complete JSON line starting around position {}", start_bytes);

            // skip to search start position
            while (current_pos < search_start)
            {
                size_t to_skip = search_start - current_pos > 65536 ? 65536 : search_start - current_pos;
                size_t skipped;
                if (inflate_read(&inflate_state, search_buffer, to_skip, &skipped) != 0)
                {
                    spdlog::error("Failed during initial skip: {}", gz_path);
                    free(search_buffer);
                    inflate_cleanup(&inflate_state);
                    fclose(f);
                    return -1;
                }
                if (skipped == 0)
                    break;
                current_pos += skipped;
            }

            // read and search for a complete line starting position
            size_t buffer_used = 0;
            while (current_pos < start_bytes + 512)  // limit search to 512 bytes past start
            {
                size_t to_read = 65536 - buffer_used;
                size_t bytes_read;
                if (inflate_read(&inflate_state, search_buffer + buffer_used, to_read, &bytes_read) != 0)
                {
                    break;
                }
                if (bytes_read == 0)
                    break;

                buffer_used += bytes_read;

                // finding complete JSON line boundaries (}\n{)
                for (size_t i = 0; i < buffer_used - 1; i++)
                {
                    size_t line_start_pos = current_pos + i;
                    if (line_start_pos >= start_bytes && (i == 0 || (search_buffer[i - 1] == '\n')) &&
                        search_buffer[i] == '{')
                    {
                        // found start of JSON line at or after our target
                        actual_start = line_start_pos;
                        spdlog::debug("Found JSON line start at position {}", actual_start);
                        goto found_start;
                    }
                }

                current_pos += bytes_read;
                // keep some overlap for boundary detection
                if (buffer_used > 32768)
                {
                    memmove(search_buffer, search_buffer + 32768, buffer_used - 32768);
                    buffer_used -= 32768;
                    current_pos -= 32768;
                }
            }

            // when no JSON line start found, use original start position
            actual_start = start_bytes;

        found_start:
            free(search_buffer);

            // restart decompression to get to the actual start position
            inflate_cleanup(&inflate_state);
            if (inflate_init(&inflate_state, f, 0, 0) != 0)
            {
                spdlog::error("Failed to reinitialize inflation");
                fclose(f);
                return -1;
            }

            // skip to actual start
            if (actual_start > 0)
            {
                unsigned char *skip_buffer = static_cast<unsigned char *>(malloc(65536));
                if (!skip_buffer)
                {
                    inflate_cleanup(&inflate_state);
                    fclose(f);
                    return -1;
                }

                auto remaining_skip = static_cast<long long>(actual_start);
                while (remaining_skip > 0)
                {
                    size_t to_skip = (remaining_skip > 65536) ? 65536U : static_cast<size_t>(remaining_skip);
                    size_t skipped;
                    if (inflate_read(&inflate_state, skip_buffer, to_skip, &skipped) != 0)
                    {
                        spdlog::error("Failed during final skip phase in {}", gz_path);
                        free(skip_buffer);
                        inflate_cleanup(&inflate_state);
                        fclose(f);
                        return -1;
                    }
                    if (skipped == 0)
                        break;
                    remaining_skip -= static_cast<long long>(skipped);
                }
                free(skip_buffer);
            }
        }

        // adjust target size based on actual start position
        size_t adjusted_target_size;
        if (use_exact_bytes)
        {
            // for exact byte mode, use the original target size
            adjusted_target_size = target_size;
        }
        else if (actual_start <= start_bytes)
        {
            // actual_start is at or before requested start, use original target size
            adjusted_target_size = target_size;
        }
        else
        {
            // actual_start is after requested start, adjust target size
            size_t offset_diff = actual_start - start_bytes;
            if (offset_diff >= target_size)
            {
                // read at least some data
                adjusted_target_size = 1024;
            }
            else
            {
                adjusted_target_size = target_size - offset_diff;
            }
        }

        // use streaming approach for large files instead of allocating everything at once
        // limit initial allocation to reasonable chunk size (32MB max)
        constexpr size_t MAX_CHUNK_SIZE = 32 * 1024 * 1024; // 32MB
        size_t initial_chunk = std::min(adjusted_target_size + 4096, MAX_CHUNK_SIZE);

        // Start with smaller allocation and grow as needed
        size_t buffer_capacity = static_cast<size_t>(initial_chunk);
        *output = static_cast<char *>(malloc(buffer_capacity + 1));
        if (!*output)
        {
            inflate_cleanup(&inflate_state);
            fclose(f);
            return -1;
        }

        // @note: read the requested range using streaming approach
        // this reading is line boundary-aware for larger ranges, meaning
        // we will look for complete JSON lines within the requested range
        // for small ranges, we read exact bytes
        size_t total_read = 0;
        bool boundary_search_mode = false;
        spdlog::debug("Reading {} bytes starting from position {} (adjusted from {}) - exact mode: {}",
                      adjusted_target_size,
                      actual_start,
                      start_bytes,
                      use_exact_bytes);

        while (true)
        {
            // check if we've reached our target and should stop
            if (!boundary_search_mode && total_read >= adjusted_target_size)
            {
                if (use_exact_bytes)
                {
                    // in exact mode, stop immediately when we reach the target
                    break;
                }
                else
                {
                    boundary_search_mode = true;
                    spdlog::debug("Reached target size {}, entering boundary search mode", adjusted_target_size);
                }
            }

            // safety valve for boundary search mode - limit to 4KB past target
            if (boundary_search_mode && total_read > adjusted_target_size + 4096)
            {
                spdlog::debug("Boundary search exceeded 4KB past target, stopping");
                break;
            }

            // ensure we have enough buffer space
            if (total_read + 65536 > buffer_capacity)
            {
                // grow buffer - double it or add 32MB, whichever is smaller
                size_t new_capacity =
                    std::min(buffer_capacity * 2, buffer_capacity + static_cast<size_t>(MAX_CHUNK_SIZE));
                char *new_buffer = static_cast<char *>(realloc(*output, new_capacity + 1));
                if (!new_buffer)
                {
                    spdlog::error("Failed to grow buffer from {} to {} bytes", buffer_capacity, new_capacity);
                    free(*output);
                    *output = NULL;
                    inflate_cleanup(&inflate_state);
                    fclose(f);
                    return -1;
                }
                *output = new_buffer;
                buffer_capacity = new_capacity;
                spdlog::debug("Grew buffer to {} bytes", buffer_capacity);
            }

            // read in manageable chunks - smaller chunks in boundary search mode
            size_t chunk_size;
            if (use_exact_bytes)
            {
                // In exact mode, read only what we need
                chunk_size = std::min(adjusted_target_size - total_read, static_cast<size_t>(buffer_capacity - total_read));
            }
            else if (boundary_search_mode)
            {
                chunk_size = std::min(static_cast<size_t>(256), static_cast<size_t>(buffer_capacity - total_read));
            }
            else
            {
                chunk_size = std::min(static_cast<size_t>(65536), static_cast<size_t>(buffer_capacity - total_read));
            }

            size_t bytes_read;
            if (inflate_read(
                    &inflate_state, reinterpret_cast<unsigned char *>(*output + total_read), chunk_size, &bytes_read) !=
                0)
            {
                spdlog::error("Failed during read phase at position {}", total_read);
                free(*output);
                *output = NULL;
                inflate_cleanup(&inflate_state);
                fclose(f);
                return -1;
            }

            if (bytes_read == 0)
            {
                spdlog::debug("Reached EOF at {} bytes", total_read);
                break; // EOF
            }

            total_read += bytes_read;

            // in boundary search mode (not exact mode), look for complete JSON line boundaries
            if (boundary_search_mode && !use_exact_bytes)
            {
                // scan the newly read data for }\n boundary
                for (size_t i = 1; i < bytes_read; i++)
                {
                    size_t buffer_pos = total_read - bytes_read + i;
                    if (buffer_pos > 0 && (*output)[buffer_pos - 1] == '}' && (*output)[buffer_pos] == '\n')
                    {
                        // found a boundary - truncate here and stop
                        total_read = buffer_pos + 1;
                        spdlog::debug("Found JSON boundary at position {}, truncating", total_read);
                        goto done_reading;
                    }
                }

                // check if the last character we read completes a boundary
                if (total_read >= 2 && (*output)[total_read - 2] == '}' && (*output)[total_read - 1] == '\n')
                {
                    spdlog::debug("Found JSON boundary at end of read, position {}", total_read);
                    break;
                }
            }
        }

    done_reading:

        (*output)[total_read] = '\0';
        *output_size = total_read;

        spdlog::debug("Successfully read {} bytes (requested {}, adjusted target {}, "
                      "rounded to complete JSON lines)",
                      total_read,
                      target_size,
                      adjusted_target_size);

        inflate_cleanup(&inflate_state);
        fclose(f);
        return 0;
    }
} // extern "C"


