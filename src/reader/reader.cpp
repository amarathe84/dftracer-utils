#include "reader.h"
#include "platform_compat.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <zlib.h>
#include <spdlog/spdlog.h>

// size of input buffer for decompression
#define CHUNK_SIZE 16384

typedef struct
{
    z_stream zs;
    FILE *file;
    unsigned char in[CHUNK_SIZE];
    int bits;
    uint64_t c_off;
} InflateState;

static int inflate_init(InflateState *I, FILE *f, uint64_t c_off, int bits)
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

extern "C" {

int read_data_range_bytes(
    sqlite3 *db, const char *gz_path, uint64_t start_bytes, uint64_t end_bytes, char **output, size_t *output_size)
{
    if (!db || !gz_path || !output || !output_size)
    {
        return -1;
    }

    uint64_t target_size = end_bytes - start_bytes;

    if (target_size <= 0)
    {
        return -1;
    }

    spdlog::info("Reading byte range [{}, {}] from {} ({}B to {}B)...",
                 start_bytes, end_bytes, gz_path, start_bytes, end_bytes);

    sqlite3_stmt *stmt;
    const char *sql = "SELECT chunk_idx, compressed_offset, "
                      "uncompressed_offset, uncompressed_size "
                      "FROM chunks "
                      "WHERE uncompressed_offset <= ? AND (uncompressed_offset "
                      "+ uncompressed_size) > ? "
                      "ORDER BY chunk_idx LIMIT 1";

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK)
    {
        spdlog::error("Failed to prepare chunk query: {}", sqlite3_errmsg(db));
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
                  chunk_idx, chunk_uc_off, chunk_uc_size, chunk_c_off);

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

    uint64_t actual_start = start_bytes;

    // skip to find the start of a complete JSON line
    // at or after the start position
    if (start_bytes > 0)
    {
        unsigned char *search_buffer = static_cast<unsigned char*>(malloc(65536));
        if (!search_buffer)
        {
            inflate_cleanup(&inflate_state);
            fclose(f);
            return -1;
        }

        // read and scan for the first complete JSON line
        // at or after start_bytes
        uint64_t current_pos = 0;
        // we look 1024 bytes before in case we need to find a complete line
        uint64_t search_start = start_bytes > 1024 ? start_bytes - 1024 : 0;

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
        while (current_pos < start_bytes + 4096)
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
                uint64_t line_start_pos = current_pos + i;
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

        // Skip to actual start
        if (actual_start > 0)
        {
            unsigned char *skip_buffer = static_cast<unsigned char*>(malloc(65536));
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

    // allocate output buffer with extra space to ensure
    // we can find a complete line boundary
    // then, we adjust target size based on actual start position
    uint64_t adjusted_target_size = target_size - (actual_start - start_bytes);
    if (adjusted_target_size <= 0)
    {
        // read at least some data
        adjusted_target_size = 1024;
    }

    // extra space for line boundary detection
    uint64_t buffer_size = adjusted_target_size + 4096;
    *output = static_cast<char*>(malloc(buffer_size + 1));
    if (!*output)
    {
        inflate_cleanup(&inflate_state);
        fclose(f);
        return -1;
    }

    // @note: read the requested range
    // this reading is line boundary-aware, meaning
    // we will look for complete JSON lines within the requested range
    size_t total_read = 0;
    spdlog::debug("Reading {} bytes starting from position {} (adjusted from {})",
                  adjusted_target_size, actual_start, start_bytes);
    while (total_read < buffer_size)
    {
        size_t to_read = buffer_size - total_read;
        size_t bytes_read;

        if (inflate_read(&inflate_state, reinterpret_cast<unsigned char *>(*output + total_read), to_read, &bytes_read) != 0)
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
            break; // EOF
        }
        total_read += bytes_read;

        // if we've read at least the target size,
        // look for a complete line boundary
        if (total_read >= adjusted_target_size)
        {
            // find the last complete line that ends with }\n after the target
            // position
            size_t last_complete_line = total_read;
            for (size_t i = adjusted_target_size; i < total_read - 1; i++)
            {
                if ((*output)[i] == '}' && (*output)[i + 1] == '\n')
                {
                    // found a complete JSON line boundary, include the }\n
                    last_complete_line = i + 2;
                    spdlog::debug("Found complete JSON line boundary at position {}", last_complete_line);
                    break;
                }
            }

            // found a complete line boundary
            if (last_complete_line < total_read)
            {
                total_read = last_complete_line;
                break;
            }

            // reached EOF without finding a complete boundary
            if (bytes_read == 0)
            {
                break;
            }
        }
    }

    (*output)[total_read] = '\0';
    *output_size = total_read;

    spdlog::debug("Successfully read {} bytes (requested {}, adjusted target {}, "
                 "rounded to complete JSON lines)",
                 total_read, target_size, adjusted_target_size);

    inflate_cleanup(&inflate_state);
    fclose(f);
    return 0;
}

} // extern "C"
