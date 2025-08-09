#include "indexer.h"
#include "platform_compat.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <zlib.h>
#include <spdlog/spdlog.h>

const char *SQL_SCHEMA = "CREATE TABLE IF NOT EXISTS files ("
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

int init_schema(sqlite3 *db)
{
    char *errmsg = NULL;
    int rc = sqlite3_exec(db, SQL_SCHEMA, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Schema init failed: %s\n", errmsg);
        sqlite3_free(errmsg);
    }
    else
    {
        fprintf(stderr, "Schema init succeeded\n");
    }
    return rc;
}

static int db_prep(sqlite3 *db, sqlite3_stmt **pStmt, const char *sql)
{
    return sqlite3_prepare_v2(db, sql, -1, pStmt, NULL);
}

typedef struct
{
    z_stream zs;
    FILE *file;
    unsigned char in[16384];
} InflateState;

static int inflate_init_simple(InflateState *I, FILE *f)
{
    memset(I, 0, sizeof(*I));
    I->file = f;

    if (inflateInit2(&I->zs, 31) != Z_OK)
    {
        return -1;
    }
    return 0;
}

static void inflate_cleanup_simple(InflateState *I)
{
    inflateEnd(&I->zs);
}

static int
inflate_process_chunk(InflateState *I, unsigned char *out, size_t out_size, size_t *bytes_out, long long *c_off)
{
    I->zs.next_out = out;
                I->zs.avail_out = static_cast<uInt>(out_size);
    *bytes_out = 0;

    while (I->zs.avail_out > 0)
    {
        if (I->zs.avail_in == 0)
        {
            size_t n = fread(I->in, 1, sizeof(I->in), I->file);
            if (n == 0)
            {
                break;
            }
            I->zs.next_in = I->in;
            I->zs.avail_in = static_cast<uInt>(n);
        }

        long long c_pos_before = ftello(I->file) - I->zs.avail_in;
        int ret = inflate(&I->zs, Z_NO_FLUSH);

        if (ret == Z_STREAM_END)
        {
            break;
        }
        if (ret != Z_OK)
        {
            return -1;
        }

        *c_off = c_pos_before;
    }

    *bytes_out = out_size - I->zs.avail_out;
    return 0;
}


extern "C" {

int build_gzip_index(sqlite3 *db, int file_id, const char *gz_path, long long chunk_size)
{
    FILE *fp = fopen(gz_path, "rb");
    if (!fp)
        return -1;

    sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);

    // Clean up existing data for this file before rebuilding
    sqlite3_stmt *st_cleanup_chunks = NULL;
    if (db_prep(db, &st_cleanup_chunks, "DELETE FROM chunks WHERE file_id = ?;"))
    {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }
    sqlite3_bind_int(st_cleanup_chunks, 1, file_id);
    sqlite3_step(st_cleanup_chunks);
    sqlite3_finalize(st_cleanup_chunks);

    sqlite3_stmt *st_cleanup_metadata = NULL;
    if (db_prep(db, &st_cleanup_metadata, "DELETE FROM metadata WHERE file_id = ?;"))
    {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }
    sqlite3_bind_int(st_cleanup_metadata, 1, file_id);
    sqlite3_step(st_cleanup_metadata);
    sqlite3_finalize(st_cleanup_metadata);

    sqlite3_stmt *st_meta = NULL;
    if (db_prep(db,
                &st_meta,
                "INSERT INTO metadata(file_id, chunk_size) "
                "VALUES(?, ?);"))
    {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -2;
    }

    sqlite3_bind_int(st_meta, 1, file_id);
    sqlite3_bind_int64(st_meta, 2, chunk_size);
    sqlite3_step(st_meta);
    sqlite3_finalize(st_meta);

    sqlite3_stmt *st_chunk = NULL;
    if (db_prep(db,
                &st_chunk,
                "INSERT INTO chunks(file_id, chunk_idx, compressed_offset, "
                "compressed_size, "
                "uncompressed_offset, uncompressed_size, num_events) "
                "VALUES(?, ?, ?, ?, ?, ?, ?);"))
    {
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -3;
    }

    InflateState inflate_state;
    if (inflate_init_simple(&inflate_state, fp) != 0)
    {
        sqlite3_finalize(st_chunk);
        fclose(fp);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return -4;
    }

    long long chunk_idx = 0;
    long long chunk_start_uc_off = 0;
    long long chunk_start_c_off = 0;
    long long current_uc_off = 0;
    long long current_events = 0;
    unsigned char buffer[65536];
    int chunk_has_complete_event = 0;

    spdlog::info("Building chunk index with chunk_size={} bytes", chunk_size);

    while (1)
    {
        size_t bytes_read;
        long long c_off;

        if (inflate_process_chunk(&inflate_state, buffer, sizeof(buffer), &bytes_read, &c_off) != 0)
        {
            break;
        }

        if (bytes_read == 0)
        {
            if (current_uc_off > chunk_start_uc_off)
            {
                long long chunk_uc_size = current_uc_off - chunk_start_uc_off;
                long long chunk_c_size = c_off - chunk_start_c_off;

                sqlite3_bind_int(st_chunk, 1, file_id);
                sqlite3_bind_int64(st_chunk, 2, chunk_idx);
                sqlite3_bind_int64(st_chunk, 3, chunk_start_c_off);
                sqlite3_bind_int64(st_chunk, 4, chunk_c_size);
                sqlite3_bind_int64(st_chunk, 5, chunk_start_uc_off);
                sqlite3_bind_int64(st_chunk, 6, chunk_uc_size);
                sqlite3_bind_int64(st_chunk, 7, current_events);

                if (sqlite3_step(st_chunk) != SQLITE_DONE)
                {
                    printf("Error inserting final chunk\n");
                    break;
                }
                sqlite3_reset(st_chunk);

                printf("Final chunk %lld: uc_off=%lld-%lld (%lld bytes), "
                       "events=%lld\n",
                       chunk_idx,
                       chunk_start_uc_off,
                       current_uc_off,
                       chunk_uc_size,
                       current_events);
            }
            break;
        }

        size_t last_newline_pos = SIZE_MAX;
        for (size_t i = 0; i < bytes_read; i++)
        {
            if (buffer[i] == '\n')
            {
                current_events++;
                chunk_has_complete_event = 1;
                last_newline_pos = i;
            }
        }

        current_uc_off += bytes_read;

        if ((current_uc_off - chunk_start_uc_off) >= chunk_size && chunk_has_complete_event &&
            last_newline_pos != SIZE_MAX)
        {
            // end current chunk at the last complete line boundary
            auto chunk_end_uc_off = static_cast<long long>(current_uc_off - static_cast<long long>(bytes_read) + static_cast<long long>(last_newline_pos) + 1);
            long long chunk_uc_size = chunk_end_uc_off - chunk_start_uc_off;
            long long chunk_c_size = c_off - chunk_start_c_off;

            long long chunk_events = current_events;

            sqlite3_bind_int(st_chunk, 1, file_id);
            sqlite3_bind_int64(st_chunk, 2, chunk_idx);
            sqlite3_bind_int64(st_chunk, 3, chunk_start_c_off);
            sqlite3_bind_int64(st_chunk, 4, chunk_c_size);
            sqlite3_bind_int64(st_chunk, 5, chunk_start_uc_off);
            sqlite3_bind_int64(st_chunk, 6, chunk_uc_size);
            sqlite3_bind_int64(st_chunk, 7, chunk_events);

            if (sqlite3_step(st_chunk) != SQLITE_DONE)
            {
                printf("Error inserting chunk %lld\n", chunk_idx);
                break;
            }
            sqlite3_reset(st_chunk);

            spdlog::debug("Chunk {}: uc_off={}-%lld (%lld bytes), events={} "
                   "(ended at line boundary)\n",
                   chunk_idx,
                   chunk_start_uc_off,
                   chunk_end_uc_off,
                   chunk_uc_size,
                   chunk_events);

            // start new chunk after the last complete line
            chunk_idx++;
            chunk_start_uc_off = chunk_end_uc_off;
            chunk_start_c_off = c_off;
            current_events = 0;
            chunk_has_complete_event = 0;

            // count remaining lines in buffer for next chunk
            for (size_t i = last_newline_pos + 1; i < bytes_read; i++)
            {
                if (buffer[i] == '\n')
                {
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

} // extern "C"
