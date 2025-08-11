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

struct dft_indexer
{
    char *gz_path;
    char *idx_path;
    double chunk_size_mb;
    bool force_rebuild;
    sqlite3 *db;
    bool db_opened;
};

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

extern "C"
{
    static std::string calculate_file_sha256(const char *file_path)
    {
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open())
        {
            spdlog::error("Cannot open file for SHA256 calculation: {}", file_path);
            return "";
        }

        std::vector<unsigned char> buffer(8192);
        picosha2::hash256_one_by_one hasher;
        hasher.init();

        while (file.read(reinterpret_cast<char*>(buffer.data()), buffer.size()) || file.gcount() > 0)
        {
            hasher.process(buffer.begin(), buffer.begin() + file.gcount());
        }

        hasher.finish();
        std::string hex_str;
        picosha2::get_hash_hex_string(hasher, hex_str);
        return hex_str;
    }

    static time_t get_file_mtime(const char *file_path)
    {
        struct stat st;
        if (stat(file_path, &st) == 0)
        {
            return st.st_mtime;
        }
        return 0;
    }

    static int index_exists_and_valid(const char *idx_path)
    {
        FILE *f = fopen(idx_path, "rb");
        if (!f)
            return 0;
        fclose(f);

        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            return 0;
        }

        sqlite3_stmt *stmt;
        const char *sql = "SELECT name FROM sqlite_master WHERE type='table' AND "
                          "name IN ('chunks', 'metadata', 'files')";
        int table_count = 0;

        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK)
        {
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                table_count++;
            }
            sqlite3_finalize(stmt);
        }

        sqlite3_close(db);
        return table_count >= 3;
    }

    static double get_existing_chunk_size_mb(const char *idx_path)
    {
        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            return -1;
        }

        sqlite3_stmt *stmt;
        const char *sql = "SELECT chunk_size FROM metadata LIMIT 1";
        double chunk_size_mb = -1;

        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK)
        {
            if (sqlite3_step(stmt) == SQLITE_ROW)
            {
                auto chunk_size_bytes = static_cast<uint64_t>(sqlite3_column_int64(stmt, 0));
                chunk_size_mb = static_cast<double>(chunk_size_bytes) / (1024.0 * 1024.0);
            }
            sqlite3_finalize(stmt);
        }

        sqlite3_close(db);
        return chunk_size_mb;
    }

    static bool get_stored_file_info(const char *idx_path, const char *gz_path, std::string &stored_sha256, time_t &stored_mtime)
    {
        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            return false;
        }

        sqlite3_stmt *stmt;
        const char *sql = "SELECT sha256_hex, mtime_unix FROM files WHERE logical_name = ? LIMIT 1";
        bool found = false;

        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK)
        {
            sqlite3_bind_text(stmt, 1, gz_path, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW)
            {
                const char *sha256_text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                if (sha256_text)
                {
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

    static uint64_t file_size_bytes(const char *path)
    {
        FILE *fp = fopen(path, "rb");
        if (!fp)
            return UINT64_MAX;
        fseeko(fp, 0, SEEK_END);
        auto sz = static_cast<uint64_t>(ftello(fp));
        fclose(fp);
        return sz;
    }

    dft_indexer_t* dft_indexer_create(const char* gz_path, const char* idx_path, double chunk_size_mb, bool force_rebuild)
    {
        if (!gz_path || !idx_path || chunk_size_mb <= 0)
        {
            spdlog::error("Invalid parameters for indexer creation");
            return nullptr;
        }

        dft_indexer_t* indexer = static_cast<dft_indexer_t*>(malloc(sizeof(dft_indexer_t)));
        if (!indexer)
        {
            spdlog::error("Failed to allocate memory for indexer");
            return nullptr;
        }

        indexer->db = nullptr;
        indexer->db_opened = false;
        indexer->chunk_size_mb = chunk_size_mb;
        indexer->force_rebuild = force_rebuild;

        size_t gz_len = strlen(gz_path);
        indexer->gz_path = static_cast<char*>(malloc(gz_len + 1));
        if (!indexer->gz_path)
        {
            spdlog::error("Failed to allocate memory for gz path");
            free(indexer);
            return nullptr;
        }
        strcpy(indexer->gz_path, gz_path);

        // Copy index path
        size_t idx_len = strlen(idx_path);
        indexer->idx_path = static_cast<char*>(malloc(idx_len + 1));
        if (!indexer->idx_path)
        {
            spdlog::error("Failed to allocate memory for index path");
            free(indexer->gz_path);
            free(indexer);
            return nullptr;
        }
        strcpy(indexer->idx_path, idx_path);

        spdlog::debug("Created DFT indexer for gz: {} and index: {}", gz_path, idx_path);
        return indexer;
    }

    int dft_indexer_need_rebuild(dft_indexer_t* indexer)
    {
        if (!indexer)
        {
            return -1;
        }

        // Check if index exists and is valid
        if (!index_exists_and_valid(indexer->idx_path))
        {
            spdlog::debug("Index rebuild needed: index does not exist or is invalid");
            return 1;
        }

        // If force rebuild is set, always rebuild
        if (indexer->force_rebuild)
        {
            spdlog::debug("Index rebuild needed: force rebuild is enabled");
            return 1;
        }

        // Check if chunk size differs
        double existing_chunk_size = get_existing_chunk_size_mb(indexer->idx_path);
        if (existing_chunk_size > 0)
        {
            double diff = fabs(existing_chunk_size - indexer->chunk_size_mb);
            if (diff > 0.1) // Allow small floating point differences
            {
                spdlog::debug("Index rebuild needed: chunk size differs ({:.1f} MB vs {:.1f} MB)", 
                             existing_chunk_size, indexer->chunk_size_mb);
                return 1;
            }
        }

        // Check if file content has changed using SHA256
        std::string stored_sha256;
        time_t stored_mtime;
        if (get_stored_file_info(indexer->idx_path, indexer->gz_path, stored_sha256, stored_mtime))
        {
            // // First, quick check using modification time as optimization
            // time_t current_mtime = get_file_mtime(indexer->gz_path);
            // if (current_mtime != stored_mtime && current_mtime > 0 && stored_mtime > 0)
            // {
            //     spdlog::debug("Index rebuild needed: file modification time changed");
            //     return 1;
            // }

            // If we have a stored SHA256, calculate current SHA256 and compare
            if (!stored_sha256.empty())
            {
                std::string current_sha256 = calculate_file_sha256(indexer->gz_path);
                if (current_sha256.empty())
                {
                    spdlog::error("Failed to calculate SHA256 for {}", indexer->gz_path);
                    return -1;
                }

                if (current_sha256 != stored_sha256)
                {
                    spdlog::debug("Index rebuild needed: file SHA256 changed ({} vs {})", 
                                 current_sha256.substr(0, 16) + "...", 
                                 stored_sha256.substr(0, 16) + "...");
                    return 1;
                }
            }
            else
            {
                // No stored SHA256, this might be an old index format
                spdlog::debug("Index rebuild needed: no SHA256 stored in index (old format)");
                return 1;
            }
        }
        else
        {
            // Could not get stored file info, assume rebuild needed
            spdlog::debug("Index rebuild needed: could not retrieve stored file information");
            return 1;
        }

        spdlog::debug("Index rebuild not needed: file content unchanged");
        return 0;
    }

    // Initialize database schema
    static int init_schema(sqlite3 *db)
    {
        char *errmsg = NULL;
        int rc = sqlite3_exec(db, SQL_SCHEMA, NULL, NULL, &errmsg);
        if (rc != SQLITE_OK)
        {
            spdlog::error("Failed to initialize database schema: {}", errmsg);
            sqlite3_free(errmsg);
        }
        else
        {
            spdlog::debug("Schema init succeeded");
        }
        return rc;
    }

    // Helper function for database preparation
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
    inflate_process_chunk(InflateState *I, unsigned char *out, size_t out_size, size_t *bytes_out, size_t *c_off)
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

            size_t c_pos_before = static_cast<size_t>(ftello(I->file)) - I->zs.avail_in;
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

    // Internal function to build index
    static int build_index_internal(sqlite3 *db, int file_id, const char *gz_path, size_t chunk_size)
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
        sqlite3_bind_int64(st_meta, 2, static_cast<int64_t>(chunk_size));
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

        size_t chunk_idx = 0;
        size_t chunk_start_uc_off = 0;
        size_t chunk_start_c_off = 0;
        size_t current_uc_off = 0;
        size_t current_events = 0;
        unsigned char buffer[65536];
        int chunk_has_complete_event = 0;

        spdlog::info("Building chunk index with chunk_size={} bytes", chunk_size);

        while (1)
        {
            size_t bytes_read;
            size_t c_off;

            if (inflate_process_chunk(&inflate_state, buffer, sizeof(buffer), &bytes_read, &c_off) != 0)
            {
                break;
            }

            if (bytes_read == 0)
            {
                if (current_uc_off > chunk_start_uc_off)
                {
                    size_t chunk_uc_size = current_uc_off - chunk_start_uc_off;
                    size_t chunk_c_size = c_off - chunk_start_c_off;

                    sqlite3_bind_int(st_chunk, 1, file_id);
                    sqlite3_bind_int64(st_chunk, 2, static_cast<int64_t>(chunk_idx));
                    sqlite3_bind_int64(st_chunk, 3, static_cast<int64_t>(chunk_start_c_off));
                    sqlite3_bind_int64(st_chunk, 4, static_cast<int64_t>(chunk_c_size));
                    sqlite3_bind_int64(st_chunk, 5, static_cast<int64_t>(chunk_start_uc_off));
                    sqlite3_bind_int64(st_chunk, 6, static_cast<int64_t>(chunk_uc_size));
                    sqlite3_bind_int64(st_chunk, 7, static_cast<int64_t>(current_events));

                    if (sqlite3_step(st_chunk) != SQLITE_DONE)
                    {
                        spdlog::error("Error inserting final chunk");
                        break;
                    }
                    sqlite3_reset(st_chunk);

                    spdlog::debug("Final chunk {}: uc_off={}-{} ({} bytes), "
                                  "events={}\n",
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

                if (sqlite3_step(st_chunk) != SQLITE_DONE)
                {
                    spdlog::error("Error inserting chunk {}", chunk_idx);
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

    int dft_indexer_build(dft_indexer_t* indexer)
    {
        if (!indexer)
        {
            return -1;
        }

        // Check if rebuild is needed
        int need_rebuild = dft_indexer_need_rebuild(indexer);
        if (need_rebuild == -1)
        {
            return -1;
        }
        
        if (need_rebuild == 0)
        {
            spdlog::info("Index is up to date, skipping rebuild");
            return 0;
        }

        spdlog::info("Building index for {} with {:.1f} MB chunks...", indexer->gz_path, indexer->chunk_size_mb);

        // Open database
        if (sqlite3_open(indexer->idx_path, &indexer->db) != SQLITE_OK)
        {
            spdlog::error("Cannot create/open database {}: {}", indexer->idx_path, sqlite3_errmsg(indexer->db));
            return -1;
        }
        indexer->db_opened = true;

        // Initialize schema
        if (init_schema(indexer->db) != SQLITE_OK)
        {
            spdlog::error("Failed to initialize database schema");
            return -1;
        }

        // Get file info
        uint64_t bytes = file_size_bytes(indexer->gz_path);
        if (bytes == UINT64_MAX)
        {
            spdlog::error("Cannot stat {}", indexer->gz_path);
            return -1;
        }

        // Calculate SHA256 and get modification time
        std::string file_sha256 = calculate_file_sha256(indexer->gz_path);
        if (file_sha256.empty())
        {
            spdlog::error("Failed to calculate SHA256 for {}", indexer->gz_path);
            return -1;
        }
        
        time_t file_mtime = get_file_mtime(indexer->gz_path);

        spdlog::debug("File info: size={} bytes, mtime={}, sha256={}...", 
                     bytes, file_mtime, file_sha256.substr(0, 16));

        // Insert/update file record
        sqlite3_stmt *st;
        if (sqlite3_prepare_v2(indexer->db,
                               "INSERT INTO files(logical_name, byte_size, mtime_unix, sha256_hex) "
                               "VALUES(?, ?, ?, ?) "
                               "ON CONFLICT(logical_name) DO UPDATE SET "
                               "byte_size=excluded.byte_size, "
                               "mtime_unix=excluded.mtime_unix, "
                               "sha256_hex=excluded.sha256_hex "
                               "RETURNING id;",
                               -1, &st, NULL) != SQLITE_OK)
        {
            spdlog::error("Prepare failed: {}", sqlite3_errmsg(indexer->db));
            return -1;
        }

        sqlite3_bind_text(st, 1, indexer->gz_path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(bytes));
        sqlite3_bind_int64(st, 3, static_cast<sqlite3_int64>(file_mtime));
        sqlite3_bind_text(st, 4, file_sha256.c_str(), -1, SQLITE_TRANSIENT);

        int rc = sqlite3_step(st);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE)
        {
            spdlog::error("Insert failed: {}", sqlite3_errmsg(indexer->db));
            sqlite3_finalize(st);
            return -1;
        }

        int db_file_id = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);

        // Build the index
        auto stride = static_cast<size_t>(indexer->chunk_size_mb * 1024 * 1024);
        spdlog::debug("Building index with stride: {} bytes ({:.1f} MB)", stride, indexer->chunk_size_mb);
        
        int ret = build_index_internal(indexer->db, db_file_id, indexer->gz_path, stride);
        if (ret != 0)
        {
            spdlog::error("Index build failed for {} (error code: {})", indexer->gz_path, ret);
            return -1;
        }

        spdlog::info("Index built successfully for {}", indexer->gz_path);
        return 0;
    }

    void dft_indexer_destroy(dft_indexer_t* indexer)
    {
        if (!indexer)
        {
            return;
        }

        if (indexer->db_opened && indexer->db)
        {
            sqlite3_close(indexer->db);
        }

        if (indexer->gz_path)
        {
            free(indexer->gz_path);
        }

        if (indexer->idx_path)
        {
            free(indexer->idx_path);
        }

        free(indexer);
        spdlog::debug("Successfully destroyed DFT indexer");
    }

} // extern "C"
