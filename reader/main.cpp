#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>

#include <sqlite3.h>
#include <argparse/argparse.hpp>

#include "indexer.h"
#include "reader.h"
#include "platform_compat.h"

static uint64_t file_size_bytes(const char *path)
{
    FILE *fp = fopen(path, "rb");
    if (!fp)
        return UINT64_MAX; // Use max value to indicate error instead of -1
    fseeko(fp, 0, SEEK_END);
    auto sz = static_cast<uint64_t>(ftello(fp));
    fclose(fp);
    return sz;
}

static int index_exists_and_valid(const char *idx_path)
{
    FILE *f = fopen(idx_path, "rb");
    if (!f) return 0;
    fclose(f);

    sqlite3 *db;
    if (sqlite3_open(idx_path, &db) != SQLITE_OK)
    {
        return 0;
    }

    sqlite3_stmt *stmt;
    const char *sql = "SELECT name FROM sqlite_master WHERE type='table' AND "
                      "name IN ('chunks', 'metadata')";
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
    return table_count >= 2;
}

static double get_existing_chunk_size_mb(const char *idx_path)
{
    sqlite3 *db;
    if (sqlite3_open(idx_path, &db) != SQLITE_OK)
    {
        return -1; // Error
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

static bool confirm_rebuild()
{
    printf("Do you want to rebuild the index with the new chunk size? (y/n): ");
    fflush(stdout);
    
    char response;
    if (scanf(" %c", &response) == 1)
    {
        return (response == 'y' || response == 'Y');
    }
    return false;
}

int main(int argc, char **argv)
{
    argparse::ArgumentParser program("dft_reader", "1.0");
    program.add_description("DFTracer utility for reading and indexing gzipped files");

    program.add_argument("file")
        .help("Gzipped file to process")
        .required();
    
    program.add_argument("-s", "--start")
        .help("Start position in megabytes")
        .scan<'g', double>()
        .default_value(-1.0);
        
    program.add_argument("-e", "--end")
        .help("End position in megabytes")
        .scan<'g', double>()
        .default_value(-1.0);
        
    program.add_argument("-c", "--chunk-size")
        .help("Chunk size for indexing in megabytes (default: 32)")
        .scan<'g', double>()
        .default_value(32.0);
        
    program.add_argument("-f", "--force")
        .help("Force rebuild index even if chunk size differs")
        .flag();

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    std::string gz_path = program.get<std::string>("file");
    double start_mb = program.get<double>("--start");
    double end_mb = program.get<double>("--end");
    double chunk_size_mb = program.get<double>("--chunk-size");
    bool force_rebuild = program.get<bool>("--force");

    // Validate arguments
    if (chunk_size_mb <= 0) {
        std::cerr << "Error: Chunk size must be positive (greater than 0 and in MB)" << std::endl;
        return 1;
    }

    constexpr double epsilon = 1e-9;
    
    auto is_no_value = [](double val) {
        return std::abs(val - (-1.0)) < epsilon;
    };
    
    if (!is_no_value(start_mb) && start_mb < 0) {
        std::cerr << "Error: Start position must be non-negative" << std::endl;
        return 1;
    }

    if (!is_no_value(end_mb) && end_mb < 0) {
        std::cerr << "Error: End position must be non-negative" << std::endl;
        return 1;
    }

    // Check if file exists
    FILE* test_file = fopen(gz_path.c_str(), "rb");
    if (!test_file) {
        std::cerr << "Error: File '" << gz_path << "' does not exist or cannot be opened" << std::endl;
        return 1;
    }
    fclose(test_file);

    bool has_byte_range = (!is_no_value(start_mb) || !is_no_value(end_mb));

    if (has_byte_range && (is_no_value(start_mb) || is_no_value(end_mb)))
    {
        fprintf(stderr, "Error: Both --start and --end must be specified for MB range\n");
        return 1;
    }

    // construct index path
    size_t len = gz_path.length() + 5;
    auto *idx_path = static_cast<char *>(malloc(len));
    if (!idx_path)
    {
        perror("malloc");
        return 1;
    }
    snprintf(idx_path, len, "%s.idx", gz_path.c_str());

    bool need_rebuild = false;
    bool index_exists = index_exists_and_valid(idx_path);
    
    if (index_exists)
    {
        // check if chunk size differs from existing index
        double existing_chunk_size = get_existing_chunk_size_mb(idx_path);
        if (existing_chunk_size > 0)
        {
            double diff = fabs(existing_chunk_size - chunk_size_mb);
             // allow small floating point differences
            if (diff > 0.1)
            {
                if (!force_rebuild)
                {
                    printf("Warning: Existing index was created with %.1f MB chunks, but you specified %.1f MB chunks.\n", 
                           existing_chunk_size, chunk_size_mb);
                    
                    if (!confirm_rebuild())
                    {
                        printf("Using existing index with %.1f MB chunks.\n", existing_chunk_size);
                        // use existing chunk size
                        chunk_size_mb = existing_chunk_size;
                    }
                    else
                    {
                        need_rebuild = true;
                    }
                }
                else
                {
                    printf("Force rebuild: Existing index has %.1f MB chunks, rebuilding with %.1f MB chunks.\n", 
                           existing_chunk_size, chunk_size_mb);
                    need_rebuild = true;
                }
            }
        }
    }
    else
    {
        need_rebuild = true;
    }

    if (need_rebuild)
    {
        printf("Index not found or invalid, creating index for %s...\n", gz_path.c_str());

        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            fprintf(stderr, "Cannot create DB %s: %s\n", idx_path, sqlite3_errmsg(db));
            free(idx_path);
            return 1;
        }

        if (init_schema(db) != SQLITE_OK)
        {
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        // get file info
        uint64_t bytes = file_size_bytes(gz_path.c_str());
        if (bytes == UINT64_MAX)
        {
            fprintf(stderr, "Cannot stat %s\n", gz_path.c_str());
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        uint64_t mtime = 0;
        sqlite3_stmt *st;
        if (sqlite3_prepare_v2(db,
                               "INSERT INTO files(logical_name, byte_size, "
                               "mtime_unix, sha256_hex) "
                               "VALUES(?, ?, ?, '') "
                               "ON CONFLICT(logical_name) DO UPDATE SET "
                               "byte_size=excluded.byte_size "
                               "RETURNING id;",
                               -1,
                               &st,
                               NULL) != SQLITE_OK)
        {
            fprintf(stderr, "Prepare failed: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }
        sqlite3_bind_text(st, 1, gz_path.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(bytes));
        sqlite3_bind_int64(st, 3, static_cast<sqlite3_int64>(mtime));

        int rc = sqlite3_step(st);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE)
        {
            fprintf(stderr, "Insert failed: %s\n", sqlite3_errmsg(db));
            sqlite3_finalize(st);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }
        int file_id = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);

        /* build the index with configurable stride */
        auto stride = static_cast<uint64_t>(chunk_size_mb * 1024 * 1024);
        int ret = build_gzip_index(db, file_id, gz_path.c_str(), static_cast<long long>(stride));
        if (ret != 0)
        {
            fprintf(stderr, "Index build failed for %s (error code: %d)\n", gz_path.c_str(), ret);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        printf("Index built successfully for %s\n", gz_path.c_str());
        sqlite3_close(db);
    }

    // read operations
    if (has_byte_range)
    {
        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            fprintf(stderr, "Cannot open DB %s: %s\n", idx_path, sqlite3_errmsg(db));
            free(idx_path);
            return 1;
        }

        char *output;
        size_t output_size;

        printf("Reading MB range [%.2f, %.2f] from %s...\n", start_mb, end_mb, gz_path.c_str());

        int ret = read_data_range_megabytes(db, gz_path.c_str(), start_mb, end_mb, &output, &output_size);

        if (ret != 0)
        {
            fprintf(stderr, "Failed to read range from %s\n", gz_path.c_str());
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        fwrite(output, 1, output_size, stdout);

        free(output);
        sqlite3_close(db);
    }

    free(idx_path);
    return 0;
}
