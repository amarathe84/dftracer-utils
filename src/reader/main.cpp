#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <argparse/argparse.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <sqlite3.h>

#include "indexer.h"
#include "platform_compat.h"
#include "reader.h"
#include "utils.h"

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

static bool confirm_rebuild()
{
    spdlog::info("Do you want to rebuild the index with the new chunk size? (y/n): ");
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

    program.add_argument("file").help("Gzipped file to process").required();

    program.add_argument("-s", "--start").help("Start position in bytes").scan<'d', size_t>().default_value(0);

    program.add_argument("-e", "--end").help("End position in bytes").scan<'d', size_t>().default_value(0);

    program.add_argument("-c", "--chunk-size")
        .help("Chunk size for indexing in megabytes (default: 32)")
        .scan<'g', double>()
        .default_value(32.0);

    program.add_argument("-f", "--force").help("Force rebuild index even if chunk size differs").flag();

    program.add_argument("--log-level")
        .help("Set logging level (trace, debug, info, warn, error, critical, off)")
        .default_value(std::string("info"));

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception &err)
    {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    std::string gz_path = program.get<std::string>("file");
    size_t start_bytes = program.get<size_t>("--start");
    size_t end_bytes = program.get<size_t>("--end");
    double chunk_size_mb = program.get<double>("--chunk-size");
    bool force_rebuild = program.get<bool>("--force");
    std::string log_level_str = program.get<std::string>("--log-level");

    // stderr-based logger to ensure logs don't interfere with data output
    auto logger = spdlog::stderr_color_mt("stderr");
    spdlog::set_default_logger(logger);
    dft::utils::set_log_level(log_level_str); // Use the new utility function

    spdlog::info("Log level set to: {}", log_level_str);

    spdlog::debug("Processing file: {}", gz_path);
    spdlog::debug("Start position: {} B", start_bytes);
    spdlog::debug("End position: {} B", end_bytes);
    spdlog::debug("Chunk size: {} MB", chunk_size_mb);
    spdlog::debug("Force rebuild: {}", force_rebuild);

    // Validate arguments
    if (chunk_size_mb <= 0)
    {
        std::cerr << "Error: Chunk size must be positive (greater than 0 and in MB)" << std::endl;
        return 1;
    }

    if (start_bytes == 0)
    {
        std::cerr << "Error: Start position must be non-negative" << std::endl;
        return 1;
    }

    if (end_bytes == 0)
    {
        std::cerr << "Error: End position must be non-negative" << std::endl;
        return 1;
    }

    FILE *test_file = fopen(gz_path.c_str(), "rb");
    if (!test_file)
    {
        spdlog::error("File '{}' does not exist or cannot be opened", gz_path);
        return 1;
    }
    fclose(test_file);

    bool has_byte_range = (start_bytes > 0 || end_bytes > 0);

    if (has_byte_range && (start_bytes == 0 || end_bytes == 0))
    {
        spdlog::error("Both --start and --end must be specified for byte range");
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
                    spdlog::warn(
                        "Existing index was created with {:.1f} MB chunks, but you specified {:.1f} MB chunks.",
                        existing_chunk_size,
                        chunk_size_mb);

                    if (!confirm_rebuild())
                    {
                        spdlog::info("Using existing index with {:.1f} MB chunks.", existing_chunk_size);
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
                    spdlog::info(
                        "Force rebuild: Existing index has {:.1f} MB chunks, rebuilding with {:.1f} MB chunks.",
                        existing_chunk_size,
                        chunk_size_mb);
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
        spdlog::info("Index not found or invalid, creating index for {}...", gz_path);

        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            spdlog::error("Cannot create DB {}: {}", idx_path, sqlite3_errmsg(db));
            free(idx_path);
            return 1;
        }

        spdlog::debug("Database opened successfully: {}", idx_path);

        if (dft::indexer::init(db) != SQLITE_OK)
        {
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        // get file info
        uint64_t bytes = file_size_bytes(gz_path.c_str());
        if (bytes == UINT64_MAX)
        {
            spdlog::error("Cannot stat {}", gz_path);
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
            spdlog::error("Prepare failed: {}", sqlite3_errmsg(db));
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
            spdlog::error("Insert failed: {}", sqlite3_errmsg(db));
            sqlite3_finalize(st);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }
        int file_id = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);

        /* build the index with configurable stride */
        auto stride = static_cast<size_t>(chunk_size_mb * 1024 * 1024);
        spdlog::debug("Building index with stride: {} bytes ({} MB)", stride, chunk_size_mb);
        int ret = dft::indexer::build(db, file_id, gz_path.c_str(), stride);
        if (ret != 0)
        {
            spdlog::error("Index build failed for {} (error code: {})", gz_path, ret);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        spdlog::info("Index built successfully for {}", gz_path);
        sqlite3_close(db);
    }

    // read operations
    if (has_byte_range)
    {
        spdlog::debug("Performing byte range read operation");
        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            spdlog::error("Cannot open DB {}: {}", idx_path, sqlite3_errmsg(db));
            free(idx_path);
            return 1;
        }

        char *output;
        size_t output_size;

        spdlog::info("Reading byte range [{} B, {} B] from {}...", start_bytes, end_bytes, gz_path);

        int ret = dft::reader::read_range_bytes(db, gz_path.c_str(), start_bytes, end_bytes, &output, &output_size);

        if (ret != 0)
        {
            spdlog::error("Failed to read range from {}", gz_path);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        spdlog::debug("Successfully read {} bytes from range", output_size);

        fwrite(output, 1, output_size, stdout);

        free(output);
        sqlite3_close(db);
    }

    free(idx_path);
    return 0;
}
