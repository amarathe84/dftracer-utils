#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <argparse/argparse.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "indexer.h"
#include "platform_compat.h"
#include "reader.h"
#include "utils.h"

int main(int argc, char **argv)
{
    argparse::ArgumentParser program("dft_reader", "1.0");
    program.add_description("DFTracer utility for reading and indexing gzipped files");
    program.add_argument("file").help("Gzipped file to process").required();
    program.add_argument("-s", "--start").help("Start position in bytes").default_value<size_t>(0).scan<'d', size_t>();
    program.add_argument("-e", "--end").help("End position in bytes").default_value<size_t>(0).scan<'d', size_t>();
    program.add_argument("-c", "--chunk-size")
        .help("Chunk size for indexing in megabytes (default: 32)")
        .scan<'g', double>()
        .default_value<double>(32.0);
    program.add_argument("-f", "--force").help("Force rebuild index even if chunk size differs").flag();
    program.add_argument("--log-level")
        .help("Set logging level (trace, debug, info, warn, error, critical, off)")
        .default_value<std::string>("info");
    program.add_argument("--check").help("Check if index is valid").flag();

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception &err)
    {
        spdlog::error("Error occurred: {}", err.what());
        // std::cerr << program;
        return 1;
    }

    std::string gz_path = program.get<std::string>("file");
    
    size_t start_bytes = program.get<size_t>("--start");
    size_t end_bytes = program.get<size_t>("--end");
    double chunk_size_mb = program.get<double>("--chunk-size");
    bool force_rebuild = program.get<bool>("--force");
    bool check_rebuild = program.get<bool>("--check");
    std::string log_level_str = program.get<std::string>("--log-level");

    // stderr-based logger to ensure logs don't interfere with data output
    auto logger = spdlog::stderr_color_mt("stderr");
    spdlog::set_default_logger(logger);
    dft::utils::set_log_level(log_level_str);

    // spdlog::info("Log level set to: {}", log_level_str);

    spdlog::debug("Processing file: {}", gz_path);
    spdlog::debug("Start position: {} B", start_bytes);
    spdlog::debug("End position: {} B", end_bytes);
    spdlog::debug("Chunk size: {} MB", chunk_size_mb);
    spdlog::debug("Force rebuild: {}", force_rebuild);

    // Validate arguments
    if (chunk_size_mb <= 0)
    {
        spdlog::error("Chunk size must be positive (greater than 0 and in MB)");
        return 1;
    }

    FILE *test_file = fopen(gz_path.c_str(), "rb");
    if (!test_file)
    {
        spdlog::error("File '{}' does not exist or cannot be opened", gz_path);
        return 1;
    }

    fclose(test_file);

    // construct index path
    std::string idx_path = gz_path + ".idx";

    // Create indexer
    dft_indexer_t* indexer = dft_indexer_create(gz_path.c_str(), idx_path.c_str(), chunk_size_mb, force_rebuild);
    if (!indexer)
    {
        spdlog::error("Failed to create indexer");
        return 1;
    }

    if (check_rebuild) {
        int need_rebuild_result = dft_indexer_need_rebuild(indexer);
        if (need_rebuild_result == -1)
        {
            spdlog::error("Failed to check if rebuild is needed");
            dft_indexer_destroy(indexer);
            return 1;
        }

        if (need_rebuild_result == 0)
        {
            spdlog::info("Index is up to date, no rebuild needed");
            dft_indexer_destroy(indexer);
            return 0;
        }
    }

    if (force_rebuild) {
        if (dft_indexer_build(indexer) != 0)
        {
            spdlog::error("Failed to build index");
            dft_indexer_destroy(indexer);
            return 1;
        }
    }

    dft_indexer_destroy(indexer);

    // read operations
    if (end_bytes > start_bytes)
    {
        spdlog::debug("Performing byte range read operation");
        
        // Create reader
        dft_reader_t* reader = dft_reader_create(gz_path.c_str(), idx_path.c_str());
        if (!reader)
        {
            spdlog::error("Failed to create reader");
            return 1;
        }

        char *output;
        size_t output_size;

        spdlog::info("Reading byte range [{} B, {} B] from {}...", start_bytes, end_bytes, gz_path);

        int ret = dft_reader_read_range_bytes(reader, gz_path.c_str(), start_bytes, end_bytes, &output, &output_size);

        if (ret != 0)
        {
            spdlog::error("Failed to read range from {}", gz_path);
            dft_reader_destroy(reader);
            return 1;
        }

        spdlog::debug("Successfully read {} bytes from range", output_size);

        fwrite(output, 1, output_size, stdout);

        free(output);
        dft_reader_destroy(reader);
    }

    return 0;
}
