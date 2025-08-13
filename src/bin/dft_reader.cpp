#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <argparse/argparse.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <dft_utils/indexer/indexer.h>
#include <dft_utils/reader/reader.h>
#include <dft_utils/utils/filesystem.h>
#include <dft_utils/utils/logger.h>
#include <dft_utils/utils/platform_compat.h>

int main(int argc, char **argv)
{
    argparse::ArgumentParser program("dft_reader", "1.0");
    program.add_description("DFTracer utility for reading and indexing gzipped files");
    program.add_argument("file").help("Gzipped file to process").required();
    program.add_argument("-i", "--index").help("Index file to use").default_value<std::string>("");
    program.add_argument("-s", "--start").help("Start position in bytes").default_value<int64_t>(-1).scan<'d', int64_t>();
    program.add_argument("-e", "--end").help("End position in bytes").default_value<int64_t>(-1).scan<'d', int64_t>();
    program.add_argument("-c", "--chunk-size")
        .help("Chunk size for indexing in megabytes (default: 32)")
        .scan<'g', double>()
        .default_value<double>(32.0);
    program.add_argument("-f", "--force").help("Force rebuild index even if chunk size differs").flag();
    program.add_argument("--log-level")
        .help("Set logging level (trace, debug, info, warn, error, critical, off)")
        .default_value<std::string>("info");
    program.add_argument("--check").help("Check if index is valid").flag();
    program.add_argument("--raw").help("Use raw reading mode").flag();
    program.add_argument("--read-buffer-size")
        .help("Size of the read buffer in bytes (default: 1MB)")
        .default_value<size_t>(1 * 1024 * 1024)
        .scan<'d', size_t>();

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception &err)
    {
        spdlog::error("Error occurred: {}", err.what());
        std::cerr << program;
        return 1;
    }

    std::string gz_path = program.get<std::string>("file");
    std::string index_path = program.get<std::string>("--index");
    int64_t start_bytes = program.get<int64_t>("--start");
    int64_t end_bytes = program.get<int64_t>("--end");
    double chunk_size_mb = program.get<double>("--chunk-size");
    bool force_rebuild = program.get<bool>("--force");
    bool check_rebuild = program.get<bool>("--check");
    bool raw_mode = program.get<bool>("--raw");
    std::string log_level_str = program.get<std::string>("--log-level");

    // @todo: delete this later
    size_t read_buffer_size = program.get<size_t>("--read-buffer-size");

    // stderr-based logger to ensure logs don't interfere with data output
    auto logger = spdlog::stderr_color_mt("stderr");
    spdlog::set_default_logger(logger);
    dft::utils::set_log_level(log_level_str);

    spdlog::debug("Log level set to: {}", log_level_str);
    spdlog::debug("Processing file: {}", gz_path);
    spdlog::debug("Start position: {} B ({} MB)", start_bytes, start_bytes / (1024 * 1024));
    spdlog::debug("End position: {} B ({} MB)", end_bytes, end_bytes / (1024 * 1024));
    spdlog::debug("Chunk size: {} MB", chunk_size_mb);
    spdlog::debug("Force rebuild: {}", force_rebuild);

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

    std::string idx_path = index_path.empty() ? (gz_path + ".idx") : index_path;

    try
    {
        dft::indexer::Indexer indexer(gz_path, idx_path, chunk_size_mb, force_rebuild);

        if (check_rebuild)
        {
            if (!indexer.need_rebuild())
            {
                spdlog::debug("Index is up to date, no rebuild needed");
                return 0;
            }
        }

        if (force_rebuild || !fs::exists(idx_path))
        {
            indexer.build();
        }
    }
    catch (const std::runtime_error &e)
    {
        spdlog::error("Indexer error: {}", e.what());
        return 1;
    }

    // read operations
    if (start_bytes != -1)
    {
        size_t start_bytes_ = static_cast<size_t>(start_bytes);
        size_t end_bytes_ = end_bytes == -1 ? std::numeric_limits<size_t>::max() : static_cast<size_t>(end_bytes);
        spdlog::debug("Performing byte range read operation");

        try
        {
            dft::reader::Reader reader(gz_path, idx_path);
            auto max_bytes = reader.get_max_bytes();
            if (end_bytes_ > max_bytes)
            {
                spdlog::warn("End bytes exceed maximum available bytes, clamping to max value {} B ({} MB)",
                             max_bytes,
                             max_bytes / (1024 * 1024));
                end_bytes_ = max_bytes;
            }

            spdlog::debug("Using read buffer size: {} bytes", read_buffer_size);
            auto buffer = std::make_unique<char[]>(read_buffer_size);
            size_t bytes_written;
            size_t total_bytes = 0;


            if (raw_mode) {
                spdlog::debug("Using raw mode");
                while (reader.read(start_bytes_, end_bytes_, buffer.get(), read_buffer_size, &bytes_written))
                {
                    fwrite(buffer.get(), 1, bytes_written, stdout);
                    total_bytes += bytes_written;
                }
            } else {
                spdlog::debug("Using JSON lines aware mode");
                while (reader.read(start_bytes_, end_bytes_, buffer.get(), read_buffer_size, &bytes_written))
                {
                              fwrite(buffer.get(), 1, bytes_written, stdout);
                              total_bytes += bytes_written;
                          }
            
            }
            spdlog::debug("Successfully read {} bytes from range", total_bytes);
        }
        catch (const std::runtime_error &e)
        {
            spdlog::error("Reader error: {}", e.what());
            return 1;
        }
    }

    return 0;
}
