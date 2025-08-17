#include <dftracer/utils/config.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/filesystem.h>
#include <dftracer/utils/utils/logger.h>
#include <dftracer/utils/utils/platform_compat.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <argparse/argparse.hpp>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

int main(int argc, char **argv) {
  argparse::ArgumentParser program("dft_reader",
                                   DFTRACER_UTILS_PACKAGE_VERSION);
  program.add_description(
      "DFTracer utility for reading and indexing gzipped files");
  program.add_argument("file").help("Gzipped file to process").required();
  program.add_argument("-i", "--index")
      .help("Index file to use")
      .default_value<std::string>("");
  program.add_argument("-s", "--start")
      .help("Start position in bytes")
      .default_value<int64_t>(-1)
      .scan<'d', int64_t>();
  program.add_argument("-e", "--end")
      .help("End position in bytes")
      .default_value<int64_t>(-1)
      .scan<'d', int64_t>();
  program.add_argument("-c", "--chunk-size")
      .help("Chunk size for indexing in megabytes (default: 32)")
      .scan<'g', double>()
      .default_value<double>(32.0);
  program.add_argument("-f", "--force")
      .help("Force rebuild index even if chunk size differs")
      .flag();
  program.add_argument("--log-level")
      .help(
          "Set logging level (trace, debug, info, warn, error, critical, off)")
      .default_value<std::string>("info");
  program.add_argument("--check").help("Check if index is valid").flag();
  program.add_argument("--read-buffer-size")
      .help("Size of the read buffer in bytes (default: 1MB)")
      .default_value<size_t>(1 * 1024 * 1024)
      .scan<'d', size_t>();
  program.add_argument("--mode")
      .help("Set the reading mode (bytes, line_bytes, lines)")
      .default_value<std::string>("bytes")
      .choices("bytes", "line_bytes", "lines");

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    spdlog::error("Error occurred: {}", err.what());
    std::cerr << program;
    return 1;
  }

  std::string gz_path = program.get<std::string>("file");
  std::string index_path = program.get<std::string>("--index");
  int64_t start = program.get<int64_t>("--start");
  int64_t end = program.get<int64_t>("--end");
  double chunk_size_mb = program.get<double>("--chunk-size");
  bool force_rebuild = program.get<bool>("--force");
  bool check_rebuild = program.get<bool>("--check");
  std::string read_mode = program.get<std::string>("--mode");
  std::string log_level_str = program.get<std::string>("--log-level");

  size_t read_buffer_size = program.get<size_t>("--read-buffer-size");

  // stderr-based logger to ensure logs don't interfere with data output
  auto logger = spdlog::stderr_color_mt("stderr");
  spdlog::set_default_logger(logger);
  dftracer::utils::logger::set_log_level(log_level_str);

  spdlog::debug("Log level set to: {}", log_level_str);
  spdlog::debug("Processing file: {}", gz_path);
  spdlog::debug("Start position: {}", start);
  spdlog::debug("End position: {}", end);
  spdlog::debug("Mode: {}", read_mode);
  spdlog::debug("Chunk size: {} MB", chunk_size_mb);
  spdlog::debug("Force rebuild: {}", force_rebuild);

  if (chunk_size_mb <= 0) {
    spdlog::error("Chunk size must be positive (greater than 0 and in MB)");
    return 1;
  }

  FILE *test_file = fopen(gz_path.c_str(), "rb");
  if (!test_file) {
    spdlog::error("File '{}' does not exist or cannot be opened", gz_path);
    return 1;
  }
  fclose(test_file);

  std::string idx_path = index_path.empty() ? (gz_path + ".idx") : index_path;

  try {
    dftracer::utils::indexer::Indexer indexer(
        gz_path, idx_path, static_cast<size_t>(chunk_size_mb * 1024 * 1024),
        force_rebuild);

    if (check_rebuild) {
      if (!indexer.need_rebuild()) {
        spdlog::debug("Index is up to date, no rebuild needed");
        return 0;
      }
    }

    if (force_rebuild || !fs::exists(idx_path)) {
      spdlog::info("Building index for file: {}", gz_path);
      indexer.build();
    }
  } catch (const std::runtime_error &e) {
    spdlog::error("Indexer error: {}", e.what());
    return 1;
  }

  // read operations
  if (start != -1) {
    try {
      dftracer::utils::reader::Reader reader(gz_path, idx_path);

      if (read_mode.find("bytes") == std::string::npos) {
        size_t end_line = static_cast<size_t>(end);
        if (end == -1) {
          end_line = reader.get_num_lines();
        }

        spdlog::debug("Reading lines from {} to {}", start, end_line);

        auto lines = reader.read_lines(static_cast<size_t>(start), end_line);
        if (lines.empty()) {
          spdlog::debug("No lines read in the specified range");
          return 0;
        }
        fwrite(lines.c_str(), 1, lines.size(), stdout);
        // count new line only
        size_t line_count =
            static_cast<size_t>(std::count(lines.begin(), lines.end(), '\n'));
        spdlog::debug("Successfully read {} lines from range", line_count);
      } else {
        size_t start_bytes_ = static_cast<size_t>(start);
        size_t end_bytes_ = end == -1 ? std::numeric_limits<size_t>::max()
                                      : static_cast<size_t>(end);

        auto max_bytes = reader.get_max_bytes();
        if (end_bytes_ > max_bytes) {
          end_bytes_ = max_bytes;
        }
        spdlog::debug("Performing byte range read operation");
        spdlog::debug("Using read buffer size: {} bytes", read_buffer_size);
        auto buffer = std::make_unique<char[]>(read_buffer_size);
        size_t bytes_written;
        size_t total_bytes = 0;

        while ((bytes_written =
                    read_mode == "bytes"
                        ? reader.read(start_bytes_, end_bytes_, buffer.get(),
                                      read_buffer_size)
                        : reader.read_line_bytes(start_bytes_, end_bytes_,
                                                 buffer.get(),
                                                 read_buffer_size)) > 0) {
          fwrite(buffer.get(), 1, bytes_written, stdout);
          total_bytes += bytes_written;
        }

        spdlog::debug("Successfully read {} bytes from range", total_bytes);
      }
      fflush(stdout);
    } catch (const std::runtime_error &e) {
      spdlog::error("Reader error: {}", e.what());
      return 1;
    }
  }

  return 0;
}
