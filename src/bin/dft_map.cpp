#include <dftracer/utils/analyzers/analyzer.h>
#include <dftracer/utils/config.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/execution_context/threaded.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/json.h>
#include <dftracer/utils/utils/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <argparse/argparse.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace dftracer::utils;

int main(int argc, char* argv[]) {
  size_t default_checkpoint_size =
      dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE;
  auto default_checkpoint_size_str =
      std::to_string(default_checkpoint_size) + " B (" +
      std::to_string(default_checkpoint_size / (1024 * 1024)) + " MB)";

  argparse::ArgumentParser program("dft_map", DFTRACER_UTILS_PACKAGE_VERSION);
  program.add_description(
      "DFTracer utility for computing high-level metrics from trace files "
      "using pipeline processing");

  program.add_argument("files")
      .help("Gzipped trace files to process")
      .remaining();

  program.add_argument("-c", "--checkpoint-size")
      .help("Checkpoint size for indexing in bytes (default: " +
            default_checkpoint_size_str + ")")
      .scan<'d', size_t>()
      .default_value(default_checkpoint_size);

  program.add_argument("-f", "--force-rebuild")
      .help("Force rebuild of all indexes")
      .flag();

  program.add_argument("-v", "--view-types")
      .help("Comma-separated list of view types (default: proc_name,file_name)")
      .default_value(std::string("proc_name,file_name"));

  program.add_argument("--log-level")
      .help(
          "Set logging level (trace, debug, info, warn, error, critical, off)")
      .default_value<std::string>("info");

  program.add_argument("-g", "--time-granularity")
      .help(
          "Time granularity for time_range calculation in microseconds "
          "(default: 1e6)")
      .scan<'g', double>()
      .default_value(1e6);

  program.add_argument("--checkpoint")
      .help("Enable checkpointing for intermediate results")
      .flag();

  program.add_argument("--checkpoint-dir")
      .help(
          "Directory to store checkpoint data (required if --checkpoint is "
          "used)")
      .default_value(std::string(""));

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception& err) {
    spdlog::error("Error occurred: {}", err.what());
    std::cerr << program;
    return 1;
  }

  auto logger = spdlog::stderr_color_mt("stderr");
  spdlog::set_default_logger(logger);
  logger::set_log_level(program.get<std::string>("--log-level"));

  auto trace_paths = program.get<std::vector<std::string>>("files");
  size_t checkpoint_size = program.get<size_t>("--checkpoint-size");
  bool force_rebuild = program.get<bool>("--force-rebuild");
  std::string view_types_str = program.get<std::string>("--view-types");
  double time_granularity = program.get<double>("--time-granularity");
  bool checkpoint = program.get<bool>("--checkpoint");
  std::string checkpoint_dir = program.get<std::string>("--checkpoint-dir");

  std::vector<std::string> view_types;
  std::stringstream ss(view_types_str);
  std::string item;
  while (std::getline(ss, item, ',')) {
    item.erase(0, item.find_first_not_of(" \t"));
    item.erase(item.find_last_not_of(" \t") + 1);
    if (!item.empty()) {
      view_types.push_back(item);
    }
  }

  if (trace_paths.empty()) {
    spdlog::error("No trace files specified");
    std::cerr << program;
    return 1;
  }

  // Validate checkpoint arguments
  if (checkpoint && checkpoint_dir.empty()) {
    spdlog::error(
        "--checkpoint-dir must be specified when --checkpoint is enabled");
    std::cerr << program;
    return 1;
  }

  spdlog::info("=== DFTracer High-Level Metrics Computation ===");
  spdlog::info("Configuration:");
  spdlog::info("  Checkpoint size: {} MB", checkpoint_size / (1024 * 1024));
  spdlog::info("  Force rebuild: {}", force_rebuild ? "true" : "false");
  spdlog::info("  Time granularity: {} µs", time_granularity);
  spdlog::info("  Checkpointing: {}", checkpoint ? "enabled" : "disabled");
  if (checkpoint) {
    spdlog::info("  Checkpoint directory: {}", checkpoint_dir);
  }
  std::ostringstream view_types_oss;
  for (size_t i = 0; i < view_types.size(); ++i) {
    view_types_oss << view_types[i];
    if (i < view_types.size() - 1) view_types_oss << ", ";
  }
  spdlog::info("  View types: {}", view_types_oss.str());
  spdlog::info("  Trace files: {}", trace_paths.size());

  pipeline::context::ThreadedContext ctx;
  auto start_time = std::chrono::high_resolution_clock::now();

  analyzers::Analyzer analyzer(time_granularity);
  auto result = analyzer.analyze_trace(ctx, trace_paths, view_types);

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> duration = end_time - start_time;
  spdlog::debug("Duration: {} ms", duration.count());

  return 0;
}
