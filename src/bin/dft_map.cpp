#include <dftracer/analyzers/dftracer.h>
#include <dftracer/utils/config.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/execution_context/mpi.h>
#include <dftracer/utils/utils/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <argparse/argparse.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#if DFTRACER_UTILS_MPI_ENABLE
#include <mpi.h>
#endif

using namespace dftracer::utils::pipeline::execution_context;
using namespace dftracer::analyzers;

int main(int argc, char* argv[]) {
  int mpi_rank = 0;
#if DFTRACER_UTILS_MPI_ENABLE
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
#endif

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
      .default_value(DEFAULT_TIME_GRANULARITY);

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception& err) {
    if (mpi_rank == 0) {
      spdlog::error("Error occurred: {}", err.what());
      std::cerr << program;
    }
#if DFTRACER_UTILS_MPI_ENABLE
    MPI_Finalize();
#endif
    return 1;
  }

  if (mpi_rank == 0) {
    auto logger = spdlog::stderr_color_mt("stderr");
    spdlog::set_default_logger(logger);
    dftracer::utils::logger::set_log_level(
        program.get<std::string>("--log-level"));
  }

  auto trace_paths = program.get<std::vector<std::string>>("files");
  size_t checkpoint_size = program.get<size_t>("--checkpoint-size");
  bool force_rebuild = program.get<bool>("--force-rebuild");
  std::string view_types_str = program.get<std::string>("--view-types");
  double time_granularity = program.get<double>("--time-granularity");

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
    if (mpi_rank == 0) {
      spdlog::error("No trace files specified");
      std::cerr << program;
    }
#if DFTRACER_UTILS_MPI_ENABLE
    MPI_Finalize();
#endif
    return 1;
  }

  if (mpi_rank == 0) {
    spdlog::info("=== DFTracer High-Level Metrics Computation ===");
    spdlog::info("Configuration:");
    spdlog::info("  Checkpoint size: {} MB", checkpoint_size / (1024 * 1024));
    spdlog::info("  Force rebuild: {}", force_rebuild ? "true" : "false");
    spdlog::info("  Time granularity: {} µs", time_granularity);
    std::ostringstream view_types_oss;
    for (size_t i = 0; i < view_types.size(); ++i) {
      view_types_oss << view_types[i];
      if (i < view_types.size() - 1) view_types_oss << ", ";
    }
    spdlog::info("  View types: {}", view_types_oss.str());
    spdlog::info("  Trace files: {}", trace_paths.size());
  }

  MPIContext mpi_ctx;

#if DFTRACER_UTILS_MPI_ENABLE
  if (mpi_rank == 0) {
    spdlog::info("Running with MPI: Rank {}/{}", mpi_ctx.get_rank(),
                 mpi_ctx.get_size());
  }
#else
  spdlog::info("Running without MPI (sequential fallback)");
#endif

  try {
    DFTracerAnalyzer analyzer(time_granularity, 1e6, checkpoint_size);

    auto hlm_results = analyzer.analyze_trace(mpi_ctx, trace_paths, view_types);

    if (mpi_rank == 0) {
      spdlog::info("=== Sample High-Level Metrics ===");
      size_t sample_count =
          std::min(static_cast<size_t>(5), hlm_results.size());
      for (size_t i = 0; i < sample_count; ++i) {
        const auto& hlm = hlm_results[i];
        spdlog::info("Metric {}:", i + 1);
        spdlog::info("  time_sum: {}", hlm.time_sum);
        spdlog::info("  count_sum: {}", hlm.count_sum);
        spdlog::info("  size_sum: {}", hlm.size_sum);
        spdlog::info("  bin_columns: {}", hlm.bin_sums.size());
        spdlog::info("  unique_sets: {}", hlm.unique_sets.size());
      }
    }

  } catch (const std::exception& e) {
    if (mpi_rank == 0) {
      spdlog::error("Error during high-level metrics computation: {}",
                    e.what());
    }
#if DFTRACER_UTILS_MPI_ENABLE
    MPI_Finalize();
#endif
    return 1;
  }

  if (mpi_rank == 0) {
    spdlog::info(
        "=== High-level metrics computation completed successfully! ===");
  }

#if DFTRACER_UTILS_MPI_ENABLE
  MPI_Finalize();
#endif

  return 0;
}