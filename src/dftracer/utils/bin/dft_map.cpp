#include <dftracer/utils/analyzers/analyzer.h>
#include <dftracer/utils/common/config.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/reader/reader.h>

#include <argparse/argparse.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace dftracer::utils;

int main(int argc, char* argv[]) {
    DFTRACER_UTILS_LOGGER_INIT();
    std::uint64_t default_checkpoint_size = Indexer::DEFAULT_CHECKPOINT_SIZE;
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
        .scan<'d', std::uint64_t>()
        .default_value(default_checkpoint_size);

    program.add_argument("-f", "--force-rebuild")
        .help("Force rebuild of all indexes")
        .flag();

    program.add_argument("-v", "--view-types")
        .help(
            "Comma-separated list of view types (default: proc_name,file_name)")
        .default_value(std::string("proc_name,file_name"));

    program.add_argument("--log-level")
        .help(
            "Set logging level (trace, debug, info, warn, error, critical, "
            "off)")
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
        DFTRACER_UTILS_LOG_ERROR("Error occurred: %s", err.what());
        std::cerr << program;
        return 1;
    }

    auto trace_paths = program.get<std::vector<std::string>>("files");
    std::uint64_t checkpoint_size =
        program.get<std::uint64_t>("--checkpoint-size");
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
        DFTRACER_UTILS_LOG_ERROR("No trace files specified", "");
        std::cerr << program;
        return 1;
    }

    // Validate checkpoint arguments
    if (checkpoint && checkpoint_dir.empty()) {
        DFTRACER_UTILS_LOG_ERROR(
            "--checkpoint-dir must be specified when --checkpoint is enabled",
            "");
        std::cerr << program;
        return 1;
    }

    DFTRACER_UTILS_LOG_INFO("=== DFTracer High-Level Metrics Computation ===",
                            "");
    DFTRACER_UTILS_LOG_INFO("Configuration:", "");
    DFTRACER_UTILS_LOG_INFO(
        "  Checkpoint size: %.1f MB",
        static_cast<double>(checkpoint_size) / (1024 * 1024));
    DFTRACER_UTILS_LOG_INFO("  Force rebuild: %s",
                            force_rebuild ? "true" : "false");
    DFTRACER_UTILS_LOG_INFO("  Time granularity: %.1f µs", time_granularity);
    DFTRACER_UTILS_LOG_INFO("  Checkpointing: %s",
                            checkpoint ? "enabled" : "disabled");
    if (checkpoint) {
        DFTRACER_UTILS_LOG_INFO("  Checkpoint directory: %s",
                                checkpoint_dir.c_str());
    }
    std::ostringstream view_types_oss;
    for (size_t i = 0; i < view_types.size(); ++i) {
        view_types_oss << view_types[i];
        if (i < view_types.size() - 1) view_types_oss << ", ";
    }
    DFTRACER_UTILS_LOG_INFO("  View types: %s", view_types_oss.str().c_str());
    DFTRACER_UTILS_LOG_INFO("  Trace files: %d", trace_paths.size());

    ThreadExecutor executor;
    // SequentialExecutor executor;
    auto start_time = std::chrono::high_resolution_clock::now();

    auto config = analyzers::AnalyzerConfigManager::Default()
                      .set_time_granularity(time_granularity)
                      .set_checkpoint(checkpoint)
                      .set_checkpoint_dir(checkpoint_dir)
                      .set_checkpoint_size(checkpoint_size);

    analyzers::Analyzer analyzer(config);
    auto pipeline = analyzer.analyze(trace_paths, view_types);

    executor.execute(pipeline, trace_paths);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - start_time;
    DFTRACER_UTILS_LOG_DEBUG("Duration: %.1f ms", duration.count());

    return 0;
}
