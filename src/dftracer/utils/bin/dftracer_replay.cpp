#include <dftracer/utils/common/config.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/replay/replay.h>
#include <dftracer/utils/utils/filesystem.h>

#include <argparse/argparse.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>

using namespace dftracer::utils;
using namespace dftracer::utils::replay;

/**
 * Print replay results in a formatted way
 */
void print_results(const ReplayResult& result, bool verbose) {
    std::cout << "\n=== Replay Results ===" << std::endl;
    std::cout << "Total events processed: " << result.total_events << std::endl;
    std::cout << "Events executed: " << result.executed_events << std::endl;
    std::cout << "Events filtered: " << result.filtered_events << std::endl;
    std::cout << "Events failed: " << result.failed_events << std::endl;
    
    double success_rate = result.total_events > 0 
        ? (static_cast<double>(result.executed_events) / result.total_events * 100.0)
        : 0.0;
    std::cout << "Success rate: " << std::fixed << std::setprecision(2) << success_rate << "%" << std::endl;
    
    std::cout << "Total duration: " << result.total_duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "Execution duration: " << result.execution_duration.count() / 1000.0 << " ms" << std::endl;
    
    if (verbose && !result.function_counts.empty()) {
        std::cout << "\n=== Function Statistics ===" << std::endl;
        for (const auto& [func, count] : result.function_counts) {
            std::cout << "  " << std::setw(20) << std::left << func << ": " << count << std::endl;
        }
    }
    
    if (verbose && !result.category_counts.empty()) {
        std::cout << "\n=== Category Statistics ===" << std::endl;
        for (const auto& [cat, count] : result.category_counts) {
            std::cout << "  " << std::setw(15) << std::left << cat << ": " << count << std::endl;
        }
    }
    
    if (!result.error_messages.empty()) {
        std::cout << "\n=== Errors ===" << std::endl;
        for (const auto& error : result.error_messages) {
            std::cout << "  ERROR: " << error << std::endl;
        }
    }
}

/**
 * Parse comma-separated list into a set
 */
std::unordered_set<std::string> parse_list(const std::string& list_str) {
    std::unordered_set<std::string> result;
    if (list_str.empty()) return result;
    
    std::stringstream ss(list_str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        // Trim whitespace
        item.erase(0, item.find_first_not_of(" \t"));
        item.erase(item.find_last_not_of(" \t") + 1);
        if (!item.empty()) {
            result.insert(item);
        }
    }
    return result;
}

/**
 * Collect trace files from directory or file list
 */
std::vector<std::string> collect_trace_files(const std::vector<std::string>& inputs, bool recursive) {
    std::vector<std::string> trace_files;
    
    for (const auto& input : inputs) {
        if (fs::is_directory(input)) {
            // Collect .pfw and .pfw.gz files from directory
            if (recursive) {
                for (const auto& entry : fs::recursive_directory_iterator(input)) {
                    if (entry.is_regular_file()) {
                        std::string path = entry.path().string();
                        if (path.ends_with(".pfw") || path.ends_with(".pfw.gz")) {
                            trace_files.push_back(path);
                        }
                    }
                }
            } else {
                for (const auto& entry : fs::directory_iterator(input)) {
                    if (entry.is_regular_file()) {
                        std::string path = entry.path().string();
                        if (path.ends_with(".pfw") || path.ends_with(".pfw.gz")) {
                            trace_files.push_back(path);
                        }
                    }
                }
            }
        } else if (fs::is_regular_file(input)) {
            // Single file
            trace_files.push_back(input);
        } else {
            DFTRACER_UTILS_LOG_ERROR("Input not found or not accessible: %s", input.c_str());
        }
    }
    
    return trace_files;
}

int main(int argc, char** argv) {
    DFTRACER_UTILS_LOGGER_INIT();
    
    argparse::ArgumentParser program("dftracer_replay", DFTRACER_UTILS_PACKAGE_VERSION);
    program.add_description(
        "DFTracer replay utility - replays I/O operations from DFTracer trace files (.pfw, .pfw.gz)");
    
    // Input files/directories
    program.add_argument("inputs")
        .help("Trace files (.pfw, .pfw.gz) or directories containing trace files")
        .nargs(argparse::nargs_pattern::at_least_one);
    
    // Timing options
    program.add_argument("--no-timing")
        .help("Ignore original timing and execute as fast as possible")
        .flag();
    
    program.add_argument("--timing-scale")
        .help("Scale timing (1.0 = original speed, 0.5 = 2x faster, 2.0 = 2x slower)")
        .default_value(1.0)
        .scan<'g', double>();
    
    // Filtering options
    program.add_argument("--filter-functions")
        .help("Comma-separated list of functions to replay (empty = all)")
        .default_value(std::string(""));
    
    program.add_argument("--exclude-functions")
        .help("Comma-separated list of functions to exclude")
        .default_value(std::string(""));
    
    program.add_argument("--filter-categories")
        .help("Comma-separated list of categories to replay (posix, stdio, etc.)")
        .default_value(std::string(""));
    
    // Output options
    program.add_argument("-o", "--output-dir")
        .help("Output directory for created files (default: use original paths)")
        .default_value(std::string(""));
    
    program.add_argument("--max-file-size")
        .help("Maximum file size to create in bytes (default: 100MB)")
        .default_value<std::size_t>(100 * 1024 * 1024)
        .scan<'d', std::size_t>();
    
    // Execution options
    program.add_argument("--dry-run")
        .help("Parse and analyze traces without executing operations")
        .flag();
    
    program.add_argument("-r", "--recursive")
        .help("Recursively search directories for trace files")
        .flag();
    
    program.add_argument("-v", "--verbose")
        .help("Enable verbose output and detailed statistics")
        .flag();
    
    try {
        program.parse_args(argc, argv);
    } catch (const std::exception& err) {
        DFTRACER_UTILS_LOG_ERROR("Argument parsing error: %s", err.what());
        std::cerr << program;
        return 1;
    }
    
    // Parse arguments
    std::vector<std::string> inputs = program.get<std::vector<std::string>>("inputs");
    bool no_timing = program.get<bool>("--no-timing");
    double timing_scale = program.get<double>("--timing-scale");
    std::string filter_functions_str = program.get<std::string>("--filter-functions");
    std::string exclude_functions_str = program.get<std::string>("--exclude-functions");
    std::string filter_categories_str = program.get<std::string>("--filter-categories");
    std::string output_dir = program.get<std::string>("--output-dir");
    std::size_t max_file_size = program.get<std::size_t>("--max-file-size");
    bool dry_run = program.get<bool>("--dry-run");
    bool recursive = program.get<bool>("--recursive");
    bool verbose = program.get<bool>("--verbose");
    
    // Collect trace files
    std::vector<std::string> trace_files = collect_trace_files(inputs, recursive);
    
    if (trace_files.empty()) {
        std::cerr << "No trace files found in the specified inputs." << std::endl;
        return 1;
    }
    
    std::cout << "Found " << trace_files.size() << " trace file(s) to replay:" << std::endl;
    for (const auto& file : trace_files) {
        std::cout << "  " << file << std::endl;
    }
    
    // Create output directory if specified
    if (!output_dir.empty()) {
        if (!fs::exists(output_dir)) {
            try {
                fs::create_directories(output_dir);
                std::cout << "Created output directory: " << output_dir << std::endl;
            } catch (const std::exception& e) {
                std::cerr << "Failed to create output directory " << output_dir << ": " << e.what() << std::endl;
                return 1;
            }
        }
    }
    
    // Configure replay
    ReplayConfig config;
    config.maintain_timing = !no_timing;
    config.timing_scale = timing_scale;
    config.dry_run = dry_run;
    config.verbose = verbose;
    config.output_directory = output_dir;
    config.max_file_size = max_file_size;
    config.filter_functions = parse_list(filter_functions_str);
    config.exclude_functions = parse_list(exclude_functions_str);
    config.filter_categories = parse_list(filter_categories_str);
    
    // Print configuration
    std::cout << "\n=== Replay Configuration ===" << std::endl;
    std::cout << "Maintain timing: " << (config.maintain_timing ? "yes" : "no") << std::endl;
    if (config.maintain_timing) {
        std::cout << "Timing scale: " << config.timing_scale << "x" << std::endl;
    }
    std::cout << "Dry run: " << (config.dry_run ? "yes" : "no") << std::endl;
    std::cout << "Max file size: " << (config.max_file_size / (1024 * 1024)) << " MB" << std::endl;
    if (!config.output_directory.empty()) {
        std::cout << "Output directory: " << config.output_directory << std::endl;
    }
    if (!config.filter_functions.empty()) {
        std::cout << "Filter functions: ";
        bool first = true;
        for (const auto& func : config.filter_functions) {
            if (!first) std::cout << ", ";
            std::cout << func;
            first = false;
        }
        std::cout << std::endl;
    }
    if (!config.exclude_functions.empty()) {
        std::cout << "Exclude functions: ";
        bool first = true;
        for (const auto& func : config.exclude_functions) {
            if (!first) std::cout << ", ";
            std::cout << func;
            first = false;
        }
        std::cout << std::endl;
    }
    if (!config.filter_categories.empty()) {
        std::cout << "Filter categories: ";
        bool first = true;
        for (const auto& cat : config.filter_categories) {
            if (!first) std::cout << ", ";
            std::cout << cat;
            first = false;
        }
        std::cout << std::endl;
    }
    
    // Create replay engine and execute
    std::cout << "\n=== Starting Replay ===" << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    ReplayEngine engine(config);
    ReplayResult result = engine.replay_files(trace_files);
    
    auto end_time = std::chrono::steady_clock::now();
    auto total_wall_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    std::cout << "\n=== Replay Completed ===" << std::endl;
    std::cout << "Wall clock time: " << total_wall_time.count() / 1000.0 << " ms" << std::endl;
    
    // Print results
    print_results(result, verbose);
    
    // Return appropriate exit code
    if (result.failed_events > 0) {
        std::cout << "\nReplay completed with errors." << std::endl;
        return 2; // Partial success
    } else if (result.executed_events > 0) {
        std::cout << "\nReplay completed successfully." << std::endl;
        return 0; // Success  
    } else {
        std::cout << "\nNo events were executed." << std::endl;
        return 1; // No operations performed
    }
}