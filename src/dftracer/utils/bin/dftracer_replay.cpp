#include <dftracer/utils/common/config.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/replay/replay.h>
#include <dftracer/utils/utils/filesystem.h>

#include <argparse/argparse.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iomanip>

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
                        if ((path.size() >= 4 && path.substr(path.size() - 4) == ".pfw") || 
                            (path.size() >= 7 && path.substr(path.size() - 7) == ".pfw.gz")) {
                            trace_files.push_back(path);
                        }
                    }
                }
            } else {
                for (const auto& entry : fs::directory_iterator(input)) {
                    if (entry.is_regular_file()) {
                        std::string path = entry.path().string();
                        if ((path.size() >= 4 && path.substr(path.size() - 4) == ".pfw") || 
                            (path.size() >= 7 && path.substr(path.size() - 7) == ".pfw.gz")) {
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
    
    // Execution options
    program.add_argument("--dry-run")
        .help("Parse and analyze traces without executing operations")
        .flag();
    
    program.add_argument("--dftracer-mode")
        .help("Use DFTracer sleep-based replay (sleep for operation duration instead of doing actual I/O)")
        .flag();
    
    program.add_argument("--no-sleep")
        .help("When used with --dftracer-mode, disable sleep calls for maximum speed")
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
    bool dry_run = program.get<bool>("--dry-run");
    bool dftracer_mode = program.get<bool>("--dftracer-mode");
    bool no_sleep = program.get<bool>("--no-sleep");
    bool verbose = program.get<bool>("--verbose");
    
    // Validate --no-sleep usage
    if (no_sleep && !dftracer_mode) {
        std::cerr << "Error: --no-sleep can only be used with --dftracer-mode" << std::endl;
        return 1;
    }
    
    // Collect trace files (always non-recursive since we removed --recursive)
    std::vector<std::string> trace_files = collect_trace_files(inputs, false);
    
    if (trace_files.empty()) {
        std::cerr << "No trace files found in the specified inputs." << std::endl;
        return 1;
    }
    
    std::cout << "Found " << trace_files.size() << " trace file(s) to replay:" << std::endl;
    for (const auto& file : trace_files) {
        std::cout << "  " << file << std::endl;
    }
    
    // Configure replay
    ReplayConfig config;
    config.maintain_timing = !no_timing;
    config.dry_run = dry_run;
    config.dftracer_mode = dftracer_mode;
    config.no_sleep = no_sleep;
    config.verbose = verbose;

    
    // Print configuration
    std::cout << "\n=== Replay Configuration ===" << std::endl;
    std::cout << "Maintain timing: " << (config.maintain_timing ? "yes" : "no") << std::endl;
    std::cout << "Dry run: " << (config.dry_run ? "yes" : "no") << std::endl;
    if (config.dftracer_mode) {
        std::cout << "DFTracer mode: yes (" << (config.no_sleep ? "no-sleep" : "sleep-based") << ")" << std::endl;
    } else {
        std::cout << "DFTracer mode: no (actual I/O)" << std::endl;
    }
    
    // Create replay engine and execute
    std::cout << "\n=== Starting Replay ===" << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    ReplayEngine engine(config);
    ReplayResult result = engine.replay(trace_files);
    
    auto end_time = std::chrono::steady_clock::now();
    auto total_wall_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    std::cout << "\n=== Replay Completed ===" << std::endl;
    std::cout << "Wall clock time: " << static_cast<double>(total_wall_time.count()) / 1000.0 << " ms" << std::endl;
    
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