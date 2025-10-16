#include <dftracer/utils/replay/replay.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/reader/reader_factory.h>
#include <dftracer/utils/utils/string.h>

#include <yyjson.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <thread>
#include <algorithm>
#include <iomanip>

namespace dftracer::utils::replay {

// =============================================================================
// Utility Functions
// =============================================================================

namespace {

/**
 * Parse a JSON string value from yyjson
 */
std::string get_json_string(yyjson_val* val, const char* key, const std::string& default_value = "") {
    yyjson_val* field = yyjson_obj_get(val, key);
    if (field && yyjson_is_str(field)) {
        return std::string(yyjson_get_str(field));
    }
    return default_value;
}

/**
 * Parse a JSON uint64 value from yyjson
 */
std::uint64_t get_json_uint64(yyjson_val* val, const char* key, std::uint64_t default_value = 0) {
    yyjson_val* field = yyjson_obj_get(val, key);
    if (field && yyjson_is_uint(field)) {
        return yyjson_get_uint(field);
    } else if (field && yyjson_is_int(field)) {
        std::int64_t int_val = yyjson_get_int(field);
        return int_val >= 0 ? static_cast<std::uint64_t>(int_val) : default_value;
    }
    return default_value;
}

/**
 * Parse a JSON double value from yyjson
 */
double get_json_double(yyjson_val* val, const char* key, double default_value = 0.0) {
    yyjson_val* field = yyjson_obj_get(val, key);
    if (field && yyjson_is_real(field)) {
        return yyjson_get_real(field);
    } else if (field && yyjson_is_uint(field)) {
        return static_cast<double>(yyjson_get_uint(field));
    } else if (field && yyjson_is_int(field)) {
        return static_cast<double>(yyjson_get_int(field));
    }
    return default_value;
}

/**
 * Get a string value from args object
 */
std::string get_args_string(yyjson_val* root, const char* key, const std::string& default_value = "") {
    yyjson_val* args = yyjson_obj_get(root, "args");
    if (args && yyjson_is_obj(args)) {
        return get_json_string(args, key, default_value);
    }
    return default_value;
}

/**
 * Get a uint64 value from args object
 */
std::uint64_t get_args_uint64(yyjson_val* root, const char* key, std::uint64_t default_value = 0) {
    yyjson_val* args = yyjson_obj_get(root, "args");
    if (args && yyjson_is_obj(args)) {
        return get_json_uint64(args, key, default_value);
    }
    return default_value;
}

/**
 * Get an int64 value from args object
 */
std::int64_t get_args_int64(yyjson_val* root, const char* key, std::int64_t default_value = 0) {
    yyjson_val* args = yyjson_obj_get(root, "args");
    if (args && yyjson_is_obj(args)) {
        yyjson_val* field = yyjson_obj_get(args, key);
        if (field && yyjson_is_int(field)) {
            return yyjson_get_int(field);
        } else if (field && yyjson_is_uint(field)) {
            return static_cast<std::int64_t>(yyjson_get_uint(field));
        }
    }
    return default_value;
}

/**
 * Create directory path if it doesn't exist
 */
bool ensure_directory_exists(const std::string& path) {
    std::string dir_path = path.substr(0, path.find_last_of('/'));
    if (dir_path.empty() || dir_path == path) return true;
    
    struct stat st;
    if (stat(dir_path.c_str(), &st) == 0) {
        return S_ISDIR(st.st_mode);
    }
    
    // Try to create directory recursively
    return ensure_directory_exists(dir_path) && (mkdir(dir_path.c_str(), 0755) == 0);
}

} // anonymous namespace

// =============================================================================
// PosixExecutor Implementation
// =============================================================================

bool PosixExecutor::execute(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    const std::string& func_name = trace.func_name;
    
    if (config.dry_run) {
        DFTRACER_UTILS_LOG_DEBUG("DRY RUN: Would execute POSIX %s", func_name.c_str());
        return true;
    }
    
    if (func_name == "open" || func_name == "open64" || func_name == "openat") {
        return execute_open(trace, config);
    } else if (func_name == "close") {
        return execute_close(trace, config);
    } else if (func_name == "read" || func_name == "pread" || func_name == "pread64") {
        return execute_read(trace, config);
    } else if (func_name == "write" || func_name == "pwrite" || func_name == "pwrite64") {
        return execute_write(trace, config);
    } else if (func_name == "lseek" || func_name == "lseek64") {
        return execute_seek(trace, config);
    } else if (func_name == "stat" || func_name == "stat64" || func_name == "lstat" || func_name == "fstat") {
        return execute_stat(trace, config);
    }
    
    DFTRACER_UTILS_LOG_DEBUG("Unsupported POSIX function: %s", func_name.c_str());
    return false;
}

bool PosixExecutor::can_handle(const dftracer::utils::analyzers::Trace& trace) const {
    return trace.cat == "posix";
}

bool PosixExecutor::execute_open(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    // For now, we'll simulate the open by creating a placeholder file if needed
    // In a more complete implementation, we'd need to parse the actual arguments
    // from the trace to get the file path and mode
    
    DFTRACER_UTILS_LOG_DEBUG("Executing POSIX open");
    
    // This is a simplified implementation - a real implementation would need
    // to parse the actual function arguments from the trace
    if (!trace.fhash.empty()) {
        // Use file hash as a placeholder for the actual file path
        std::string file_path = config.output_directory.empty() 
            ? ("replay_file_" + trace.fhash)
            : (config.output_directory + "/replay_file_" + trace.fhash);
        
        ensure_directory_exists(file_path);
        
        int fd = open(file_path.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd >= 0) {
            open_files_[trace.fhash] = fd;
            DFTRACER_UTILS_LOG_DEBUG("Opened file %s with fd %d", file_path.c_str(), fd);
            return true;
        } else {
            DFTRACER_UTILS_LOG_ERROR("Failed to open file %s: %s", file_path.c_str(), strerror(errno));
            return false;
        }
    }
    
    return true; // Success for cases where we can't determine the file
}

bool PosixExecutor::execute_close(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    DFTRACER_UTILS_LOG_DEBUG("Executing POSIX close");
    
    auto it = open_files_.find(trace.fhash);
    if (it != open_files_.end()) {
        close(it->second);
        open_files_.erase(it);
        DFTRACER_UTILS_LOG_DEBUG("Closed file with hash %s", trace.fhash.c_str());
    }
    
    return true;
}

bool PosixExecutor::execute_read(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    DFTRACER_UTILS_LOG_DEBUG("Executing POSIX read (size: %lld)", static_cast<long long>(trace.size));
    
    auto it = open_files_.find(trace.fhash);
    if (it != open_files_.end() && trace.size > 0) {
        std::vector<char> buffer(std::min(static_cast<std::size_t>(trace.size), config.max_file_size));
        ssize_t bytes_read = read(it->second, buffer.data(), buffer.size());
        DFTRACER_UTILS_LOG_DEBUG("Read %zd bytes", bytes_read);
    }
    
    return true;
}

bool PosixExecutor::execute_write(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    DFTRACER_UTILS_LOG_DEBUG("Executing POSIX write (size: %lld)", static_cast<long long>(trace.size));
    
    auto it = open_files_.find(trace.fhash);
    if (it != open_files_.end() && trace.size > 0) {
        std::size_t write_size = std::min(static_cast<std::size_t>(trace.size), config.max_file_size);
        std::vector<char> buffer(write_size, 'A'); // Fill with dummy data
        ssize_t bytes_written = write(it->second, buffer.data(), buffer.size());
        DFTRACER_UTILS_LOG_DEBUG("Wrote %zd bytes", bytes_written);
    }
    
    return true;
}

bool PosixExecutor::execute_seek(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    DFTRACER_UTILS_LOG_DEBUG("Executing POSIX seek (offset: %lld)", static_cast<long long>(trace.offset));
    
    auto it = open_files_.find(trace.fhash);
    if (it != open_files_.end() && trace.offset >= 0) {
        off_t result = lseek(it->second, trace.offset, SEEK_SET);
        DFTRACER_UTILS_LOG_DEBUG("Seek to offset %lld, result: %lld", 
                                 static_cast<long long>(trace.offset), 
                                 static_cast<long long>(result));
    }
    
    return true;
}

bool PosixExecutor::execute_stat(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    DFTRACER_UTILS_LOG_DEBUG("Executing POSIX stat");
    
    // For stat operations, we don't need to do much in replay mode
    // Just log that we would have stat'd the file
    if (!trace.fhash.empty()) {
        DFTRACER_UTILS_LOG_DEBUG("Would stat file with hash %s", trace.fhash.c_str());
    }
    
    return true;
}


// =============================================================================
// ReplayEngine Implementation
// =============================================================================

ReplayEngine::ReplayEngine(const ReplayConfig& config) 
    : config_(config), replay_start_time_(std::chrono::steady_clock::now()) {
    
    if (config.dftracer_mode) {
        // In DFTracer mode, only use the DFTracerExecutor for sleep-based replay
        add_executor(std::make_unique<DFTracerExecutor>());
    } else {
        // Add default executors for normal replay mode
        add_executor(std::make_unique<PosixExecutor>());
        // TODO: Add StdioExecutor when implemented
    }
}

ReplayEngine::~ReplayEngine() = default;

void ReplayEngine::add_executor(std::unique_ptr<TraceExecutor> executor) {
    executors_.push_back(std::move(executor));
}

ReplayResult ReplayEngine::replay_file(const std::string& trace_file, const std::string& index_file) {
    ReplayResult result;
    
    DFTRACER_UTILS_LOG_DEBUG("Starting replay of file: %s", trace_file.c_str());
    
    auto start_time = std::chrono::steady_clock::now();
    
    try {
        // Check if the file is compressed or plain text
        bool is_compressed = (trace_file.size() >= 3 && trace_file.substr(trace_file.size() - 3) == ".gz") ||
                            (trace_file.size() >= 7 && trace_file.substr(trace_file.size() - 7) == ".tar.gz");
        
        if (is_compressed) {
            // Handle compressed files with ReaderFactory
            std::string idx_path = index_file.empty() ? (trace_file + ".idx") : index_file;
            auto reader = ReaderFactory::create(trace_file, idx_path);
            
            if (!reader) {
                result.error_messages.push_back("Failed to create reader for file: " + trace_file);
                return result;
            }
            
            // Create line processor for handling trace lines
            ReplayLineProcessor processor(*this, result);
            
            // Read all lines using the line processor
            reader->read_lines_with_processor(0, reader->get_num_lines(), processor);
        } else {
            // Handle plain text files directly
            std::ifstream file(trace_file);
            if (!file.is_open()) {
                result.error_messages.push_back("Failed to open plain text file: " + trace_file);
                return result;
            }
            
            std::string line;
            while (std::getline(file, line)) {
                // Skip empty lines and bracket lines
                if (line.empty() || line == "[" || line == "]") {
                    continue;
                }
                
                // Remove trailing comma if present
                if (!line.empty() && line.back() == ',') {
                    line.pop_back();
                }
                
                process_trace_line(line, result);
            }
        }
        
    } catch (const std::exception& e) {
        result.error_messages.push_back("Exception during replay: " + std::string(e.what()));
    }
    
    auto end_time = std::chrono::steady_clock::now();
    result.total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    
    DFTRACER_UTILS_LOG_DEBUG("Replay completed. Total events: %zu, Executed: %zu, Failed: %zu", 
                             result.total_events, result.executed_events, result.failed_events);
    
    return result;
}

ReplayResult ReplayEngine::replay_files(const std::vector<std::string>& trace_files) {
    ReplayResult aggregated_result;
    
    for (const auto& file : trace_files) {
        ReplayResult file_result = replay_file(file);
        
        // Aggregate results
        aggregated_result.total_events += file_result.total_events;
        aggregated_result.executed_events += file_result.executed_events;
        aggregated_result.filtered_events += file_result.filtered_events;
        aggregated_result.failed_events += file_result.failed_events;
        aggregated_result.total_duration += file_result.total_duration;
        aggregated_result.execution_duration += file_result.execution_duration;
        
        // Merge function and category counts
        for (const auto& [func, count] : file_result.function_counts) {
            aggregated_result.function_counts[func] += count;
        }
        for (const auto& [cat, count] : file_result.category_counts) {
            aggregated_result.category_counts[cat] += count;
        }
        
        // Merge error messages
        aggregated_result.error_messages.insert(aggregated_result.error_messages.end(),
                                                file_result.error_messages.begin(),
                                                file_result.error_messages.end());
    }
    
    return aggregated_result;
}

bool ReplayEngine::process_trace_line(const std::string& line, ReplayResult& result) {
    dftracer::utils::analyzers::Trace trace;
    
    if (!parse_trace_json(line, trace)) {
        return false; // Skip invalid JSON
    }
    
    result.total_events++;
    result.function_counts[trace.func_name]++;
    result.category_counts[trace.cat]++;
    
    if (!should_execute_trace(trace)) {
        result.filtered_events++;
        return true;
    }
    
    // Apply timing logic (skip during dry-run or dftracer-mode)
    // In dftracer-mode, the DFTracerExecutor handles all timing internally
    if (config_.maintain_timing && !config_.dry_run && !config_.dftracer_mode && trace.time_start > 0 && trace.type == dftracer::utils::analyzers::TraceType::Regular) {
        apply_timing(trace);
    }
    
    // Find and execute with appropriate executor
    TraceExecutor* executor = find_executor(trace);
    if (executor) {
        auto exec_start = std::chrono::steady_clock::now();
        bool success = executor->execute(trace, config_);
        auto exec_end = std::chrono::steady_clock::now();
        
        result.execution_duration += std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start);
        
        if (success) {
            result.executed_events++;
        } else {
            result.failed_events++;
            result.error_messages.push_back("Failed to execute " + trace.func_name + " with " + executor->get_name());
        }
    } else {
        result.failed_events++;
        if (config_.verbose) {
            DFTRACER_UTILS_LOG_DEBUG("No executor found for function: %s (category: %s)", 
                                     trace.func_name.c_str(), trace.cat.c_str());
        }
    }
    
    return true;
}

bool ReplayEngine::parse_trace_json(const std::string& json_line, dftracer::utils::analyzers::Trace& trace) {
    const char* trimmed;
    std::size_t trimmed_length;
    if (!json_trim_and_validate(json_line.c_str(), json_line.length(), trimmed, trimmed_length)) {
        return false;
    }
    
    yyjson_doc* doc = yyjson_read(trimmed, trimmed_length, 0);
    if (!doc) {
        return false;
    }
    
    yyjson_val* root = yyjson_doc_get_root(doc);
    if (!root || !yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return false;
    }
    
    // Parse basic fields
    trace.func_name = get_json_string(root, "name");
    trace.cat = get_json_string(root, "cat");
    std::string phase = get_json_string(root, "ph");
    
    trace.pid = get_json_uint64(root, "pid");
    trace.tid = get_json_uint64(root, "tid");
    trace.time_start = get_json_uint64(root, "ts");
    trace.duration = get_json_double(root, "dur");
    trace.time_end = trace.time_start + static_cast<std::uint64_t>(trace.duration);
    
    // Parse arguments
    trace.fhash = get_args_string(root, "fhash");
    trace.hhash = get_args_string(root, "hhash");
    trace.size = get_args_int64(root, "size", -1);
    trace.offset = get_args_int64(root, "offset", -1);
    
    // Determine trace type
    if (phase == "M") {
        if (trace.func_name == "FH") {
            trace.type = dftracer::utils::analyzers::TraceType::FileHash;
        } else if (trace.func_name == "HH") {
            trace.type = dftracer::utils::analyzers::TraceType::HostHash;
        } else {
            trace.type = dftracer::utils::analyzers::TraceType::OtherMetadata;
        }
    } else {
        trace.type = dftracer::utils::analyzers::TraceType::Regular;
    }
    
    trace.is_valid = !trace.func_name.empty();
    
    yyjson_doc_free(doc);
    return trace.is_valid;
}

void ReplayEngine::apply_timing(const dftracer::utils::analyzers::Trace& trace) {
    if (!config_.maintain_timing) {
        return; // Skip timing logic if timing is disabled
    }
    
    if (!first_timestamp_set_) {
        first_trace_timestamp_ = trace.time_start;
        first_timestamp_set_ = true;
        return;
    }
    
    // Calculate elapsed time since first trace
    std::uint64_t trace_elapsed_us = trace.time_start - first_trace_timestamp_;
    
    // Apply timing scale
    std::uint64_t scaled_elapsed_us = static_cast<std::uint64_t>(trace_elapsed_us * config_.timing_scale);
    
    // Calculate how long we should sleep
    auto replay_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - replay_start_time_);
    
    if (scaled_elapsed_us > static_cast<std::uint64_t>(replay_elapsed.count())) {
        std::uint64_t sleep_us = scaled_elapsed_us - replay_elapsed.count();
        
        // Add safety limits to prevent extremely long sleeps
        const std::uint64_t MAX_SLEEP_US = 10 * 1000 * 1000; // 10 seconds max
        if (sleep_us > MAX_SLEEP_US) {
            if (config_.verbose) {
                std::cout << "Warning: Capping sleep from " << sleep_us / 1000.0 
                          << " ms to " << MAX_SLEEP_US / 1000.0 << " ms" << std::endl;
            }
            sleep_us = MAX_SLEEP_US;
        }
        
        if (config_.verbose && sleep_us > 1000) { // Log sleeps > 1ms
            std::cout << "Timing sleep: " << sleep_us / 1000.0 << " ms" << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    }
}

bool ReplayEngine::should_execute_trace(const dftracer::utils::analyzers::Trace& trace) const {
    // Check function filters
    if (!config_.filter_functions.empty()) {
        if (config_.filter_functions.find(trace.func_name) == config_.filter_functions.end()) {
            return false;
        }
    }
    
    // Check exclusion filters
    if (!config_.exclude_functions.empty()) {
        if (config_.exclude_functions.find(trace.func_name) != config_.exclude_functions.end()) {
            return false;
        }
    }
    
    // Check category filters
    if (!config_.filter_categories.empty()) {
        if (config_.filter_categories.find(trace.cat) == config_.filter_categories.end()) {
            return false;
        }
    }
    
    // Skip metadata events by default
    if (trace.type != dftracer::utils::analyzers::TraceType::Regular) {
        return false;
    }
    
    return true;
}

TraceExecutor* ReplayEngine::find_executor(const dftracer::utils::analyzers::Trace& trace) {
    for (auto& executor : executors_) {
        if (executor->can_handle(trace)) {
            return executor.get();
        }
    }
    return nullptr;
}

std::string ReplayEngine::get_replay_file_path(const std::string& original_path) const {
    if (config_.output_directory.empty()) {
        return original_path;
    }
    
    // Extract filename from original path
    std::size_t last_slash = original_path.find_last_of('/');
    std::string filename = (last_slash != std::string::npos) 
        ? original_path.substr(last_slash + 1) 
        : original_path;
    
    return config_.output_directory + "/" + filename;
}

// =============================================================================
// ReplayLineProcessor Implementation
// =============================================================================

ReplayLineProcessor::ReplayLineProcessor(ReplayEngine& engine, ReplayResult& result)
    : engine_(engine), result_(result) {
}

bool ReplayLineProcessor::process(const char* data, std::size_t length) {
    std::string line(data, length);
    return engine_.process_trace_line(line, result_);
}

// =============================================================================
// DFTracerExecutor Implementation: Support DFTracer-mode execution


bool DFTracerExecutor::execute(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) {
    if (config.dry_run) {
        return true;
    }
    
    // DFTracer mode: sleep for the duration of the operation instead of doing actual I/O
    // This simulates the timing of the original operation without the overhead
    double duration_us = static_cast<double>(trace.time_end - trace.time_start);
    
    // Apply much more aggressive limits for practical testing
    // Cap individual operation sleeps to 1ms (vs 10 seconds in apply_timing)
    const double MAX_DFTRACER_SLEEP_US = 1.0 * 1000.0; // 1ms max
    if (duration_us > MAX_DFTRACER_SLEEP_US) {
        duration_us = MAX_DFTRACER_SLEEP_US;
    }
    
    // Handle sleep based on configuration
    if (config.no_sleep) {
        // No sleep mode: skip all sleep calls for maximum speed
        if (config.verbose && duration_us >= 100000.0) {
            std::cout << "DFTracer would sleep for " << std::fixed << std::setprecision(3) 
                      << duration_us / 1000.0 << " ms for " << trace.func_name << " (skipped)" << std::endl;            
        }
    } else {
        // Normal dftracer mode: perform sleep with duration limits
        if (config.verbose && duration_us >= 100.0) {
            std::cout << "DFTracer sleeping for " << std::fixed << std::setprecision(3) 
                      << duration_us / 1000.0 << " ms for " << trace.func_name << std::endl;
        }
        sleep_for_duration(duration_us);
    }
    
    return true;
}

bool DFTracerExecutor::can_handle(const dftracer::utils::analyzers::Trace& /* trace */) const {
    // Handle all trace events for dftracer replay mode
    return true;
}

void DFTracerExecutor::sleep_for_duration(double duration_microseconds) {
    if (duration_microseconds <= 0) return;
    
    // Add safety limits to prevent extremely long sleeps
    const double MAX_SLEEP_US = 10.0 * 1000.0 * 1000.0; // 10 seconds max
    if (duration_microseconds > MAX_SLEEP_US) {
        duration_microseconds = MAX_SLEEP_US;
    }
    
    // Convert microseconds to nanoseconds for high precision sleep
    auto sleep_duration = std::chrono::nanoseconds(
        static_cast<std::int64_t>(duration_microseconds * 1000)
    );
    
    std::this_thread::sleep_for(sleep_duration);
}

} // namespace dftracer::utils::replay