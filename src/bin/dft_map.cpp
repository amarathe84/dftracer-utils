#include <dftracer/utils/config.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/utils/filesystem.h>
#include <dftracer/utils/utils/json.h>
#include <dftracer/utils/utils/logger.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#include <dftracer/utils/pipeline/execution_context/threaded.h>
#include <dftracer/utils/pipeline/execution_context/mpi.h>
#include <dftracer/utils/reader/reader.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <argparse/argparse.hpp>
#include <iostream>
#include <vector>
#include <string>
#include <numeric>
#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <memory>
#include <cmath>
#include <cstdio>
#include <limits>

#if DFTRACER_UTILS_MPI_ENABLE
#include <mpi.h>
#endif

using namespace dftracer::utils::pipeline;
using namespace dftracer::utils::pipeline::execution_context;

struct HLM_AGG {
    static constexpr const char* TIME = "time";
    static constexpr const char* COUNT = "count";
    static constexpr const char* SIZE = "size";
};

const std::vector<std::string> HLM_EXTRA_COLS = {"cat", "io_cat", "acc_pat", "func_name"};

const double KiB = 1024.0;
const double MiB = KiB * KiB;
const double GiB = KiB * MiB;

const std::vector<double> SIZE_BINS = {
    -std::numeric_limits<double>::infinity(),
    4 * KiB,
    16 * KiB,
    64 * KiB,
    256 * KiB,
    1 * MiB,
    4 * MiB,
    16 * MiB,
    64 * MiB,
    256 * MiB,
    1 * GiB,
    4 * GiB,
    std::numeric_limits<double>::infinity(),
};

const std::vector<std::string> SIZE_BIN_SUFFIXES = {
    "0_4kib",
    "4kib_16kib", 
    "16kib_64kib",
    "64kib_256kib",
    "256kib_1mib",
    "1mib_4mib",
    "4mib_16mib",
    "16mib_64mib",
    "64mib_256mib",
    "256mib_1gib",
    "1gib_4gib",
    "4gib_plus",
};

const double DEFAULT_TIME_GRANULARITY = 1e6;

struct TraceRecord {
    std::string cat;
    std::string io_cat;
    std::string acc_pat;
    std::string func_name;
    double time;
    uint64_t count;
    uint64_t size;
    uint64_t time_range;  // Added for unique_sets collection
    
    // View type fields (dynamically populated based on view_types parameter)
    std::unordered_map<std::string, std::string> view_fields;
    
    // Bin columns (e.g., size_bin_*)
    std::unordered_map<std::string, uint32_t> bin_fields;
};

struct HighLevelMetrics {
    double time_sum = 0.0;
    uint64_t count_sum = 0;
    uint64_t size_sum = 0;
    std::unordered_map<std::string, uint32_t> bin_sums;          // Bin columns
    std::unordered_map<std::string, std::unordered_set<std::string>> unique_sets; // Unique view types
    
    // Grouping key components (for reconstruction)
    std::unordered_map<std::string, std::string> group_values;
};

std::string get_string_field(const dftracer::utils::json::JsonDocument& doc, const std::string& key) {
    if (!doc.is_object()) return "";
    
    auto obj_result = doc.get_object();
    if (obj_result.error()) return "";
    
    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == key) {
            if (field.value.is_string()) {
                auto str_result = field.value.get_string();
                if (!str_result.error()) {
                    return std::string(str_result.value());
                }
            }
        }
    }
    return "";
}

double get_double_field(const dftracer::utils::json::JsonDocument& doc, const std::string& key) {
    if (!doc.is_object()) return 0.0;
    
    auto obj_result = doc.get_object();
    if (obj_result.error()) return 0.0;
    
    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == key) {
            if (field.value.is_double()) {
                auto val_result = field.value.get_double();
                if (!val_result.error()) {
                    return val_result.value();
                }
            } else if (field.value.is_int64()) {
                auto val_result = field.value.get_int64();
                if (!val_result.error()) {
                    return static_cast<double>(val_result.value());
                }
            } else if (field.value.is_uint64()) {
                auto val_result = field.value.get_uint64();
                if (!val_result.error()) {
                    return static_cast<double>(val_result.value());
                }
            } else if (field.value.is_string()) {
                auto str_result = field.value.get_string();
                if (!str_result.error()) {
                    try {
                        return std::stod(std::string(str_result.value()));
                    } catch (...) {
                        // Invalid number string, return 0.0
                    }
                }
            }
        }
    }
    return 0.0;
}

uint64_t get_uint64_field(const dftracer::utils::json::JsonDocument& doc, const std::string& key) {
    if (!doc.is_object()) return 0;
    
    auto obj_result = doc.get_object();
    if (obj_result.error()) return 0;
    
    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == key) {
            if (field.value.is_uint64()) {
                auto val_result = field.value.get_uint64();
                if (!val_result.error()) {
                    return val_result.value();
                }
            } else if (field.value.is_int64()) {
                auto val_result = field.value.get_int64();
                if (!val_result.error()) {
                    return static_cast<uint64_t>(val_result.value());
                }
            } else if (field.value.is_double()) {
                auto val_result = field.value.get_double();
                if (!val_result.error()) {
                    return static_cast<uint64_t>(val_result.value());
                }
            } else if (field.value.is_string()) {
                auto str_result = field.value.get_string();
                if (!str_result.error()) {
                    try {
                        return std::stoull(std::string(str_result.value()));
                    } catch (...) {
                        // Invalid number string, return 0
                    }
                }
            }
        }
    }
    return 0;
}

std::string get_args_string_field(const dftracer::utils::json::JsonDocument& doc, const std::string& key) {
    if (!doc.is_object()) return "";
    
    auto obj_result = doc.get_object();
    if (obj_result.error()) return "";
    
    auto obj = obj_result.value();
    for (auto field : obj) {
        std::string field_key = std::string(field.key);
        if (field_key == "args" && field.value.is_object()) {
            auto args_result = field.value.get_object();
            if (!args_result.error()) {
                auto args = args_result.value();
                for (auto arg_field : args) {
                    std::string arg_key = std::string(arg_field.key);
                    if (arg_key == key && arg_field.value.is_string()) {
                        auto str_result = arg_field.value.get_string();
                        if (!str_result.error()) {
                            return std::string(str_result.value());
                        }
                    }
                }
            }
        }
    }
    return "";
}

std::unordered_map<std::string, std::string> POSIX_IO_CAT_MAPPING = {
    {"read", "read"},
    {"pread", "read"},
    {"pread64", "read"},
    {"readv", "read"},
    {"preadv", "read"},
    {"write", "write"},
    {"pwrite", "write"},
    {"pwrite64", "write"},
    {"writev", "write"},
    {"pwritev", "write"},
    {"open", "open"},
    {"open64", "open"},
    {"openat", "open"},
    {"close", "close"},
    {"__xstat64", "stat"},
    {"__lxstat64", "stat"},
    {"stat", "stat"},
    {"lstat", "stat"},
    {"fstat", "stat"}
};

std::unordered_set<std::string> POSIX_METADATA_FUNCTIONS = {
    "__xstat64", "__lxstat64", "stat", "lstat", "fstat", "access", "faccessat"
};

std::string derive_io_cat(const std::string& func_name) {
    if (POSIX_METADATA_FUNCTIONS.find(func_name) != POSIX_METADATA_FUNCTIONS.end()) {
        return "metadata";
    }
    
    auto it = POSIX_IO_CAT_MAPPING.find(func_name);
    if (it != POSIX_IO_CAT_MAPPING.end()) {
        return it->second;
    }
    
    return "other";
}

std::unordered_set<std::string> IGNORED_FUNC_NAMES = {
    "DLIOBenchmark.__init__",
    "DLIOBenchmark.initialize",
    "FileStorage.__init__",
    "IndexedBinaryMMapReader.__init__",
    "IndexedBinaryMMapReader.load_index",
    "IndexedBinaryMMapReader.next",
    "IndexedBinaryMMapReader.read_index",
    "NPZReader.__init__",
    "NPZReader.next",
    "NPZReader.read_index",
    "PyTorchCheckpointing.__init__",
    "PyTorchCheckpointing.finalize",
    "PyTorchCheckpointing.get_tensor",
    "SCRPyTorchCheckpointing.__init__",
    "SCRPyTorchCheckpointing.finalize",
    "SCRPyTorchCheckpointing.get_tensor",
    "TFCheckpointing.__init__",
    "TFCheckpointing.finalize",
    "TFCheckpointing.get_tensor",
    "TFDataLoader.__init__",
    "TFDataLoader.finalize",
    "TFDataLoader.next",
    "TFDataLoader.read",
    "TFFramework.get_loader",
    "TFFramework.init_loader",
    "TFFramework.is_nativeio_available",
    "TFFramework.trace_object",
    "TFReader.__init__",
    "TFReader.next",
    "TFReader.read_index",
    "TorchDataLoader.__init__",
    "TorchDataLoader.finalize",
    "TorchDataLoader.next",
    "TorchDataLoader.read",
    "TorchDataset.__init__",
    "TorchFramework.get_loader",
    "TorchFramework.init_loader",
    "TorchFramework.is_nativeio_available",
    "TorchFramework.trace_object"
};

std::vector<std::string> IGNORED_FUNC_PATTERNS = {
    ".save_state",
    "checkpoint_end_",
    "checkpoint_start_"
};

bool should_ignore_event(const std::string& func_name, const std::string& phase) {
    if (phase == "M") return true;
    
    if (IGNORED_FUNC_NAMES.find(func_name) != IGNORED_FUNC_NAMES.end()) {
        return true;
    }
    
    for (const auto& pattern : IGNORED_FUNC_PATTERNS) {
        if (func_name.find(pattern) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

// Calculate size bin using Python pd.cut logic
int get_size_bin_index(uint64_t size) {
    double size_double = static_cast<double>(size);
    
    auto it = std::upper_bound(SIZE_BINS.begin(), SIZE_BINS.end(), size_double);
    
    int bin_index = std::distance(SIZE_BINS.begin(), it) - 1;
    
    bin_index = std::max(0, std::min(bin_index, static_cast<int>(SIZE_BIN_SUFFIXES.size()) - 1));
    
    return bin_index;
}

void set_size_bins(TraceRecord& record) {
    if (record.size > 0) {
        int bin_index = get_size_bin_index(record.size);
        
        for (size_t i = 0; i < SIZE_BIN_SUFFIXES.size(); ++i) {
            std::string bin_name = "size_bin_" + SIZE_BIN_SUFFIXES[i];
            record.bin_fields[bin_name] = (i == static_cast<size_t>(bin_index)) ? 1 : 0;
        }
    } else {
        for (const auto& suffix : SIZE_BIN_SUFFIXES) {
            std::string bin_name = "size_bin_" + suffix;
            record.bin_fields[bin_name] = 0;
        }
    }
}

TraceRecord parse_trace_record(const dftracer::utils::json::JsonDocument& doc, 
                               const std::vector<std::string>& view_types,
                               double time_granularity) {
    TraceRecord record;
    
    std::string cat = get_string_field(doc, "cat");
    std::string func_name = get_string_field(doc, "name");
    std::string phase = get_string_field(doc, "ph");
    
    if (should_ignore_event(func_name, phase)) {
        return record;
    }
    
    std::transform(cat.begin(), cat.end(), cat.begin(), ::tolower);
    
    record.cat = cat;
    record.func_name = func_name;
    
    record.time = get_double_field(doc, "dur");
    record.count = 1;
    
    // Calculate time_range: ((ts + dur) / 2.0) / time_granularity
    double ts = get_double_field(doc, "ts");
    double dur = record.time;
    record.time_range = static_cast<uint64_t>(((ts + dur) / 2.0) / time_granularity);
    
    record.size = 0;
    if (cat == "posix" || cat == "stdio") {
        record.io_cat = derive_io_cat(func_name);
        
        std::string ret_str = get_args_string_field(doc, "ret");
        if (!ret_str.empty()) {
            try {
                uint64_t ret_value = std::stoull(ret_str);
                if (ret_value > 0 && (record.io_cat == "read" || record.io_cat == "write")) {
                    record.size = ret_value;
                }
            } catch (...) {
            }
        }
    } else {
        record.io_cat = "other";
    }
    
    // Set access pattern to 0 (matches Python COL_ACC_PAT = 0)
    record.acc_pat = "0";
    
    // Parse view type fields
    for (const auto& view_type : view_types) {
        if (view_type == "proc_name") {
            // Use combination of hostname + pid + tid (matches Python postread_trace)
            std::string hostname = get_args_string_field(doc, "hostname");
            std::string pid = get_string_field(doc, "pid");
            std::string tid = get_string_field(doc, "tid");
            
            if (hostname.empty()) hostname = "unknown";
            record.view_fields[view_type] = "app#" + hostname + "#" + pid + "#" + tid;
        } else if (view_type == "file_name") {
            // Try to get filename from args (matches Python file hash extraction)
            std::string fname = get_args_string_field(doc, "fname");
            if (fname.empty()) {
                fname = get_args_string_field(doc, "name");
            }
            record.view_fields[view_type] = fname;
        } else {
            // Try to get from main fields or args
            std::string value = get_string_field(doc, view_type);
            if (value.empty()) {
                value = get_args_string_field(doc, view_type);
            }
            record.view_fields[view_type] = value;
        }
    }
    
    // Set size bins (matches Python set_size_bins function)
    set_size_bins(record);
    
    return record;
}

// Create grouping key from view_types + HLM_EXTRA_COLS
std::string create_grouping_key(const TraceRecord& record, 
                               const std::vector<std::string>& view_types) {
    std::ostringstream key;
    
    // Add view type values
    for (const auto& view_type : view_types) {
        auto it = record.view_fields.find(view_type);
        if (it != record.view_fields.end()) {
            key << view_type << ":" << it->second << "|";
        }
    }
    
    // Add extra columns
    key << "cat:" << record.cat << "|";
    key << "io_cat:" << record.io_cat << "|";
    key << "acc_pat:" << record.acc_pat << "|";
    key << "func_name:" << record.func_name;
    
    return key.str();
}

// Ensure index exists for trace file (only on rank 0 in MPI)
void ensure_index_exists(const std::string& gz_path, size_t checkpoint_size, 
                        bool force_rebuild, int mpi_rank = 0) {
    if (mpi_rank == 0) {  // Only rank 0 creates indexes
        std::string idx_path = gz_path + ".idx";
        
        try {
            // Check if file exists
            FILE *test_file = fopen(gz_path.c_str(), "rb");
            if (!test_file) {
                throw std::runtime_error("File '" + gz_path + "' does not exist or cannot be opened");
            }
            fclose(test_file);
            
            // Create indexer
            dftracer::utils::indexer::Indexer indexer(gz_path, idx_path, checkpoint_size, force_rebuild);
            
            // Build index if needed
            if (force_rebuild || !fs::exists(idx_path) || indexer.need_rebuild()) {
                spdlog::info("Building index for file: {} (checkpoint size: {} MB)", 
                           gz_path, checkpoint_size / (1024 * 1024));
                indexer.build();
                spdlog::info("Index built successfully: {}", idx_path);
            } else {
                spdlog::debug("Index already exists and is up to date: {}", idx_path);
            }
            
        } catch (const std::exception& e) {
            spdlog::error("Error creating index for {}: {}", gz_path, e.what());
            throw;
        }
    }
    
#if DFTRACER_UTILS_MPI_ENABLE
    // Synchronize all ranks after index creation
    MPI_Barrier(MPI_COMM_WORLD);
#endif
}

// Read and parse traces from a file path
std::vector<TraceRecord> read_and_parse_traces(const std::string& gz_path, 
                                              const std::vector<std::string>& view_types,
                                              size_t checkpoint_size,
                                              double time_granularity) {
    std::vector<TraceRecord> records;
    
    try {
        // Create reader with the specified checkpoint size
        dftracer::utils::reader::Reader reader(gz_path, gz_path + ".idx", checkpoint_size);
        
        size_t max_bytes = reader.get_max_bytes();
        const size_t buffer_size = 1024 * 1024; // 1MB chunks
        std::vector<char> buffer(buffer_size);
        
        spdlog::info("Reading traces from: {} ({} bytes)", gz_path, max_bytes);
        
        for (size_t offset = 0; offset < max_bytes; ) {
            size_t end_offset = std::min(offset + buffer_size, max_bytes);
            size_t bytes_read = reader.read_line_bytes(offset, end_offset, 
                                                     buffer.data(), buffer_size);
            spdlog::debug("Read {} bytes from offset {} to {}", bytes_read, offset, end_offset);
            if (bytes_read == 0) break;
            
            // Parse content line by line, ignoring [ and ] characters
            std::string content(buffer.data(), bytes_read);
            std::istringstream stream(content);
            std::string line;
            
            while (std::getline(stream, line)) {
                // Skip empty lines, [ and ] lines
                if (line.empty() || line == "[" || line == "]" || line == " ") continue;
                
                // Remove trailing comma if present
                if (!line.empty() && line.back() == ',') {
                    line.pop_back();
                }
                
                // Skip if line is now empty
                if (line.empty()) continue;
                
                // Parse JSON object
                try {
                    auto doc = dftracer::utils::json::parse_json(line.c_str(), line.size());
                    TraceRecord record = parse_trace_record(doc, view_types, time_granularity);
                    // Only add non-empty records (empty records indicate filtered events)
                    if (!record.func_name.empty()) {
                        records.push_back(record);
                        spdlog::debug("Added record: func_name={}, cat={}, time={}", record.func_name, record.cat, record.time);
                    } else {
                        // Debug what fields we actually got
                        std::string cat = get_string_field(doc, "cat");
                        std::string func_name = get_string_field(doc, "name");
                        std::string phase = get_string_field(doc, "ph");
                        spdlog::debug("Filtered record: cat='{}', name='{}', ph='{}', ignored={}", 
                                     cat, func_name, phase, should_ignore_event(func_name, phase));
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("Exception parsing JSON: {}", e.what());
                }
            }
            
            offset += bytes_read;
            
            if (records.size() % 10000 == 0) {
                spdlog::debug("Parsed {} records...", records.size());
            }
        }
        
        spdlog::info("Total records parsed: {}", records.size());
        
    } catch (const std::exception& e) {
        spdlog::error("Error reading traces from {}: {}", gz_path, e.what());
    }
    
    return records;
}


// Replace 0 values with NaN equivalent (use negative values as NaN marker)
std::vector<HighLevelMetrics> replace_zeros_with_nan(std::vector<HighLevelMetrics> metrics) {
    for (auto& hlm : metrics) {
        if (hlm.time_sum == 0.0) hlm.time_sum = std::numeric_limits<double>::quiet_NaN();
        if (hlm.count_sum == 0) hlm.count_sum = UINT64_MAX; // Use max as NaN marker
        if (hlm.size_sum == 0) hlm.size_sum = UINT64_MAX;   // Use max as NaN marker
        
        for (auto& [bin_name, bin_value] : hlm.bin_sums) {
            if (bin_value == 0) bin_value = UINT32_MAX; // Use max as NaN marker
        }
    }
    return metrics;
}

// DFTracer Chrome tracing analyzer class - equivalent to Python DFTracerAnalyzer
class DFTracerAnalyzer {
public:
    // Constructor - matches Python Analyzer.__init__
    DFTracerAnalyzer(
        double time_granularity = DEFAULT_TIME_GRANULARITY,
        double time_resolution = 1e6,
        size_t checkpoint_size = dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE
    ) : time_granularity_(time_granularity),
        time_resolution_(time_resolution),
        checkpoint_size_(checkpoint_size) {
    }
    
    // Read trace data - equivalent to Python read_trace method
    std::vector<TraceRecord> read_trace(
        const std::string& trace_path,
        const std::vector<std::string>& view_types
    ) {
        spdlog::info("Reading trace from: {}", trace_path);
        return read_and_parse_traces(trace_path, view_types, checkpoint_size_, time_granularity_);
    }
    
    // Post-process trace data - equivalent to Python postread_trace method
    std::vector<TraceRecord> postread_trace(
        const std::vector<TraceRecord>& traces,
        const std::vector<std::string>& view_types
    ) {
        spdlog::info("Post-processing {} trace records", traces.size());
        
        // Apply filtering similar to Python implementation
        std::vector<TraceRecord> filtered_traces;
        for (const auto& record : traces) {
            // Skip if function name is in ignored list (matches Python filtering)
            if (IGNORED_FUNC_NAMES.find(record.func_name) != IGNORED_FUNC_NAMES.end()) {
                continue;
            }
            
            // Skip if function name matches ignored patterns
            bool should_skip = false;
            for (const auto& pattern : IGNORED_FUNC_PATTERNS) {
                if (record.func_name.find(pattern) != std::string::npos) {
                    should_skip = true;
                    break;
                }
            }
            
            if (!should_skip) {
                filtered_traces.push_back(record);
            }
        }
        
        spdlog::info("Filtered to {} records after post-processing", filtered_traces.size());
        return filtered_traces;
    }
    
    template<typename ExecutionContext>
    std::vector<HighLevelMetrics> compute_high_level_metrics(
        ExecutionContext& ctx,
        const std::vector<std::string>& trace_paths,
        const std::vector<std::string>& view_types
    ) {
        std::ostringstream view_types_stream;
        for (size_t i = 0; i < view_types.size(); ++i) {
            view_types_stream << view_types[i];
            if (i < view_types.size() - 1) view_types_stream << ", ";
        }
        spdlog::info("Computing high-level metrics for {} trace files with view types: {}", 
                    trace_paths.size(), view_types_stream.str());
        
        // Build HLM groupby columns: view_types + HLM_EXTRA_COLS
        std::vector<std::string> hlm_groupby = view_types;
        hlm_groupby.insert(hlm_groupby.end(), HLM_EXTRA_COLS.begin(), HLM_EXTRA_COLS.end());
        
        // Create pipeline that reads all traces
        auto hlm_pipeline = make_pipeline<std::string>()
            .map<std::vector<TraceRecord>>([this, &view_types](const std::string& path) {
                auto traces = this->read_trace(path, view_types);
                return this->postread_trace(traces, view_types);
            });
        
        // Execute pipeline to get all record batches
        auto start = std::chrono::high_resolution_clock::now();
        auto all_batches = hlm_pipeline.run(ctx, trace_paths);
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        
        spdlog::info("Pipeline execution completed in {}ms", 
                    std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());

        return _compute_high_level_metrics(all_batches, view_types);
    }

    template<typename ExecutionContext>
    std::vector<HighLevelMetrics> analyze_trace(
        ExecutionContext& ctx,
        const std::vector<std::string>& trace_paths,
        const std::vector<std::string>& view_types
    ) {
        spdlog::info("=== Starting DFTracer analysis ===");
        spdlog::info("Configuration:");
        spdlog::info("  Time granularity: {} µs", time_granularity_);
        spdlog::info("  Time resolution: {} µs", time_resolution_);
        spdlog::info("  Checkpoint size: {} MB", checkpoint_size_ / (1024 * 1024));
        
        // Ensure indexes exist for all trace files
        for (const auto& trace_path : trace_paths) {
            ensure_index_exists(trace_path, checkpoint_size_, false, 0);
        }
        
        // Compute high-level metrics
        auto hlm_results = compute_high_level_metrics(ctx, trace_paths, view_types);
        
        spdlog::info("=== Analysis completed ===");
        return hlm_results;
    }
    
private:
    double time_granularity_;
    double time_resolution_;
    size_t checkpoint_size_;

    std::vector<HighLevelMetrics> _compute_high_level_metrics(
        const std::vector<std::vector<TraceRecord>>& all_batches,
        const std::vector<std::string>& view_types
    ) {
        // Flatten all batches and apply groupby and aggregation
        std::unordered_map<std::string, std::vector<TraceRecord>> groups;
        for (const auto& batch : all_batches) {
            for (const auto& record : batch) {
                std::string key = create_grouping_key(record, view_types);
                groups[key].push_back(record);
            }
        }
        
        // Apply aggregation using class method
        auto aggregated_groups = aggregate_hlm(groups, view_types);
        
        // Convert to vector format and apply .replace(0, np.nan)
        std::vector<HighLevelMetrics> flattened_results;
        for (const auto& [key, hlm] : aggregated_groups) {
            flattened_results.push_back(hlm);
        }
        
        // Apply .replace(0, np.nan) equivalent
        flattened_results = replace_zeros_with_nan(std::move(flattened_results));
        
        spdlog::info("Total high-level metrics computed: {}", flattened_results.size());
        
        return flattened_results;
    }
    
    // Aggregate function equivalent to HLM_AGG
    std::unordered_map<std::string, HighLevelMetrics> 
    aggregate_hlm(const std::unordered_map<std::string, std::vector<TraceRecord>>& groups,
                  const std::vector<std::string>& view_types) {
        std::unordered_map<std::string, HighLevelMetrics> result;
        
        for (const auto& [key, records] : groups) {
            HighLevelMetrics hlm;
            
            // Aggregate core metrics (time: sum, count: sum, size: sum)
            for (const auto& record : records) {
                hlm.time_sum += record.time;
                hlm.count_sum += record.count;
                hlm.size_sum += record.size;
            }
            
            // Aggregate bin columns (sum for each bin)
            for (const auto& record : records) {
                for (const auto& [bin_name, bin_value] : record.bin_fields) {
                    hlm.bin_sums[bin_name] += bin_value;
                }
            }
            
            // Collect unique sets for view types not in current grouping
            // This implements: hlm_agg.update({col: unique_set() for col in view_types_diff})
            std::unordered_set<std::string> current_view_types_set(view_types.begin(), view_types.end());
            
            // Python VIEW_TYPES = ['file_name', 'proc_name', 'time_range']
            // If time_range is not in our grouping view_types, collect unique time_range values
            if (current_view_types_set.find("time_range") == current_view_types_set.end()) {
                for (const auto& record : records) {
                    hlm.unique_sets["time_range"].insert(std::to_string(record.time_range));
                }
            }
            
            for (const auto& record : records) {
                for (const auto& [view_type, value] : record.view_fields) {
                    // If this view_type is not in our current grouping, collect unique values
                    if (current_view_types_set.find(view_type) == current_view_types_set.end()) {
                        hlm.unique_sets[view_type].insert(value);
                    }
                }
            }
            
            result[key] = hlm;
        }
        
        return result;
    }
};




int main(int argc, char* argv[]) {
    // Initialize MPI if enabled
    int mpi_rank = 0;
#if DFTRACER_UTILS_MPI_ENABLE
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
#endif

    size_t default_checkpoint_size = dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE;
    auto default_checkpoint_size_str = std::to_string(default_checkpoint_size) + " B (" + std::to_string(default_checkpoint_size / (1024 * 1024)) + " MB)";
    
    // Argument parsing with argparse
    argparse::ArgumentParser program("dft_map", DFTRACER_UTILS_PACKAGE_VERSION);
    program.add_description("DFTracer utility for computing high-level metrics from trace files using pipeline processing");
    
    program.add_argument("files")
        .help("Gzipped trace files to process")
        .remaining();
    
    program.add_argument("-c", "--checkpoint-size")
        .help("Checkpoint size for indexing in bytes (default: " + default_checkpoint_size_str + ")")
        .scan<'d', size_t>()
        .default_value(default_checkpoint_size);
    
    program.add_argument("-f", "--force-rebuild")
        .help("Force rebuild of all indexes")
        .flag();
    
    program.add_argument("-v", "--view-types")
        .help("Comma-separated list of view types (default: proc_name,file_name)")
        .default_value(std::string("proc_name,file_name"));
    
    program.add_argument("--log-level")
        .help("Set logging level (trace, debug, info, warn, error, critical, off)")
        .default_value<std::string>("info");
    
    program.add_argument("-g", "--time-granularity")
        .help("Time granularity for time_range calculation in microseconds (default: 1e6)")
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
    
    // Setup logging
    if (mpi_rank == 0) {
        auto logger = spdlog::stderr_color_mt("stderr");
        spdlog::set_default_logger(logger);
        dftracer::utils::logger::set_log_level(program.get<std::string>("--log-level"));
    }
    
    // Extract arguments
    auto trace_paths = program.get<std::vector<std::string>>("files");
    size_t checkpoint_size = program.get<size_t>("--checkpoint-size");
    bool force_rebuild = program.get<bool>("--force-rebuild");
    std::string view_types_str = program.get<std::string>("--view-types");
    double time_granularity = program.get<double>("--time-granularity");
    
    // Parse view types
    std::vector<std::string> view_types;
    std::stringstream ss(view_types_str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        // Trim whitespace
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
    
    // Create execution context
    MPIContext mpi_ctx;
    
#if DFTRACER_UTILS_MPI_ENABLE
    if (mpi_rank == 0) {
        spdlog::info("Running with MPI: Rank {}/{}", mpi_ctx.get_rank(), mpi_ctx.get_size());
    }
#else
    spdlog::info("Running without MPI (sequential fallback)");
#endif
    
    try {
        // Note: Index creation is now handled internally by the analyzer
        
        // Create analyzer instance with configuration  
        DFTracerAnalyzer analyzer(
            /*time_granularity=*/time_granularity,
            /*time_resolution=*/1e6,  // Default time resolution
            /*checkpoint_size=*/checkpoint_size
        );
        
        // Analyze traces using the class-based API
        auto hlm_results = analyzer.analyze_trace(mpi_ctx, trace_paths, view_types);
        
        if (mpi_rank == 0) {
            // Display sample results
            spdlog::info("=== Sample High-Level Metrics ===");
            size_t sample_count = std::min(static_cast<size_t>(5), hlm_results.size());
            for (size_t i = 0; i < sample_count; ++i) {
                const auto& hlm = hlm_results[i];
                spdlog::info("Metric {}:", i+1);
                spdlog::info("  time_sum: {}", hlm.time_sum);
                spdlog::info("  count_sum: {}", hlm.count_sum);
                spdlog::info("  size_sum: {}", hlm.size_sum);
                spdlog::info("  bin_columns: {}", hlm.bin_sums.size());
                spdlog::info("  unique_sets: {}", hlm.unique_sets.size());
            }
        }
        
    } catch (const std::exception& e) {
        if (mpi_rank == 0) {
            spdlog::error("Error during high-level metrics computation: {}", e.what());
        }
#if DFTRACER_UTILS_MPI_ENABLE
        MPI_Finalize();
#endif
        return 1;
    }
    
    if (mpi_rank == 0) {
        spdlog::info("=== High-level metrics computation completed successfully! ===");
    }
    
    // Finalize MPI if enabled
#if DFTRACER_UTILS_MPI_ENABLE
    MPI_Finalize();
#endif
    
    return 0;
}
