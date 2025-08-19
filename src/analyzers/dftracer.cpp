#include <dftracer/analyzers/dftracer.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/filesystem.h>
#include <spdlog/spdlog.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <fstream>
#include <limits>
#include <memory>
#include <sstream>

#if DFTRACER_UTILS_MPI_ENABLE
#include <mpi.h>
#endif

namespace dftracer {
namespace analyzers {

const std::vector<std::string> HLM_EXTRA_COLS = {"cat", "io_cat", "acc_pat",
                                                 "func_name"};
const double DEFAULT_TIME_GRANULARITY = 1e6;

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
    "0_4kib",       "4kib_16kib",  "16kib_64kib", "64kib_256kib",
    "256kib_1mib",  "1mib_4mib",   "4mib_16mib",  "16mib_64mib",
    "64mib_256mib", "256mib_1gib", "1gib_4gib",   "4gib_plus",
};

const std::unordered_map<std::string, std::string> POSIX_IO_CAT_MAPPING = {
    {"read", "read"},       {"pread", "read"},     {"pread64", "read"},
    {"readv", "read"},      {"preadv", "read"},    {"write", "write"},
    {"pwrite", "write"},    {"pwrite64", "write"}, {"writev", "write"},
    {"pwritev", "write"},   {"open", "open"},      {"open64", "open"},
    {"openat", "open"},     {"close", "close"},    {"__xstat64", "stat"},
    {"__lxstat64", "stat"}, {"stat", "stat"},      {"lstat", "stat"},
    {"fstat", "stat"}};

const std::unordered_set<std::string> POSIX_METADATA_FUNCTIONS = {
    "__xstat64", "__lxstat64", "stat", "lstat", "fstat", "access", "faccessat"};

const std::unordered_set<std::string> IGNORED_FUNC_NAMES = {
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
    "TorchFramework.trace_object"};

const std::vector<std::string> IGNORED_FUNC_PATTERNS = {
    ".save_state", "checkpoint_end_", "checkpoint_start_"};

std::string derive_io_cat(const std::string& func_name) {
  if (POSIX_METADATA_FUNCTIONS.find(func_name) !=
      POSIX_METADATA_FUNCTIONS.end()) {
    return "metadata";
  }

  auto it = POSIX_IO_CAT_MAPPING.find(func_name);
  if (it != POSIX_IO_CAT_MAPPING.end()) {
    return it->second;
  }

  return "other";
}

bool should_ignore_event(const std::string& func_name,
                         const std::string& phase) {
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
  bin_index = std::max(
      0, std::min(bin_index, static_cast<int>(SIZE_BIN_SUFFIXES.size()) - 1));

  return bin_index;
}

void set_size_bins(TraceRecord& record) {
  if (record.size > 0) {
    int bin_index = get_size_bin_index(record.size);

    for (size_t i = 0; i < SIZE_BIN_SUFFIXES.size(); ++i) {
      std::string bin_name = "size_bin_" + SIZE_BIN_SUFFIXES[i];
      record.bin_fields[bin_name] =
          (i == static_cast<size_t>(bin_index)) ? 1 : 0;
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
  using namespace dftracer::utils::json;

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
  record.time_range =
      static_cast<uint64_t>(((ts + dur) / 2.0) / time_granularity);

  record.size = 0;
  if (cat == "posix" || cat == "stdio") {
    record.io_cat = derive_io_cat(func_name);

    std::string ret_str = get_args_string_field(doc, "ret");
    if (!ret_str.empty()) {
      try {
        uint64_t ret_value = std::stoull(ret_str);
        if (ret_value > 0 &&
            (record.io_cat == "read" || record.io_cat == "write")) {
          record.size = ret_value;
        }
      } catch (...) {
      }
    }
  } else {
    record.io_cat = "other";
  }

  record.acc_pat = "0";

  for (const auto& view_type : view_types) {
    if (view_type == "proc_name") {
      std::string hostname = get_args_string_field(doc, "hostname");
      std::string pid = get_string_field(doc, "pid");
      std::string tid = get_string_field(doc, "tid");

      if (hostname.empty()) hostname = "unknown";
      record.view_fields[view_type] = "app#" + hostname + "#" + pid + "#" + tid;
    } else if (view_type == "file_name") {
      std::string fname = get_args_string_field(doc, "fname");
      if (fname.empty()) {
        fname = get_args_string_field(doc, "name");
      }
      record.view_fields[view_type] = fname;
    } else {
      std::string value = get_string_field(doc, view_type);
      if (value.empty()) {
        value = get_args_string_field(doc, view_type);
      }
      record.view_fields[view_type] = value;
    }
  }

  set_size_bins(record);

  return record;
}

std::string create_grouping_key(const TraceRecord& record,
                                const std::vector<std::string>& view_types) {
  std::ostringstream key;

  for (const auto& view_type : view_types) {
    auto it = record.view_fields.find(view_type);
    if (it != record.view_fields.end()) {
      key << view_type << ":" << it->second << "|";
    }
  }

  key << "cat:" << record.cat << "|";
  key << "io_cat:" << record.io_cat << "|";
  key << "acc_pat:" << record.acc_pat << "|";
  key << "func_name:" << record.func_name;

  return key.str();
}

void ensure_index_exists(const std::string& gz_path, size_t checkpoint_size,
                         bool force_rebuild, int mpi_rank) {
  if (mpi_rank == 0) {
    std::string idx_path = gz_path + ".idx";

    try {
      FILE* test_file = fopen(gz_path.c_str(), "rb");
      if (!test_file) {
        throw std::runtime_error("File '" + gz_path +
                                 "' does not exist or cannot be opened");
      }
      fclose(test_file);

      dftracer::utils::indexer::Indexer indexer(gz_path, idx_path,
                                                checkpoint_size, force_rebuild);

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
  MPI_Barrier(MPI_COMM_WORLD);
#endif
}

std::vector<TraceRecord> read_and_parse_traces(
    const std::string& gz_path, const std::vector<std::string>& view_types,
    size_t checkpoint_size, double time_granularity) {
  using namespace dftracer::utils::json;

  std::vector<TraceRecord> records;

  try {
    dftracer::utils::reader::Reader reader(gz_path, gz_path + ".idx",
                                           checkpoint_size);

    size_t max_bytes = reader.get_max_bytes();
    const size_t buffer_size = 1024 * 1024;  // 1MB chunks
    std::vector<char> buffer(buffer_size);

    spdlog::info("Reading traces from: {} ({} bytes)", gz_path, max_bytes);

    for (size_t offset = 0; offset < max_bytes;) {
      size_t end_offset = std::min(offset + buffer_size, max_bytes);
      size_t bytes_read = reader.read_line_bytes(offset, end_offset,
                                                 buffer.data(), buffer_size);
      spdlog::debug("Read {} bytes from offset {} to {}", bytes_read, offset,
                    end_offset);
      if (bytes_read == 0) break;

      // Parse content line by line, ignoring [ and ]
      std::string content(buffer.data(), bytes_read);
      std::istringstream stream(content);
      std::string line;

      while (std::getline(stream, line)) {
        if (line.empty() || line == "[" || line == "]" || line == " ") continue;

        if (!line.empty() && line.back() == ',') {
          line.pop_back();
        }

        if (line.empty()) continue;

        try {
          auto doc = parse_json(line.c_str(), line.size());
          TraceRecord record =
              parse_trace_record(doc, view_types, time_granularity);
          // Only add non-empty records
          if (!record.func_name.empty()) {
            records.push_back(record);
            spdlog::debug("Added record: func_name={}, cat={}, time={}",
                          record.func_name, record.cat, record.time);
          } else {
            std::string cat = get_string_field(doc, "cat");
            std::string func_name = get_string_field(doc, "name");
            std::string phase = get_string_field(doc, "ph");
            spdlog::debug(
                "Filtered record: cat='{}', name='{}', ph='{}', ignored={}",
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

// Replace 0 values with NaN equivalent
std::vector<HighLevelMetrics> replace_zeros_with_nan(
    std::vector<HighLevelMetrics> metrics) {
  for (auto& hlm : metrics) {
    if (hlm.time_sum == 0.0)
      hlm.time_sum = std::numeric_limits<double>::quiet_NaN();
    if (hlm.count_sum == 0) hlm.count_sum = UINT64_MAX;
    if (hlm.size_sum == 0) hlm.size_sum = UINT64_MAX;

    for (auto& [bin_name, bin_value] : hlm.bin_sums) {
      if (bin_value == 0) bin_value = UINT32_MAX;
    }
  }
  return metrics;
}

// DFTracerAnalyzer implementation
DFTracerAnalyzer::DFTracerAnalyzer(double time_granularity,
                                   double time_resolution,
                                   size_t checkpoint_size,
                                   bool checkpoint,
                                   const std::string& checkpoint_dir)
    : time_granularity_(time_granularity),
      time_resolution_(time_resolution),
      checkpoint_size_(checkpoint_size),
      checkpoint_(checkpoint),
      checkpoint_dir_(checkpoint_dir) {}

std::vector<TraceRecord> DFTracerAnalyzer::read_trace(
    const std::string& trace_path, const std::vector<std::string>& view_types) {
  spdlog::info("Reading trace from: {}", trace_path);
  return read_and_parse_traces(trace_path, view_types, checkpoint_size_,
                               time_granularity_);
}

std::vector<TraceRecord> DFTracerAnalyzer::postread_trace(
    const std::vector<TraceRecord>& traces,
    const std::vector<std::string>& view_types) {
  spdlog::info("Post-processing {} trace records", traces.size());

  // Apply filtering
  std::vector<TraceRecord> filtered_traces;
  for (const auto& record : traces) {
    if (IGNORED_FUNC_NAMES.find(record.func_name) != IGNORED_FUNC_NAMES.end()) {
      continue;
    }

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

  spdlog::info("Filtered to {} records after post-processing",
               filtered_traces.size());
  return filtered_traces;
}

std::vector<HighLevelMetrics> DFTracerAnalyzer::_compute_high_level_metrics(
    const std::vector<std::vector<TraceRecord>>& all_batches,
    const std::vector<std::string>& view_types) {
  std::unordered_map<std::string, std::vector<TraceRecord>> groups;
  for (const auto& batch : all_batches) {
    for (const auto& record : batch) {
      std::string key = create_grouping_key(record, view_types);
      groups[key].push_back(record);
    }
  }

  auto aggregated_groups = aggregate_hlm(groups, view_types);

  std::vector<HighLevelMetrics> flattened_results;
  for (const auto& [key, hlm] : aggregated_groups) {
    flattened_results.push_back(hlm);
  }

  flattened_results = replace_zeros_with_nan(std::move(flattened_results));

  spdlog::info("Total high-level metrics computed: {}",
               flattened_results.size());

  return flattened_results;
}

std::unordered_map<std::string, HighLevelMetrics>
DFTracerAnalyzer::aggregate_hlm(
    const std::unordered_map<std::string, std::vector<TraceRecord>>& groups,
    const std::vector<std::string>& view_types) {
  std::unordered_map<std::string, HighLevelMetrics> result;

  for (const auto& [key, records] : groups) {
    HighLevelMetrics hlm;

    for (const auto& record : records) {
      hlm.time_sum += record.time;
      hlm.count_sum += record.count;
      hlm.size_sum += record.size;
    }

    for (const auto& record : records) {
      for (const auto& [bin_name, bin_value] : record.bin_fields) {
        hlm.bin_sums[bin_name] += bin_value;
      }
    }

    // Parse grouping key to extract group values
    // Key format: "view_type:value|cat:value|io_cat:value|acc_pat:value|func_name:value"
    std::istringstream key_stream(key);
    std::string segment;
    
    while (std::getline(key_stream, segment, '|')) {
      size_t colon_pos = segment.find(':');
      if (colon_pos != std::string::npos) {
        std::string field_name = segment.substr(0, colon_pos);
        std::string field_value = segment.substr(colon_pos + 1);
        hlm.group_values[field_name] = field_value;
      }
    }

    // Collect unique sets for view types not in current grouping
    std::unordered_set<std::string> current_view_types_set(view_types.begin(),
                                                           view_types.end());

    // If time_range is not in grouping view_types, collect unique time_range
    // values
    if (current_view_types_set.find("time_range") ==
        current_view_types_set.end()) {
      for (const auto& record : records) {
        hlm.unique_sets["time_range"].insert(std::to_string(record.time_range));
      }
    }

    for (const auto& record : records) {
      for (const auto& [view_type, value] : record.view_fields) {
        if (current_view_types_set.find(view_type) ==
            current_view_types_set.end()) {
          hlm.unique_sets[view_type].insert(value);
        }
      }
    }

    result[key] = hlm;
  }

  return result;
}

std::string DFTracerAnalyzer::get_checkpoint_name(const std::vector<std::string>& args) const {
  std::ostringstream name_stream;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0) name_stream << "_";
    name_stream << args[i];
  }
  name_stream << "_" << static_cast<int>(time_granularity_);
  return name_stream.str();
}

std::string DFTracerAnalyzer::get_checkpoint_path(const std::string& name) const {
  return checkpoint_dir_ + "/" + name;
}

bool DFTracerAnalyzer::has_checkpoint(const std::string& name) const {
  std::string checkpoint_path = get_checkpoint_path(name);
  std::string metadata_path = checkpoint_path + "/_metadata";
  return fs::exists(metadata_path);
}

void DFTracerAnalyzer::store_view(const std::string& name, const std::vector<HighLevelMetrics>& view) {
  try {
    std::string view_path = get_checkpoint_path(name);
    
    // Create checkpoint directory
    fs::create_directories(view_path);

    if (view.empty()) {
      // Create empty parquet file with basic schema
      auto schema = arrow::schema({
        arrow::field("time_sum", arrow::float64()),
        arrow::field("count_sum", arrow::uint64()),
        arrow::field("size_sum", arrow::uint64()),
      });
      
      std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
      auto empty_table = arrow::Table::Make(schema, empty_arrays);
      
      std::string parquet_file = view_path + "/part.0.parquet";
      auto outfile_result = arrow::io::FileOutputStream::Open(parquet_file);
      if (!outfile_result.ok()) {
        spdlog::error("Failed to open parquet file {}: {}", parquet_file, outfile_result.status().ToString());
        return;
      }
      auto outfile = *outfile_result;
      
      parquet::WriterProperties::Builder properties_builder;
      properties_builder.compression(parquet::Compression::UNCOMPRESSED);
      auto properties = properties_builder.build();
      
      auto writer_result = parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), outfile, properties);
      if (!writer_result.ok()) {
        spdlog::error("Failed to create parquet writer: {}", writer_result.status().ToString());
        return;
      }
      auto writer = std::move(*writer_result);
      
      auto write_status = writer->WriteTable(*empty_table);
      if (!write_status.ok()) {
        spdlog::error("Failed to write empty table to parquet: {}", write_status.ToString());
        return;
      }
      
      auto close_status = writer->Close();
      if (!close_status.ok()) {
        spdlog::error("Failed to close parquet writer: {}", close_status.ToString());
        return;
      }
      
      auto outfile_close_status = outfile->Close();
      if (!outfile_close_status.ok()) {
        spdlog::error("Failed to close output file: {}", outfile_close_status.ToString());
        return;
      }
      
      std::ofstream metadata(view_path + "/_metadata");
      metadata << "checkpoint_name=" << name << std::endl;
      metadata << "timestamp=" << std::time(nullptr) << std::endl;
      metadata.close();
      
      spdlog::info("Stored empty view to checkpoint directory: {}", view_path);
      return;
    }

    // Build comprehensive schema based on first record
    const auto& first_hlm = view[0];
    std::vector<std::shared_ptr<arrow::Field>> schema_fields;
    
    // Add basic fields
    schema_fields.push_back(arrow::field("time_sum", arrow::float64()));
    schema_fields.push_back(arrow::field("count_sum", arrow::uint64()));  
    schema_fields.push_back(arrow::field("size_sum", arrow::uint64()));
    
    // Add bin fields
    std::vector<std::string> bin_field_names;
    for (const auto& [bin_name, _] : first_hlm.bin_sums) {
      schema_fields.push_back(arrow::field(bin_name, arrow::uint32()));
      bin_field_names.push_back(bin_name);
    }
    
    // Add group value fields (for multi-index)
    std::vector<std::string> group_field_names;
    for (const auto& [group_name, _] : first_hlm.group_values) {
      schema_fields.push_back(arrow::field(group_name, arrow::utf8()));
      group_field_names.push_back(group_name);
    }
    
    // Add unique set fields (as string representation)
    std::vector<std::string> unique_set_field_names;
    for (const auto& [set_name, _] : first_hlm.unique_sets) {
      schema_fields.push_back(arrow::field(set_name, arrow::utf8()));
      unique_set_field_names.push_back(set_name);
    }

    auto schema = arrow::schema(schema_fields);

    // Convert metrics to Arrow arrays
    arrow::DoubleBuilder time_builder;
    arrow::UInt64Builder count_builder;
    arrow::UInt64Builder size_builder;
    
    std::unordered_map<std::string, std::unique_ptr<arrow::UInt32Builder>> bin_builders;
    for (const auto& bin_name : bin_field_names) {
      bin_builders[bin_name] = std::make_unique<arrow::UInt32Builder>();
    }
    
    std::unordered_map<std::string, std::unique_ptr<arrow::StringBuilder>> group_builders;
    for (const auto& group_name : group_field_names) {
      group_builders[group_name] = std::make_unique<arrow::StringBuilder>();
    }
    
    std::unordered_map<std::string, std::unique_ptr<arrow::StringBuilder>> unique_set_builders;
    for (const auto& set_name : unique_set_field_names) {
      unique_set_builders[set_name] = std::make_unique<arrow::StringBuilder>();
    }

    for (const auto& hlm : view) {
      time_builder.Append(hlm.time_sum);
      count_builder.Append(hlm.count_sum);
      size_builder.Append(hlm.size_sum);
      
      // Add bin values
      for (const auto& bin_name : bin_field_names) {
        auto it = hlm.bin_sums.find(bin_name);
        uint32_t value = (it != hlm.bin_sums.end()) ? it->second : 0;
        bin_builders[bin_name]->Append(value);
      }
      
      // Add group values  
      for (const auto& group_name : group_field_names) {
        auto it = hlm.group_values.find(group_name);
        std::string value = (it != hlm.group_values.end()) ? it->second : "";
        group_builders[group_name]->Append(value);
      }
      
      // Add unique sets as comma-separated strings
      for (const auto& set_name : unique_set_field_names) {
        auto it = hlm.unique_sets.find(set_name);
        std::string value = "";
        if (it != hlm.unique_sets.end() && !it->second.empty()) {
          std::ostringstream oss;
          bool first = true;
          for (const auto& item : it->second) {
            if (!first) oss << ",";
            oss << item;
            first = false;
          }
          value = oss.str();
        }
        unique_set_builders[set_name]->Append(value);
      }
    }

    // Build arrays
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    
    std::shared_ptr<arrow::Array> time_array;
    std::shared_ptr<arrow::Array> count_array;
    std::shared_ptr<arrow::Array> size_array;

    time_builder.Finish(&time_array);
    count_builder.Finish(&count_array);  
    size_builder.Finish(&size_array);
    
    arrays.push_back(time_array);
    arrays.push_back(count_array);
    arrays.push_back(size_array);
    
    for (const auto& bin_name : bin_field_names) {
      std::shared_ptr<arrow::Array> bin_array;
      bin_builders[bin_name]->Finish(&bin_array);
      arrays.push_back(bin_array);
    }
    
    for (const auto& group_name : group_field_names) {
      std::shared_ptr<arrow::Array> group_array;
      group_builders[group_name]->Finish(&group_array);
      arrays.push_back(group_array);
    }
    
    for (const auto& set_name : unique_set_field_names) {
      std::shared_ptr<arrow::Array> set_array;
      unique_set_builders[set_name]->Finish(&set_array);
      arrays.push_back(set_array);
    }

    // Create Arrow table
    auto table = arrow::Table::Make(schema, arrays);

    // Write to parquet directory with metadata (similar to Python implementation)
    std::string parquet_file = view_path + "/part.0.parquet";
    auto outfile_result = arrow::io::FileOutputStream::Open(parquet_file);
    if (!outfile_result.ok()) {
      spdlog::error("Failed to open parquet file {}: {}", parquet_file, outfile_result.status().ToString());
      return;
    }
    auto outfile = *outfile_result;

    parquet::WriterProperties::Builder properties_builder;
    properties_builder.compression(parquet::Compression::SNAPPY);
    auto properties = properties_builder.build();

    auto writer_result = parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), outfile, properties);
    if (!writer_result.ok()) {
      spdlog::error("Failed to create parquet writer: {}", writer_result.status().ToString());
      return;
    }
    auto writer = std::move(*writer_result);
    
    auto write_status = writer->WriteTable(*table);
    if (!write_status.ok()) {
      spdlog::error("Failed to write table to parquet: {}", write_status.ToString());
      return;
    }
    
    auto close_status = writer->Close();
    if (!close_status.ok()) {
      spdlog::error("Failed to close parquet writer: {}", close_status.ToString());
      return;
    }
    
    auto outfile_close_status = outfile->Close();
    if (!outfile_close_status.ok()) {
      spdlog::error("Failed to close output file: {}", outfile_close_status.ToString());
      return;
    }

    // Create _metadata file to mark checkpoint as complete (similar to Python)
    std::ofstream metadata(view_path + "/_metadata");
    metadata << "checkpoint_name=" << name << std::endl;
    metadata << "timestamp=" << std::time(nullptr) << std::endl;
    metadata.close();

    spdlog::info("Stored view to checkpoint directory: {}", view_path);

  } catch (const std::exception& e) {
    spdlog::error("Failed to store view {}: {}", name, e.what());
  }
}

std::vector<HighLevelMetrics> DFTracerAnalyzer::load_view_from_parquet(const std::string& path) {
  std::vector<HighLevelMetrics> metrics;
  
  try {
    // Look for parquet files in the checkpoint directory
    std::string parquet_file = path + "/part.0.parquet";
    
    auto infile_result = arrow::io::ReadableFile::Open(parquet_file, arrow::default_memory_pool());
    if (!infile_result.ok()) {
      return {};
    }
    auto infile = *infile_result;

    auto reader_result = parquet::arrow::OpenFile(infile, arrow::default_memory_pool());
    if (!reader_result.ok()) {
      return {};
    }
    auto reader = std::move(*reader_result);

    std::shared_ptr<arrow::Table> table;
    reader->ReadTable(&table);

    // Convert Arrow table back to HighLevelMetrics
    auto time_column = table->GetColumnByName("time_sum");
    auto count_column = table->GetColumnByName("count_sum");
    auto size_column = table->GetColumnByName("size_sum");

    if (time_column && count_column && size_column) {
      auto time_array = std::static_pointer_cast<arrow::DoubleArray>(time_column->chunk(0));
      auto count_array = std::static_pointer_cast<arrow::UInt64Array>(count_column->chunk(0));
      auto size_array = std::static_pointer_cast<arrow::UInt64Array>(size_column->chunk(0));

      for (int64_t i = 0; i < table->num_rows(); ++i) {
        HighLevelMetrics hlm;
        hlm.time_sum = time_array->Value(i);
        hlm.count_sum = count_array->Value(i);
        hlm.size_sum = size_array->Value(i);
        // TODO: Load bin_sums and unique_sets maps
        metrics.push_back(hlm);
      }
    }

    spdlog::info("Loaded {} HighLevelMetrics from checkpoint directory: {}", metrics.size(), path);

  } catch (const std::exception& e) {
    spdlog::error("Failed to load view from {}: {}", path, e.what());
  }
  
  return metrics;
}

}  // namespace analyzers
}  // namespace dftracer
