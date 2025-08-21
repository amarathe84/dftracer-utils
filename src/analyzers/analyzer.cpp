#include <dftracer/utils/analyzers/analyzer.h>
#include <dftracer/utils/utils/filesystem.h>

#include <spdlog/spdlog.h>

#include <stdexcept>
#include <algorithm>
#include <fstream>
#include <limits>
#include <memory>
#include <sstream>

using namespace dftracer::utils::json;

namespace dftracer {
namespace utils {
namespace analyzers {

namespace constants {
const std::unordered_map<std::string, uint8_t> IO_CAT_TO_CODE = {
  {"read", 0},
  {"write", 1}, 
  {"metadata", 2},
  {"other", 3}
};

const std::unordered_map<std::string, std::string> POSIX_IO_CAT_MAPPING = {
    {"read", "read"},       {"pread", "read"},     {"pread64", "read"},
    {"readv", "read"},      {"preadv", "read"},    {"write", "write"},
    {"pwrite", "write"},    {"pwrite64", "write"}, {"writev", "write"},
    {"pwritev", "write"},   {"open", "open"},      {"open64", "open"},
    {"openat", "open"},     {"close", "close"},    {"__xstat64", "stat"},
    {"__lxstat64", "stat"}, {"stat", "stat"},      {"lstat", "stat"},
    {"fstat", "stat"}};

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
}

namespace helpers {
uint8_t encode_io_cat(const std::string& io_cat_str) {
  auto it = constants::IO_CAT_TO_CODE.find(io_cat_str);
  return (it != constants::IO_CAT_TO_CODE.end()) ? it->second : 3; // default to "other"
}

std::string derive_io_cat(const std::string& func_name) {
  if (constants::POSIX_METADATA_FUNCTIONS.find(func_name) !=
      constants::POSIX_METADATA_FUNCTIONS.end()) {
    return "metadata";
  }

  auto it = constants::POSIX_IO_CAT_MAPPING.find(func_name);
  if (it != constants::POSIX_IO_CAT_MAPPING.end()) {
    return it->second;
  }

  return "other";
}

bool should_ignore_event(const std::string& func_name,
                         const std::string& phase) {
  if (phase == "M") return true;

  if (constants::IGNORED_FUNC_NAMES.find(func_name) != constants::IGNORED_FUNC_NAMES.end()) {
    return true;
  }

  for (const auto& pattern : constants::IGNORED_FUNC_PATTERNS) {
    if (func_name.find(pattern) != std::string::npos) {
      return true;
    }
  }

  return false;
}

// Calculate size bin using Python pd.cut logic
int get_size_bin_index(uint64_t size) {
  double size_double = static_cast<double>(size);

  auto it = std::upper_bound(constants::SIZE_BINS.begin(), constants::SIZE_BINS.end(), size_double);
  int bin_index = std::distance(constants::SIZE_BINS.begin(), it) - 1;
  bin_index = std::max(
      0, std::min(bin_index, static_cast<int>(constants::SIZE_BIN_SUFFIXES.size()) - 1));

  return bin_index;
}

void set_size_bins(TraceRecord& record) {
  if (record.size > 0) {
    int bin_index = get_size_bin_index(record.size);

    for (size_t i = 0; i < constants::SIZE_BIN_SUFFIXES.size(); ++i) {
      std::string bin_name = "size_bin_" + constants::SIZE_BIN_SUFFIXES[i];
      record.bin_fields[bin_name] =
          (i == static_cast<size_t>(bin_index)) ? 1 : 0;
    }
  } else {
    for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
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
} // namespace helpers

Analyzer::Analyzer(double time_granularity,
                   double time_resolution,
                   size_t checkpoint_size,
                   bool checkpoint,
                   const std::string& checkpoint_dir)
    : time_granularity_(time_granularity),
      time_resolution_(time_resolution),
      checkpoint_size_(checkpoint_size),
      checkpoint_(checkpoint),
      checkpoint_dir_(checkpoint_dir) {}

// Template implementation moved to analyzer_impl.h

Bag<HighLevelMetrics> Analyzer::compute_high_level_metrics(
    const std::vector<TraceRecord>& records,
    const std::vector<std::string>& view_types,
    const std::string& partition_size,
    const std::string& checkpoint_name
) {
    // @TODO: implement this
    return from_sequence<HighLevelMetrics>({});
}

Bag<TraceRecord> Analyzer::read_trace(
    const std::string& trace_path,
    const std::unordered_map<std::string, std::string>& extra_columns
) {
    // @TODO: implement this
    return from_sequence<TraceRecord>({});
}

Bag<TraceRecord> Analyzer::postread_trace(
    const std::vector<TraceRecord>& traces,
    const std::vector<std::string>& view_types
) {
    // @TODO: implement this
    return from_sequence(traces);
}

std::string Analyzer::get_checkpoint_path(const std::string& name) const {
  return checkpoint_dir_ + "/" + name;
}

std::string Analyzer::get_checkpoint_name(const std::vector<std::string>& args) const {
  std::ostringstream name_stream;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0) name_stream << "_";
    name_stream << args[i];
  }
  name_stream << "_" << static_cast<int>(time_granularity_);
  return name_stream.str();
}

bool Analyzer::has_checkpoint(const std::string& name) const {
  std::string checkpoint_path = get_checkpoint_path(name);
  std::string metadata_path = checkpoint_path + "/_checkpoint_metadata";
  return fs::exists(metadata_path);
}

} // namespace analyzers
} // namespace utils
} // namespace dftracer
