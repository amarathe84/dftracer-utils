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

uint64_t calc_time_range(uint64_t time, double time_granularity) {
  return static_cast<uint64_t>(std::floor(time / time_granularity));
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
  // Don't filter out metadata events (ph == "M") - they need to be processed!
  // if (phase == "M") return true;

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

static void set_size_bins(TraceRecord& record) {
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

std::optional<TraceRecord> parse_trace_record(const dftracer::utils::json::OwnedJsonDocument& doc,
                               const std::vector<std::string>& view_types,
                               double time_granularity) {
  using namespace dftracer::utils::json;

  TraceRecord record = {};  // Initialize all fields to zero/empty

  try {
    if (!doc.is_object()) { return std::nullopt; }
  
    std::string func_name = get_string_field_owned(doc, "name");
    std::string phase = get_string_field_owned(doc, "ph");

    if (should_ignore_event(func_name, phase)) {
      return std::nullopt;
    }

    // Get basic fields that all events have
    record.func_name = func_name;
    
    // Debug logging for first few records
    static size_t parse_count = 0;
    parse_count++;
    if (parse_count <= 10) {
      spdlog::debug("RAY DEBUG: Parsing record #{}: name='{}', ph='{}', cat='{}'", 
                   parse_count, func_name, phase, get_string_field_owned(doc, "cat"));
    }

    auto obj = doc.get_object().value();
    
    // Extract cat field
    std::string cat = get_string_field_owned(doc, "cat");
    if (!cat.empty()) {
      std::transform(cat.begin(), cat.end(), cat.begin(), ::tolower);
      record.cat = cat;
    }
    
    // Extract pid and tid
    record.pid = get_uint64_field_owned(doc, "pid");
    record.tid = get_uint64_field_owned(doc, "tid");

    // Extract hhash from args if available
    record.hhash = get_args_string_field_owned(doc, "hhash");

    // Handle metadata events (phase == "M")
    if (phase == "M") {
      if (func_name == "FH") {
        record.event_type = 1; // file hash
        record.func_name = get_args_string_field_owned(doc, "name");
        record.fhash = get_args_string_field_owned(doc, "value");
      } else if (func_name == "HH") {
        record.event_type = 2; // host hash
        record.func_name = get_args_string_field_owned(doc, "name");
        record.hhash = get_args_string_field_owned(doc, "value");
      } else if (func_name == "SH") {
        record.event_type = 3; // string hash
        record.func_name = get_args_string_field_owned(doc, "name");
        // Store hash in fhash field for simplicity
        record.fhash = get_args_string_field_owned(doc, "value");
      } else if (func_name == "PR") {
        record.event_type = 5; // process metadata
        record.func_name = get_args_string_field_owned(doc, "name");
        record.fhash = get_args_string_field_owned(doc, "value");
      } else {
        record.event_type = 4; // other metadata
        record.func_name = get_args_string_field_owned(doc, "name");
        record.fhash = get_args_string_field_owned(doc, "value");
      }
    } else {
      // Regular event (type = 0)
      record.event_type = 0;
      
      // Extract duration and timestamp
      record.duration = get_double_field_owned(doc, "dur");
      record.time_start = get_uint64_field_owned(doc, "ts");
      record.time_end = record.time_start + static_cast<uint64_t>(record.duration);
      record.count = 1;

      // Don't calculate time_range here - will be calculated after timestamp normalization
      // to match Python's behavior: ts = ts - min(ts), then trange = ts // granularity
      record.time_range = 0; // Will be recalculated later

      // Extract IO-related fields
      record.fhash = get_args_string_field_owned(doc, "fhash");
      record.size = 0;
      
      if (record.cat == "posix" || record.cat == "stdio") {
        record.io_cat = derive_io_cat(func_name);
        
        std::string ret_str = get_args_string_field_owned(doc, "ret");
        if (!ret_str.empty()) {
          try {
            uint64_t ret_value = std::stoull(ret_str);
            if (ret_value > 0 && (record.io_cat == "read" || record.io_cat == "write")) {
              record.size = ret_value;
            }
          } catch (...) {
            // Ignore parse errors
          }
        }

        std::string offset_str = get_args_string_field_owned(doc, "offset");
        if (!offset_str.empty()) {
          try {
            record.offset = std::stoull(offset_str);
          } catch (...) {
            // Ignore parse errors
          }
        }
      } else {
        record.io_cat = "other";
        
        // Extract image_id for non-POSIX events
        std::string image_idx_str = get_args_string_field_owned(doc, "image_idx");
        if (!image_idx_str.empty()) {
          try {
            record.image_id = std::stoull(image_idx_str);
          } catch (...) {
            // Ignore parse errors
          }
        }
      }

      record.acc_pat = "0";
      
      // Extract epoch from args if available
      std::string epoch_str = get_args_string_field_owned(doc, "epoch");
      if (!epoch_str.empty()) {
        try {
          record.epoch = std::stoull(epoch_str);
        } catch (...) {
          record.epoch = 0; // Default if parsing fails
        }
      }

      // Set size bins for regular events
      set_size_bins(record);
    }

  } catch (const std::exception& e) {
    spdlog::debug("Exception in parse_trace_record: {}", e.what());
    // Return empty record on error
    return std::nullopt;
  }

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
