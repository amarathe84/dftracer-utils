#include <dftracer/utils/analyzers/analyzer.h>
#include <dftracer/utils/utils/filesystem.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>

using namespace dftracer::utils::json;

namespace dftracer {
namespace utils {
namespace analyzers {

namespace constants {
const std::unordered_map<std::string, uint8_t> IO_CAT_TO_CODE = {
    {"read", 0}, {"write", 1}, {"metadata", 2}, {"other", 3}};

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
}  // namespace constants

namespace helpers {

uint64_t calc_time_range(uint64_t time, double time_granularity) {
  if (time_granularity <= 0.0) return 0;
  return static_cast<uint64_t>(static_cast<double>(time) / time_granularity);
}

static std::string derive_io_cat(const std::string& func_name) {
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

static bool should_ignore_event(const std::string& func_name) {
  if (constants::IGNORED_FUNC_NAMES.find(func_name) !=
      constants::IGNORED_FUNC_NAMES.end()) {
    return true;
  }

  for (const auto& pattern : constants::IGNORED_FUNC_PATTERNS) {
    if (func_name.find(pattern) != std::string::npos) {
      return true;
    }
  }

  return false;
}

static std::ptrdiff_t get_size_bin_index(uint64_t size) {
  double size_double = static_cast<double>(size);

  auto it = std::upper_bound(constants::SIZE_BINS.begin(),
                             constants::SIZE_BINS.end(), size_double);
  std::ptrdiff_t bin_index =
      std::distance(constants::SIZE_BINS.begin(), it) - 1;

  // Adjust to match Python's bin placement (shift one bin earlier)
  if (bin_index > 0) {
    bin_index = bin_index - 1;
  }

  bin_index =
      std::max(static_cast<std::ptrdiff_t>(0),
               std::min(bin_index, static_cast<std::ptrdiff_t>(
                                       constants::SIZE_BIN_SUFFIXES.size()) -
                                       1));

  return bin_index;
}

static void set_size_bins(TraceRecord& record) {
  // Initialize all bins as nullopt first to mimic NaN
  for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
    std::string bin_name = constants::SIZE_BIN_PREFIX + suffix;
    record.bin_fields[bin_name] = std::nullopt;
  }

  if (record.size.has_value() && record.size.value() > 0) {
    size_t bin_index =
        static_cast<size_t>(get_size_bin_index(record.size.value()));
    std::string matching_bin =
        constants::SIZE_BIN_PREFIX + constants::SIZE_BIN_SUFFIXES[bin_index];
    record.bin_fields[matching_bin] = 1;
  }
}

std::optional<TraceRecord> parse_trace_record(
    const dftracer::utils::json::OwnedJsonDocument& doc) {
  using namespace dftracer::utils::json;

  TraceRecord record = {};

  try {
    if (!doc.is_object()) {
      return std::nullopt;
    }

    std::string func_name = get_string_field_owned(doc, "name");
    std::string phase = get_string_field_owned(doc, "ph");

    if (should_ignore_event(func_name)) {
      return std::nullopt;
    }

    record.func_name = func_name;

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
        record.event_type = 1;  // file hash
        record.func_name = get_args_string_field_owned(doc, "name");
        record.fhash = get_args_string_field_owned(doc, "value");
      } else if (func_name == "HH") {
        record.event_type = 2;  // host hash
        record.func_name = get_args_string_field_owned(doc, "name");
        record.hhash = get_args_string_field_owned(doc, "value");
      } else if (func_name == "SH") {
        record.event_type = 3;  // string hash
        record.func_name = get_args_string_field_owned(doc, "name");
        // Store hash in fhash field for simplicity
        record.fhash = get_args_string_field_owned(doc, "value");
      } else if (func_name == "PR") {
        record.event_type = 5;  // process metadata
        record.func_name = get_args_string_field_owned(doc, "name");
        record.fhash = get_args_string_field_owned(doc, "value");
      } else {
        record.event_type = 4;  // other metadata
        record.func_name = get_args_string_field_owned(doc, "name");
        record.fhash = get_args_string_field_owned(doc, "value");
      }
    } else {
      // Regular event (type = 0)
      record.event_type = 0;

      // Extract duration and timestamp
      record.duration = get_double_field_owned(doc, "dur");
      record.time_start = get_uint64_field_owned(doc, "ts");
      record.time_end =
          record.time_start + static_cast<uint64_t>(record.duration);
      record.count = 1;

      // this will be recalculated later
      record.time_range = 0;

      // Extract IO-related fields
      record.fhash = get_args_string_field_owned(doc, "fhash");

      if (record.cat == "posix" || record.cat == "stdio") {
        record.io_cat = derive_io_cat(func_name);

        // Get ret value directly as numeric from args
        auto obj_result = doc.get_object();
        if (!obj_result.error()) {
          auto obj = obj_result.value();
          for (auto field : obj) {
            std::string field_key = std::string(field.key);
            if (field_key == "args" && field.value.is_object()) {
              auto args_result = field.value.get_object();
              if (!args_result.error()) {
                auto args = args_result.value();
                for (auto arg_field : args) {
                  std::string arg_key = std::string(arg_field.key);
                  if (arg_key == "ret") {
                    uint64_t ret_value = 0;
                    if (arg_field.value.is_uint64()) {
                      ret_value = arg_field.value.get_uint64();
                    } else if (arg_field.value.is_int64()) {
                      int64_t signed_ret = arg_field.value.get_int64();
                      if (signed_ret > 0) {
                        ret_value = static_cast<uint64_t>(signed_ret);
                      }
                    }

                    if (ret_value > 0 &&
                        (record.io_cat == "read" || record.io_cat == "write")) {
                      record.size = ret_value;
                    }
                    break;
                  }
                }
              }
              break;
            }
          }
        }

        std::string offset_str = get_args_string_field_owned(doc, "offset");
        if (!offset_str.empty()) {
          try {
            record.offset = std::stoull(offset_str);
          } catch (...) {
            // Ignore parse errors - offset remains nullopt
          }
        }
      } else {
        record.io_cat = "other";

        // Extract image_id for non-POSIX events
        std::string image_idx_str =
            get_args_string_field_owned(doc, "image_idx");
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
          record.epoch = 0;  // Default if parsing fails
        }
      }

      // Set size bins
      set_size_bins(record);
    }

  } catch (const std::exception& e) {
    spdlog::debug("Exception in parse_trace_record: {}", e.what());
    // Return empty record on error
    return std::nullopt;
  }

  return record;
}

static std::vector<std::string> generate_size_bins_vec() {
  std::vector<std::string> size_bins;
  for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
    size_bins.push_back(constants::SIZE_BIN_PREFIX + suffix);
  }
  return size_bins;
}

static std::string generate_size_bin_headers() {
  std::ostringstream header_stream;
  for (size_t i = 0; i < constants::SIZE_BIN_SUFFIXES.size(); ++i) {
    if (i > 0) header_stream << ",";
    header_stream << constants::SIZE_BIN_PREFIX
                  << constants::SIZE_BIN_SUFFIXES[i];
  }
  return header_stream.str();
}

std::string hlms_to_csv(const std::vector<HighLevelMetrics>& hlms,
                        bool header) {
  std::ostringstream csv_stream;

  // CSV Header
  if (header) {
    csv_stream << "proc_name,cat,epoch,acc_pat,func_name,io_cat,time_range,"
               << "time,count,size," << generate_size_bin_headers();
    csv_stream << std::endl;
  }

  // CSV Data rows
  for (const auto& hlm : hlms) {
    // Get basic fields
    std::string cat =
        hlm.group_values.count("cat") ? hlm.group_values.at("cat") : "";
    std::string acc_pat =
        hlm.group_values.count("acc_pat") ? hlm.group_values.at("acc_pat") : "";
    std::string epoch =
        hlm.group_values.count("epoch") ? hlm.group_values.at("epoch") : "";
    std::string io_cat =
        hlm.group_values.count("io_cat") ? hlm.group_values.at("io_cat") : "";
    std::string func_name = hlm.group_values.count("func_name")
                                ? hlm.group_values.at("func_name")
                                : "";
    std::string proc_name = hlm.group_values.count("proc_name")
                                ? hlm.group_values.at("proc_name")
                                : "";
    std::string time_range = hlm.group_values.count("time_range")
                                 ? hlm.group_values.at("time_range")
                                 : "";

    // Output row with proper CSV formatting
    csv_stream << proc_name << "," << cat << "," << epoch << "," << acc_pat
               << "," << func_name << "," << io_cat << "," << time_range << ","
               << std::fixed << std::setprecision(6) << hlm.time_sum
               << std::defaultfloat << "," << hlm.count_sum << ",";

    // Handle optional size_sum (nullopt -> empty string for NaN)
    if (hlm.size_sum.has_value()) {
      csv_stream << hlm.size_sum.value();
    }

    auto size_bins = generate_size_bins_vec();
    for (size_t i = 0; i < size_bins.size(); ++i) {
      csv_stream << ",";
      if (hlm.bin_sums.count(size_bins[i]) &&
          hlm.bin_sums.at(size_bins[i]).has_value()) {
        csv_stream << hlm.bin_sums.at(size_bins[i]).value();
      }
      // else output empty string for nullopt (NaN equivalent)
    }

    csv_stream << std::endl;
  }
  return csv_stream.str();
}
}  // namespace helpers

Analyzer::Analyzer(double time_granularity, double time_resolution,
                   size_t checkpoint_size, bool checkpoint,
                   const std::string& checkpoint_dir)
    : time_granularity_(time_granularity),
      time_resolution_(time_resolution),
      checkpoint_size_(checkpoint_size),
      checkpoint_dir_(checkpoint_dir),
      checkpoint_(checkpoint) {}

std::string Analyzer::get_checkpoint_path(const std::string& name) const {
  return checkpoint_dir_ + "/" + name;
}

std::string Analyzer::get_checkpoint_name(
    const std::vector<std::string>& args) const {
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

}  // namespace analyzers
}  // namespace utils
}  // namespace dftracer
