// #include <arrow/api.h>
// #include <arrow/io/api.h>
#include <dftracer/utils/analyzers/analyzer.h>
#include <dftracer/utils/utils/filesystem.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>

using namespace dftracer::utils::json;

namespace dftracer::utils::analyzers {

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

arrow::Status hlms_to_parquet(const std::vector<HighLevelMetrics>& hlms,
                              const std::string& output_path) {
  if (hlms.empty()) {
    return arrow::Status::OK();
  }

  // Build column arrays
  arrow::StringBuilder proc_name_builder, cat_builder, epoch_builder,
      acc_pat_builder, func_name_builder, io_cat_builder, time_range_builder;
  arrow::DoubleBuilder time_builder;
  arrow::UInt64Builder count_builder;
  arrow::UInt64Builder size_builder;

  // Size bin builders
  std::vector<arrow::UInt32Builder> size_bin_builders(
      constants::SIZE_BIN_SUFFIXES.size());

  for (const auto& hlm : hlms) {
    // Basic fields
    std::string proc_name = hlm.group_values.count("proc_name")
                                ? hlm.group_values.at("proc_name")
                                : "";
    std::string cat =
        hlm.group_values.count("cat") ? hlm.group_values.at("cat") : "";
    std::string epoch =
        hlm.group_values.count("epoch") ? hlm.group_values.at("epoch") : "";
    std::string acc_pat =
        hlm.group_values.count("acc_pat") ? hlm.group_values.at("acc_pat") : "";
    std::string func_name = hlm.group_values.count("func_name")
                                ? hlm.group_values.at("func_name")
                                : "";
    std::string io_cat =
        hlm.group_values.count("io_cat") ? hlm.group_values.at("io_cat") : "";
    std::string time_range = hlm.group_values.count("time_range")
                                 ? hlm.group_values.at("time_range")
                                 : "";

    ARROW_RETURN_NOT_OK(proc_name_builder.Append(proc_name));
    ARROW_RETURN_NOT_OK(cat_builder.Append(cat));
    ARROW_RETURN_NOT_OK(epoch_builder.Append(epoch));
    ARROW_RETURN_NOT_OK(acc_pat_builder.Append(acc_pat));
    ARROW_RETURN_NOT_OK(func_name_builder.Append(func_name));
    ARROW_RETURN_NOT_OK(io_cat_builder.Append(io_cat));
    ARROW_RETURN_NOT_OK(time_range_builder.Append(time_range));

    ARROW_RETURN_NOT_OK(time_builder.Append(hlm.time_sum));
    ARROW_RETURN_NOT_OK(count_builder.Append(hlm.count_sum));

    // Handle optional size_sum
    if (hlm.size_sum.has_value()) {
      ARROW_RETURN_NOT_OK(size_builder.Append(hlm.size_sum.value()));
    } else {
      ARROW_RETURN_NOT_OK(size_builder.AppendNull());
    }

    // Handle size bins
    auto size_bins = generate_size_bins_vec();
    for (size_t i = 0; i < size_bins.size(); ++i) {
      if (hlm.bin_sums.count(size_bins[i]) &&
          hlm.bin_sums.at(size_bins[i]).has_value()) {
        ARROW_RETURN_NOT_OK(
            size_bin_builders[i].Append(hlm.bin_sums.at(size_bins[i]).value()));
      } else {
        ARROW_RETURN_NOT_OK(size_bin_builders[i].AppendNull());
      }
    }
  }

  // Build arrays
  std::shared_ptr<arrow::Array> proc_name_array, cat_array, epoch_array,
      acc_pat_array, func_name_array, io_cat_array, time_range_array,
      time_array, count_array, size_array;

  ARROW_RETURN_NOT_OK(proc_name_builder.Finish(&proc_name_array));
  ARROW_RETURN_NOT_OK(cat_builder.Finish(&cat_array));
  ARROW_RETURN_NOT_OK(epoch_builder.Finish(&epoch_array));
  ARROW_RETURN_NOT_OK(acc_pat_builder.Finish(&acc_pat_array));
  ARROW_RETURN_NOT_OK(func_name_builder.Finish(&func_name_array));
  ARROW_RETURN_NOT_OK(io_cat_builder.Finish(&io_cat_array));
  ARROW_RETURN_NOT_OK(time_range_builder.Finish(&time_range_array));
  ARROW_RETURN_NOT_OK(time_builder.Finish(&time_array));
  ARROW_RETURN_NOT_OK(count_builder.Finish(&count_array));
  ARROW_RETURN_NOT_OK(size_builder.Finish(&size_array));

  // Build size bin arrays
  std::vector<std::shared_ptr<arrow::Array>> size_bin_arrays(
      constants::SIZE_BIN_SUFFIXES.size());
  for (size_t i = 0; i < size_bin_builders.size(); ++i) {
    ARROW_RETURN_NOT_OK(size_bin_builders[i].Finish(&size_bin_arrays[i]));
  }

  // Create schema
  std::vector<std::shared_ptr<arrow::Field>> fields = {
      arrow::field("proc_name", arrow::utf8()),
      arrow::field("cat", arrow::utf8()),
      arrow::field("epoch", arrow::utf8()),
      arrow::field("acc_pat", arrow::utf8()),
      arrow::field("func_name", arrow::utf8()),
      arrow::field("io_cat", arrow::utf8()),
      arrow::field("time_range", arrow::utf8()),
      arrow::field("time", arrow::float64()),
      arrow::field("count", arrow::uint64()),
      arrow::field("size", arrow::uint64())};

  // Add size bin fields
  for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
    fields.push_back(
        arrow::field(constants::SIZE_BIN_PREFIX + suffix, arrow::uint32()));
  }

  auto schema = arrow::schema(fields);

  // Create arrays vector
  std::vector<std::shared_ptr<arrow::Array>> arrays = {
      proc_name_array, cat_array,    epoch_array,      acc_pat_array,
      func_name_array, io_cat_array, time_range_array, time_array,
      count_array,     size_array};

  // Add size bin arrays
  arrays.insert(arrays.end(), size_bin_arrays.begin(), size_bin_arrays.end());

  // Create table
  auto table = arrow::Table::Make(schema, arrays);

  // Write to parquet file
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile,
                        arrow::io::FileOutputStream::Open(output_path));

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, /*chunk_size=*/1024));

  return arrow::Status::OK();
}

arrow::Result<std::vector<HighLevelMetrics>> hlms_from_parquet(
    const std::string& input_path) {
  // Read parquet file
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(input_path));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(
      reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  std::vector<HighLevelMetrics> hlms;
  int64_t num_rows = table->num_rows();

  if (num_rows == 0) {
    return hlms;
  }

  // Get column arrays
  auto proc_name_array = std::static_pointer_cast<arrow::StringArray>(
      table->GetColumnByName("proc_name")->chunk(0));
  auto cat_array = std::static_pointer_cast<arrow::StringArray>(
      table->GetColumnByName("cat")->chunk(0));
  auto epoch_array = std::static_pointer_cast<arrow::StringArray>(
      table->GetColumnByName("epoch")->chunk(0));
  auto acc_pat_array = std::static_pointer_cast<arrow::StringArray>(
      table->GetColumnByName("acc_pat")->chunk(0));
  auto func_name_array = std::static_pointer_cast<arrow::StringArray>(
      table->GetColumnByName("func_name")->chunk(0));
  auto io_cat_array = std::static_pointer_cast<arrow::StringArray>(
      table->GetColumnByName("io_cat")->chunk(0));
  auto time_range_array = std::static_pointer_cast<arrow::StringArray>(
      table->GetColumnByName("time_range")->chunk(0));
  auto time_array = std::static_pointer_cast<arrow::DoubleArray>(
      table->GetColumnByName("time")->chunk(0));
  auto count_array = std::static_pointer_cast<arrow::UInt64Array>(
      table->GetColumnByName("count")->chunk(0));
  auto size_array = std::static_pointer_cast<arrow::UInt64Array>(
      table->GetColumnByName("size")->chunk(0));

  // Get size bin columns
  std::vector<std::shared_ptr<arrow::UInt32Array>> size_bin_arrays;
  for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
    std::string col_name = constants::SIZE_BIN_PREFIX + suffix;
    auto col = table->GetColumnByName(col_name);
    if (col) {
      size_bin_arrays.push_back(
          std::static_pointer_cast<arrow::UInt32Array>(col->chunk(0)));
    } else {
      size_bin_arrays.push_back(nullptr);
    }
  }

  hlms.reserve(num_rows);

  for (int64_t i = 0; i < num_rows; i++) {
    HighLevelMetrics hlm;

    // Basic numeric fields
    hlm.time_sum = time_array->Value(i);
    hlm.count_sum = count_array->Value(i);

    // Handle optional size
    if (size_array->IsNull(i)) {
      hlm.size_sum = std::nullopt;
    } else {
      hlm.size_sum = size_array->Value(i);
    }

    // Group values
    hlm.group_values["proc_name"] = proc_name_array->GetString(i);
    hlm.group_values["cat"] = cat_array->GetString(i);
    hlm.group_values["epoch"] = epoch_array->GetString(i);
    hlm.group_values["acc_pat"] = acc_pat_array->GetString(i);
    hlm.group_values["func_name"] = func_name_array->GetString(i);
    hlm.group_values["io_cat"] = io_cat_array->GetString(i);
    hlm.group_values["time_range"] = time_range_array->GetString(i);

    // Handle size bins
    auto size_bins = generate_size_bins_vec();
    for (size_t j = 0; j < size_bins.size() && j < size_bin_arrays.size();
         ++j) {
      if (size_bin_arrays[j] && !size_bin_arrays[j]->IsNull(i)) {
        hlm.bin_sums[size_bins[j]] = size_bin_arrays[j]->Value(i);
      } else {
        hlm.bin_sums[size_bins[j]] = std::nullopt;
      }
    }

    hlms.push_back(std::move(hlm));
  }

  return hlms;
}
}  // namespace helpers

AnalyzerConfig::AnalyzerConfig(double time_granularity, bool checkpoint,
                               const std::string& checkpoint_dir,
                               size_t checkpoint_size, double time_resolution)
    : time_granularity_(time_granularity),
      checkpoint_(checkpoint),
      checkpoint_dir_(checkpoint_dir),
      checkpoint_size_(checkpoint_size),
      time_resolution_(time_resolution) {
  if (checkpoint_) {
    if (checkpoint_dir_.empty()) {
      throw std::invalid_argument(
          "Checkpointing is enabled but checkpoint_dir is empty.");
    }
    // Create checkpoint directory if it doesn't exist
    if (!fs::exists(checkpoint_dir_)) {
      fs::create_directories(checkpoint_dir_);
    }
  }
}

AnalyzerConfig AnalyzerConfig::Default() { return AnalyzerConfig(); }

AnalyzerConfig AnalyzerConfig::create(double time_granularity, bool checkpoint,
                                      const std::string& checkpoint_dir,
                                      size_t checkpoint_size,
                                      double time_resolution) {
  return AnalyzerConfig(time_granularity, checkpoint, checkpoint_dir,
                        checkpoint_size, time_resolution);
}

double AnalyzerConfig::time_granularity() const { return time_granularity_; }

bool AnalyzerConfig::checkpoint() const { return checkpoint_; }

const std::string& AnalyzerConfig::checkpoint_dir() const {
  return checkpoint_dir_;
}

size_t AnalyzerConfig::checkpoint_size() const { return checkpoint_size_; }

double AnalyzerConfig::time_resolution() const { return time_resolution_; }

AnalyzerConfig& AnalyzerConfig::set_time_granularity(double time_granularity) {
  time_granularity_ = time_granularity;
  return *this;
}

AnalyzerConfig& AnalyzerConfig::set_checkpoint(bool checkpoint) {
  checkpoint_ = checkpoint;
  return *this;
}

AnalyzerConfig& AnalyzerConfig::set_checkpoint_dir(
    const std::string& checkpoint_dir) {
  checkpoint_dir_ = checkpoint_dir;
  return *this;
}

AnalyzerConfig& AnalyzerConfig::set_checkpoint_size(size_t checkpoint_size) {
  checkpoint_size_ = checkpoint_size;
  return *this;
}

AnalyzerConfig& AnalyzerConfig::set_time_resolution(double time_resolution) {
  time_resolution_ = time_resolution;
  return *this;
}

Analyzer::Analyzer(double time_granularity, bool checkpoint,
                   const std::string& checkpoint_dir, size_t checkpoint_size,
                   double time_resolution)
    : config_(time_granularity, checkpoint, checkpoint_dir, checkpoint_size,
              time_resolution) {}

Analyzer::Analyzer(const AnalyzerConfig& config) : config_(config) {}

std::string Analyzer::get_checkpoint_path(const std::string& name) const {
  return config_.checkpoint_dir() + "/" + name;
}

std::string Analyzer::get_checkpoint_name(
    const std::vector<std::string>& args) const {
  std::ostringstream name_stream;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0) name_stream << "_";
    name_stream << args[i];
  }
  name_stream << "_" << static_cast<int>(config_.time_granularity());
  return name_stream.str();
}

bool Analyzer::has_checkpoint(const std::string& name) const {
  std::string checkpoint_path = get_checkpoint_path(name);
  std::string metadata_path = checkpoint_path + "/_checkpoint_metadata";
  return fs::exists(metadata_path);
}

}  // namespace dftracer
