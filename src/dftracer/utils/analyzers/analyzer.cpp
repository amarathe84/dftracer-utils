// #include <arrow/api.h>
// #include <arrow/io/api.h>
#include <dftracer/utils/analyzers/analyzer.h>
#include <dftracer/utils/analyzers/pipeline/trace_reader.h>
#include <dftracer/utils/utils/filesystem.h>
// #include <parquet/arrow/reader.h>
// #include <parquet/arrow/writer.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>

using namespace dftracer::utils::json;

namespace dftracer::utils::analyzers {

namespace helpers {
// static std::vector<std::string> generate_size_bins_vec() {
//     std::vector<std::string> size_bins;
//     for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
//         size_bins.push_back(constants::SIZE_BIN_PREFIX + suffix);
//     }
//     return size_bins;
// }

// static std::string generate_size_bin_headers() {
//     std::ostringstream header_stream;
//     for (size_t i = 0; i < constants::SIZE_BIN_SUFFIXES.size(); ++i) {
//         if (i > 0) header_stream << ",";
//         header_stream << constants::SIZE_BIN_PREFIX
//                       << constants::SIZE_BIN_SUFFIXES[i];
//     }
//     return header_stream.str();
// }

// std::string hlms_to_csv(const std::vector<HighLevelMetrics>& hlms,
//                         bool header) {
//     std::ostringstream csv_stream;

//     // CSV Header
//     if (header) {
//         csv_stream <<
//         "proc_name,cat,epoch,acc_pat,func_name,io_cat,time_range,"
//                    << "time,count,size," << generate_size_bin_headers();
//         csv_stream << std::endl;
//     }

//     // CSV Data rows
//     for (const auto& hlm : hlms) {
//         // Get basic fields
//         std::string cat =
//             hlm.group_values.count("cat") ? hlm.group_values.at("cat") : "";
//         std::string acc_pat = hlm.group_values.count("acc_pat")
//                                   ? hlm.group_values.at("acc_pat")
//                                   : "";
//         std::string epoch =
//             hlm.group_values.count("epoch") ? hlm.group_values.at("epoch") :
//             "";
//         std::string io_cat = hlm.group_values.count("io_cat")
//                                  ? hlm.group_values.at("io_cat")
//                                  : "";
//         std::string func_name = hlm.group_values.count("func_name")
//                                     ? hlm.group_values.at("func_name")
//                                     : "";
//         std::string proc_name = hlm.group_values.count("proc_name")
//                                     ? hlm.group_values.at("proc_name")
//                                     : "";
//         std::string time_range = hlm.group_values.count("time_range")
//                                      ? hlm.group_values.at("time_range")
//                                      : "";

//         // Output row with proper CSV formatting
//         csv_stream << proc_name << "," << cat << "," << epoch << "," <<
//         acc_pat
//                    << "," << func_name << "," << io_cat << "," << time_range
//                    << "," << std::fixed << std::setprecision(6) <<
//                    hlm.time_sum
//                    << std::defaultfloat << "," << hlm.count_sum << ",";

//         // Handle optional size_sum (nullopt -> empty string for NaN)
//         if (hlm.size_sum.has_value()) {
//             csv_stream << hlm.size_sum.value();
//         }

//         auto size_bins = generate_size_bins_vec();
//         for (size_t i = 0; i < size_bins.size(); ++i) {
//             csv_stream << ",";
//             if (hlm.bin_sums.count(size_bins[i]) &&
//                 hlm.bin_sums.at(size_bins[i]).has_value()) {
//                 csv_stream << hlm.bin_sums.at(size_bins[i]).value();
//             }
//             // else output empty string for nullopt (NaN equivalent)
//         }

//         csv_stream << std::endl;
//     }
//     return csv_stream.str();
// }

// arrow::Status hlms_to_parquet(const std::vector<HighLevelMetrics>& hlms,
//                               const std::string& output_path) {
//     if (hlms.empty()) {
//         return arrow::Status::OK();
//     }

//     // Build column arrays
//     arrow::StringBuilder proc_name_builder, cat_builder, epoch_builder,
//         acc_pat_builder, func_name_builder, io_cat_builder,
//         time_range_builder;
//     arrow::DoubleBuilder time_builder;
//     arrow::UInt64Builder count_builder;
//     arrow::UInt64Builder size_builder;

//     // Size bin builders
//     std::vector<arrow::UInt32Builder> size_bin_builders(
//         constants::SIZE_BIN_SUFFIXES.size());

//     for (const auto& hlm : hlms) {
//         // Basic fields
//         std::string proc_name = hlm.group_values.count("proc_name")
//                                     ? hlm.group_values.at("proc_name")
//                                     : "";
//         std::string cat =
//             hlm.group_values.count("cat") ? hlm.group_values.at("cat") : "";
//         std::string epoch =
//             hlm.group_values.count("epoch") ? hlm.group_values.at("epoch") :
//             "";
//         std::string acc_pat = hlm.group_values.count("acc_pat")
//                                   ? hlm.group_values.at("acc_pat")
//                                   : "";
//         std::string func_name = hlm.group_values.count("func_name")
//                                     ? hlm.group_values.at("func_name")
//                                     : "";
//         std::string io_cat = hlm.group_values.count("io_cat")
//                                  ? hlm.group_values.at("io_cat")
//                                  : "";
//         std::string time_range = hlm.group_values.count("time_range")
//                                      ? hlm.group_values.at("time_range")
//                                      : "";

//         ARROW_RETURN_NOT_OK(proc_name_builder.Append(proc_name));
//         ARROW_RETURN_NOT_OK(cat_builder.Append(cat));
//         ARROW_RETURN_NOT_OK(epoch_builder.Append(epoch));
//         ARROW_RETURN_NOT_OK(acc_pat_builder.Append(acc_pat));
//         ARROW_RETURN_NOT_OK(func_name_builder.Append(func_name));
//         ARROW_RETURN_NOT_OK(io_cat_builder.Append(io_cat));
//         ARROW_RETURN_NOT_OK(time_range_builder.Append(time_range));

//         ARROW_RETURN_NOT_OK(time_builder.Append(hlm.time_sum));
//         ARROW_RETURN_NOT_OK(count_builder.Append(hlm.count_sum));

//         // Handle optional size_sum
//         if (hlm.size_sum.has_value()) {
//             ARROW_RETURN_NOT_OK(size_builder.Append(hlm.size_sum.value()));
//         } else {
//             ARROW_RETURN_NOT_OK(size_builder.AppendNull());
//         }

//         // Handle size bins
//         auto size_bins = generate_size_bins_vec();
//         for (size_t i = 0; i < size_bins.size(); ++i) {
//             if (hlm.bin_sums.count(size_bins[i]) &&
//                 hlm.bin_sums.at(size_bins[i]).has_value()) {
//                 ARROW_RETURN_NOT_OK(size_bin_builders[i].Append(
//                     hlm.bin_sums.at(size_bins[i]).value()));
//             } else {
//                 ARROW_RETURN_NOT_OK(size_bin_builders[i].AppendNull());
//             }
//         }
//     }

//     // Build arrays
//     std::shared_ptr<arrow::Array> proc_name_array, cat_array, epoch_array,
//         acc_pat_array, func_name_array, io_cat_array, time_range_array,
//         time_array, count_array, size_array;

//     ARROW_RETURN_NOT_OK(proc_name_builder.Finish(&proc_name_array));
//     ARROW_RETURN_NOT_OK(cat_builder.Finish(&cat_array));
//     ARROW_RETURN_NOT_OK(epoch_builder.Finish(&epoch_array));
//     ARROW_RETURN_NOT_OK(acc_pat_builder.Finish(&acc_pat_array));
//     ARROW_RETURN_NOT_OK(func_name_builder.Finish(&func_name_array));
//     ARROW_RETURN_NOT_OK(io_cat_builder.Finish(&io_cat_array));
//     ARROW_RETURN_NOT_OK(time_range_builder.Finish(&time_range_array));
//     ARROW_RETURN_NOT_OK(time_builder.Finish(&time_array));
//     ARROW_RETURN_NOT_OK(count_builder.Finish(&count_array));
//     ARROW_RETURN_NOT_OK(size_builder.Finish(&size_array));

//     // Build size bin arrays
//     std::vector<std::shared_ptr<arrow::Array>> size_bin_arrays(
//         constants::SIZE_BIN_SUFFIXES.size());
//     for (size_t i = 0; i < size_bin_builders.size(); ++i) {
//         ARROW_RETURN_NOT_OK(size_bin_builders[i].Finish(&size_bin_arrays[i]));
//     }

//     // Create schema
//     std::vector<std::shared_ptr<arrow::Field>> fields = {
//         arrow::field("proc_name", arrow::utf8()),
//         arrow::field("cat", arrow::utf8()),
//         arrow::field("epoch", arrow::utf8()),
//         arrow::field("acc_pat", arrow::utf8()),
//         arrow::field("func_name", arrow::utf8()),
//         arrow::field("io_cat", arrow::utf8()),
//         arrow::field("time_range", arrow::utf8()),
//         arrow::field("time", arrow::float64()),
//         arrow::field("count", arrow::uint64()),
//         arrow::field("size", arrow::uint64())};

//     // Add size bin fields
//     for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
//         fields.push_back(
//             arrow::field(constants::SIZE_BIN_PREFIX + suffix,
//             arrow::uint32()));
//     }

//     auto schema = arrow::schema(fields);

//     // Create arrays vector
//     std::vector<std::shared_ptr<arrow::Array>> arrays = {
//         proc_name_array, cat_array,    epoch_array,      acc_pat_array,
//         func_name_array, io_cat_array, time_range_array, time_array,
//         count_array,     size_array};

//     // Add size bin arrays
//     arrays.insert(arrays.end(), size_bin_arrays.begin(),
//     size_bin_arrays.end());

//     // Create table
//     auto table = arrow::Table::Make(schema, arrays);

//     // Write to parquet file
//     std::shared_ptr<arrow::io::FileOutputStream> outfile;
//     ARROW_ASSIGN_OR_RAISE(outfile,
//                           arrow::io::FileOutputStream::Open(output_path));

//     ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
//         *table, arrow::default_memory_pool(), outfile, /*chunk_size=*/1024));

//     return arrow::Status::OK();
// }

// arrow::Result<std::vector<HighLevelMetrics>> hlms_from_parquet(
//     const std::string& input_path) {
//     // Read parquet file
//     std::shared_ptr<arrow::io::ReadableFile> infile;
//     ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(input_path));

//     std::unique_ptr<parquet::arrow::FileReader> reader;
//     ARROW_ASSIGN_OR_RAISE(
//         reader, parquet::arrow::OpenFile(infile,
//         arrow::default_memory_pool()));

//     std::shared_ptr<arrow::Table> table;
//     ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

//     std::vector<HighLevelMetrics> hlms;
//     int64_t num_rows = table->num_rows();

//     if (num_rows == 0) {
//         return hlms;
//     }

//     // Get column arrays
//     auto proc_name_array = std::static_pointer_cast<arrow::StringArray>(
//         table->GetColumnByName("proc_name")->chunk(0));
//     auto cat_array = std::static_pointer_cast<arrow::StringArray>(
//         table->GetColumnByName("cat")->chunk(0));
//     auto epoch_array = std::static_pointer_cast<arrow::StringArray>(
//         table->GetColumnByName("epoch")->chunk(0));
//     auto acc_pat_array = std::static_pointer_cast<arrow::StringArray>(
//         table->GetColumnByName("acc_pat")->chunk(0));
//     auto func_name_array = std::static_pointer_cast<arrow::StringArray>(
//         table->GetColumnByName("func_name")->chunk(0));
//     auto io_cat_array = std::static_pointer_cast<arrow::StringArray>(
//         table->GetColumnByName("io_cat")->chunk(0));
//     auto time_range_array = std::static_pointer_cast<arrow::StringArray>(
//         table->GetColumnByName("time_range")->chunk(0));
//     auto time_array = std::static_pointer_cast<arrow::DoubleArray>(
//         table->GetColumnByName("time")->chunk(0));
//     auto count_array = std::static_pointer_cast<arrow::UInt64Array>(
//         table->GetColumnByName("count")->chunk(0));
//     auto size_array = std::static_pointer_cast<arrow::UInt64Array>(
//         table->GetColumnByName("size")->chunk(0));

//     // Get size bin columns
//     std::vector<std::shared_ptr<arrow::UInt32Array>> size_bin_arrays;
//     for (const auto& suffix : constants::SIZE_BIN_SUFFIXES) {
//         std::string col_name = constants::SIZE_BIN_PREFIX + suffix;
//         auto col = table->GetColumnByName(col_name);
//         if (col) {
//             size_bin_arrays.push_back(
//                 std::static_pointer_cast<arrow::UInt32Array>(col->chunk(0)));
//         } else {
//             size_bin_arrays.push_back(nullptr);
//         }
//     }

//     hlms.reserve(num_rows);

//     for (int64_t i = 0; i < num_rows; i++) {
//         HighLevelMetrics hlm;

//         // Basic numeric fields
//         hlm.time_sum = time_array->Value(i);
//         hlm.count_sum = count_array->Value(i);

//         // Handle optional size
//         if (size_array->IsNull(i)) {
//             hlm.size_sum = std::nullopt;
//         } else {
//             hlm.size_sum = size_array->Value(i);
//         }

//         // Group values
//         hlm.group_values["proc_name"] = proc_name_array->GetString(i);
//         hlm.group_values["cat"] = cat_array->GetString(i);
//         hlm.group_values["epoch"] = epoch_array->GetString(i);
//         hlm.group_values["acc_pat"] = acc_pat_array->GetString(i);
//         hlm.group_values["func_name"] = func_name_array->GetString(i);
//         hlm.group_values["io_cat"] = io_cat_array->GetString(i);
//         hlm.group_values["time_range"] = time_range_array->GetString(i);

//         // Handle size bins
//         auto size_bins = generate_size_bins_vec();
//         for (size_t j = 0; j < size_bins.size() && j <
//         size_bin_arrays.size();
//              ++j) {
//             if (size_bin_arrays[j] && !size_bin_arrays[j]->IsNull(i)) {
//                 hlm.bin_sums[size_bins[j]] = size_bin_arrays[j]->Value(i);
//             } else {
//                 hlm.bin_sums[size_bins[j]] = std::nullopt;
//             }
//         }

//         hlms.push_back(std::move(hlm));
//     }

//     return hlms;
// }
}  // namespace helpers

Analyzer::Analyzer(const AnalyzerConfigManager& config) : config_(config) {}

Pipeline Analyzer::analyze(
    const std::vector<std::string>& traces,
    const std::vector<std::string>& /*view_types*/,
    const std::vector<std::string>& /*exclude_characteristics*/,
    const std::unordered_map<std::string, std::string>& /*extra_columns*/
) {
    TraceReader trace_reader(traces, 128 * 1024 * 1024 /* 128MB */);
    return trace_reader.build();
}

Pipeline Analyzer::compute_high_level_metrics(
    const std::vector<Trace>& /*trace_records*/,
    const std::vector<std::string>& /*view_types*/) {
    return {};
}

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

}  // namespace dftracer::utils::analyzers
