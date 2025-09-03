#include <dftracer/utils/analyzers/helpers/helpers.h>

#include <algorithm>

// using namespace dftracer::utils::json;
// using namespace dftracer::utils::analyzers;

// Trace parse_trace(const dftracer::utils::json::JsonDocument& doc) {
//     Trace trace = {};
//     // trace.is_valid = false;

//     // try {
//     //     if (!doc.is_object()) {
//     //         return trace;
//     //     }

//     //     std::string func_name = get_string_field(doc, "name");
//     //     std::string phase = get_string_field(doc, "ph");

//     //     if (func_name.empty()) {
//     //         return trace;
//     //     }

//     //     if (should_ignore_event(func_name)) {
//     //         return trace;
//     //     }

//     //     trace.func_name = func_name;

//     //     // Extract cat field
//     //     std::string cat = get_string_field(doc, "cat");
//     //     if (cat.empty()) {
//     //         return trace;
//     //     } else {
//     //         std::transform(cat.begin(), cat.end(), cat.begin(),
//     ::tolower);
//     //         trace.cat = cat;
//     //     }

//     //     // Extract pid and tid
//     //     trace.pid = get_uint64_field(doc, "pid");
//     //     trace.tid = get_uint64_field(doc, "tid");

//     //     // Extract hhash from args if available
//     //     trace.hhash = get_args_string_field(doc, "hhash");

//     //     // Handle metadata events (phase == "M")
//     //     if (phase == "M") {
//     //         if (func_name == "FH") {
//     //             trace.type = TraceType::FileHash;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         } else if (func_name == "HH") {
//     //             trace.type = TraceType::HostHash;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.hhash = get_args_string_field(doc, "value");
//     //         } else if (func_name == "SH") {
//     //             trace.type = TraceType::StringHash;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         } else if (func_name == "PR") {
//     //             trace.type = TraceType::ProcessMetadata;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         } else {
//     //             trace.type = TraceType::OtherMetadata;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         }
//     //     } else {
//     //         // Regular event (type = 0)
//     //         trace.type = TraceType::Regular;

//     //         // Extract duration and timestamp
//     //         trace.duration = get_double_field(doc, "dur");
//     //         trace.time_start = get_uint64_field(doc, "ts");
//     //         trace.time_end =
//     //             trace.time_start + static_cast<uint64_t>(trace.duration);
//     //         trace.count = 1;

//     //         // this will be recalculated later
//     //         trace.time_range = 0;

//     //         // Extract IO-related fields
//     //         trace.fhash = get_args_string_field(doc, "fhash");

//     //         if (trace.cat == "posix" || trace.cat == "stdio") {
//     //             trace.io_cat = derive_io_cat(func_name);

//     //             // Get ret value directly as numeric from args
//     //             auto obj_result = doc.get_object();
//     //             if (!obj_result.error()) {
//     //                 auto obj = obj_result.value();
//     //                 for (auto field : obj) {
//     //                     std::string field_key = std::string(field.key);
//     //                     if (field_key == "args" &&
//     field.value.is_object()) {
//     //                         auto args_result = field.value.get_object();
//     //                         if (!args_result.error()) {
//     //                             auto args = args_result.value();
//     //                             for (auto arg_field : args) {
//     //                                 std::string arg_key =
//     //                                     std::string(arg_field.key);
//     //                                 if (arg_key == "ret") {
//     //                                     std::uint64_t ret_value = 0;
//     //                                     if (arg_field.value.is_uint64()) {
//     //                                         ret_value =
//     // arg_field.value.get_uint64();
//     //                                     } else if
//     //                                     (arg_field.value.is_int64()) {
//     //                                         int64_t signed_ret =
//     // arg_field.value.get_int64();
//     //                                         if (signed_ret > 0) {
//     //                                             ret_value +=
//     // static_cast<std::uint64_t>(
//     //                                                     signed_ret);
//     //                                         }
//     //                                     }

//     //                                     if (ret_value > 0 &&
//     //                                         (trace.io_cat == "read" ||
//     //                                          trace.io_cat == "write")) {
//     //                                         trace.size = ret_value;
//     //                                     }
//     //                                     break;
//     //                                 }
//     //                             }
//     //                         }
//     //                         break;
//     //                     }
//     //                 }
//     //             }

//     //             std::string offset_str = get_args_string_field(doc,
//     //             "offset"); if (!offset_str.empty()) {
//     //                 try {
//     //                     trace.offset = std::stoull(offset_str);
//     //                 } catch (...) {
//     //                     // Ignore parse errors - offset remains nullopt
//     //                 }
//     //             }
//     //         } else {
//     //             trace.io_cat = "other";

//     //             // Extract image_id for non-POSIX events
//     //             std::string image_idx_str =
//     //                 get_args_string_field(doc, "image_idx");
//     //             if (!image_idx_str.empty()) {
//     //                 try {
//     //                     trace.image_id = std::stoull(image_idx_str);
//     //                 } catch (...) {
//     //                     // Ignore parse errors
//     //                 }
//     //             }
//     //         }

//     //         trace.acc_pat = "0";

//     //         // Extract epoch from args if available
//     //         std::string epoch_str = get_args_string_field(doc, "epoch");
//     //         if (!epoch_str.empty()) {
//     //             try {
//     //                 trace.epoch = std::stoull(epoch_str);
//     //             } catch (...) {
//     //                 trace.epoch = 0;  // Default if parsing fails
//     //             }
//     //         }

//     //         // Set size bins
//     //         set_size_bins(trace);
//     //     }

//     // } catch (const std::exception& e) {
//     //     return trace;
//     // }

//     trace.is_valid = true;
//     return trace;
// }

// Trace parse_trace_owned(const dftracer::utils::json::JsonDocument& doc) {
//     Trace trace = {};
//     // trace.is_valid = false;

//     // try {
//     //     if (!doc.is_object()) {
//     //         return trace;
//     //     }

//     //     std::string func_name = get_string_field(doc, "name");
//     //     std::string phase = get_string_field(doc, "ph");

//     //     if (func_name.empty()) {
//     //         return trace;
//     //     }

//     //     if (should_ignore_event(func_name)) {
//     //         return trace;
//     //     }
//     //     trace.func_name = func_name;

//     //     // Extract cat field
//     //     std::string cat = get_string_field(doc, "cat");
//     //     if (cat.empty()) {
//     //         return trace;
//     //     } else {
//     //         std::transform(cat.begin(), cat.end(), cat.begin(),
//     ::tolower);
//     //         trace.cat = cat;
//     //     }

//     //     // Extract pid and tid
//     //     trace.pid = get_uint64_field(doc, "pid");
//     //     trace.tid = get_uint64_field(doc, "tid");

//     //     // Extract hhash from args if available
//     //     trace.hhash = get_args_string_field(doc, "hhash");

//     //     // Handle metadata events (phase == "M")
//     //     if (phase == "M") {
//     //         if (func_name == "FH") {
//     //             trace.type = TraceType::FileHash;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         } else if (func_name == "HH") {
//     //             trace.type = TraceType::HostHash;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.hhash = get_args_string_field(doc, "value");
//     //         } else if (func_name == "SH") {
//     //             trace.type = TraceType::StringHash;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         } else if (func_name == "PR") {
//     //             trace.type = TraceType::ProcessMetadata;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         } else {
//     //             trace.type = TraceType::OtherMetadata;
//     //             trace.func_name = get_args_string_field(doc, "name");
//     //             trace.fhash = get_args_string_field(doc, "value");
//     //         }
//     //     } else {
//     //         // Regular event (type = 0)
//     //         trace.type = TraceType::Regular;

//     //         // Extract duration and timestamp
//     //         trace.duration = get_double_field(doc, "dur");
//     //         trace.time_start = get_uint64_field(doc, "ts");
//     //         trace.time_end =
//     //             trace.time_start + static_cast<uint64_t>(trace.duration);
//     //         trace.count = 1;

//     //         // this will be recalculated later
//     //         trace.time_range = 0;

//     //         // Extract IO-related fields
//     //         trace.fhash = get_args_string_field(doc, "fhash");

//     //         if (trace.cat == "posix" || trace.cat == "stdio") {
//     //             trace.io_cat = derive_io_cat(func_name);

//     //             // Get ret value directly as numeric from args
//     //             auto obj_result = doc.get_object();
//     //             if (!obj_result.error()) {
//     //                 auto obj = obj_result.value();
//     //                 for (auto field : obj) {
//     //                     std::string field_key = std::string(field.key);
//     //                     if (field_key == "args" &&
//     field.value.is_object()) {
//     //                         auto args_result = field.value.get_object();
//     //                         if (!args_result.error()) {
//     //                             auto args = args_result.value();
//     //                             for (auto arg_field : args) {
//     //                                 std::string arg_key =
//     //                                     std::string(arg_field.key);
//     //                                 if (arg_key == "ret") {
//     //                                     std::uint64_t ret_value = 0;
//     //                                     if (arg_field.value.is_uint64()) {
//     //                                         ret_value =
//     // arg_field.value.get_uint64();
//     //                                     } else if
//     //                                     (arg_field.value.is_int64()) {
//     //                                         int64_t signed_ret =
//     // arg_field.value.get_int64();
//     //                                         if (signed_ret > 0) {
//     //                                             ret_value =
//     // static_cast<std::uint64_t>(
//     //                                                     signed_ret);
//     //                                         }
//     //                                     }

//     //                                     if (ret_value > 0 &&
//     //                                         (trace.io_cat == "read" ||
//     //                                          trace.io_cat == "write")) {
//     //                                         trace.size = ret_value;
//     //                                     }
//     //                                     break;
//     //                                 }
//     //                             }
//     //                         }
//     //                         break;
//     //                     }
//     //                 }
//     //             }

//     //             std::string offset_str = get_args_string_field(doc,
//     //             "offset"); if (!offset_str.empty()) {
//     //                 try {
//     //                     trace.offset = std::stoull(offset_str);
//     //                 } catch (...) {
//     //                     // Ignore parse errors - offset remains nullopt
//     //                 }
//     //             }
//     //         } else {
//     //             trace.io_cat = "other";

//     //             // Extract image_id for non-POSIX events
//     //             std::string image_idx_str =
//     //                 get_args_string_field(doc, "image_idx");
//     //             if (!image_idx_str.empty()) {
//     //                 try {
//     //                     trace.image_id = std::stoull(image_idx_str);
//     //                 } catch (...) {
//     //                     // Ignore parse errors
//     //                 }
//     //             }
//     //         }

//     //         trace.acc_pat = "0";

//     //         // Extract epoch from args if available
//     //         std::string epoch_str = get_args_string_field(doc, "epoch");
//     //         if (!epoch_str.empty()) {
//     //             try {
//     //                 trace.epoch = std::stoull(epoch_str);
//     //             } catch (...) {
//     //                 trace.epoch = 0;  // Default if parsing fails
//     //             }
//     //         }

//     //         // Set size bins
//     //         set_size_bins(trace);
//     //     }

//     // } catch (const std::exception& e) {
//     //     return trace;
//     // }

//     // trace.is_valid = true;
//     return trace;
// }
