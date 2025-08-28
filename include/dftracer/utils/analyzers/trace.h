#ifndef DFTRACER_UTILS_ANALYZERS_TRACE_RECORD_H
#define DFTRACER_UTILS_ANALYZERS_TRACE_RECORD_H

#include <dftracer/utils/utils/json.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::analyzers {

enum class TraceType {
    Regular,
    FileHash,
    HostHash,
    StringHash,
    ProcessMetadata,
    OtherMetadata
};

typedef std::unordered_map<std::string, std::int32_t> BinFields;

struct Trace {
    std::string cat;
    std::string io_cat;
    std::string acc_pat;
    std::string func_name;
    double duration;
    std::uint64_t count;
    std::uint64_t time_range;
    std::uint64_t time_start;
    std::uint64_t time_end;
    std::uint64_t epoch;
    std::uint64_t pid;
    std::uint64_t tid;
    std::string fhash;
    std::string hhash;
    std::uint64_t image_id;
    TraceType type;

    std::int64_t size = -1; // -1 means NaN or unknown
    std::int64_t offset = -1; // -1 means NaN or unknown
    std::unordered_map<std::string, std::string> view_fields;
    BinFields bin_fields;
};

}  // namespace dftracer::utils::analyzers

#endif  // DFTRACER_UTILS_ANALYZERS_TRACE_RECORD_H
