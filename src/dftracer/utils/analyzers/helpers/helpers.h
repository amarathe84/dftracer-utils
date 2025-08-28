#ifndef DFTRACER_UTILS_ANALYZERS_HELPERS_HELPERS_H
#define DFTRACER_UTILS_ANALYZERS_HELPERS_HELPERS_H

#include <cstdint>
#include <string>
#include <optional>

#include <dftracer/utils/analyzers/trace.h>
#include <dftracer/utils/utils/json.h>

inline std::uint64_t calc_time_range(std::uint64_t time, double time_granularity) {
    if (time_granularity <= 0.0) return 0;
    return static_cast<uint64_t>(static_cast<double>(time) / time_granularity);
}

std::string derive_io_cat(const std::string& func_name);
bool should_ignore_event(const std::string& func_name);

// bins
std::size_t get_size_bin_index(std::uint64_t size);
std::size_t get_num_size_bins();
void set_size_bins(dftracer::utils::analyzers::Trace& trace);
std::optional<dftracer::utils::analyzers::Trace> parse_trace(const dftracer::utils::json::OwnedJsonDocument& doc);

#endif  // DFTRACER_UTILS_ANALYZERS_HELPERS_HELPERS_H
