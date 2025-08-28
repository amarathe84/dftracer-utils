#ifndef DFTRACER_UTILS_ANALYZERS_ANALYZER_RESULT_H
#define DFTRACER_UTILS_ANALYZERS_ANALYZER_RESULT_H

#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/bag.h>
#include <dftracer/utils/utils/json.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <cstdint>

namespace dftracer::utils::analyzers {

using namespace dftracer::utils::pipeline;

struct HighLevelMetrics {
    double time_sum = 0.0;
    uint64_t count_sum = 0;
    std::optional<std::uint64_t> size_sum;
    std::unordered_map<std::string, std::optional<std::uint32_t>> bin_sums;
    std::unordered_map<std::string, std::unordered_set<std::string>>
        unique_sets;
    std::unordered_map<std::string, std::string> group_values;
};

struct AnalyzerResult {
    std::vector<HighLevelMetrics> _hlms;
};

}  // namespace dftracer::utils::analyzers

#endif  // DFTRACER_UTILS_ANALYZERS_ANALYZER_RESULT_H
