#ifndef DFTRACER_UTILS_ANALYZERS_PIPELINE_HIGH_LEVEL_METRICS_H
#define DFTRACER_UTILS_ANALYZERS_PIPELINE_HIGH_LEVEL_METRICS_H

#include <dftracer/utils/analyzers/analyzer_result.h>
#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/analyzers/pipeline/trace_reader.h>
#include <dftracer/utils/analyzers/trace.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/factory.h>

#include <string>
#include <vector>

namespace dftracer::utils::analyzers::pipeline {

using namespace dftracer::utils;

class HLMPipelineGenerator {
   public:
    static Pipeline build(const std::vector<std::string>& traces,
                          size_t batch_size,
                          const std::vector<std::string>& view_types);
};

}  // namespace dftracer::utils::analyzers::pipeline

#endif  // DFTRACER_UTILS_ANALYZERS_PIPELINE_HIGH_LEVEL_METRICS_H
