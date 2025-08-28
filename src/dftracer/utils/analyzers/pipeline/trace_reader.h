#ifndef DFTRACER_UTILS_ANALYZERS_PIPELINE_TRACE_READER_H
#define DFTRACER_UTILS_ANALYZERS_PIPELINE_TRACE_READER_H

#include <dftracer/utils/analyzers/trace.h>
#include <dftracer/utils/pipeline/pipeline.h>

#include <cstdint>
#include <string>
#include <vector>

namespace dftracer::utils::analyzers {

using namespace dftracer::utils;

struct TraceReader {
    std::vector<std::string> traces;
    std::size_t batch_size;

    TraceReader(const std::vector<std::string>& traces_,
                std::size_t batch_size_)
        : traces(traces_), batch_size(batch_size_){};
    Pipeline build();
};

}  // namespace dftracer::utils::analyzers

#endif  // DFTRACER_UTILS_ANALYZERS_PIPELINE_TRACE_READER_H
