#ifndef DFTRACER_UTILS_ANALYZERS_PIPELINE_TRACE_READER_H
#define DFTRACER_UTILS_ANALYZERS_PIPELINE_TRACE_READER_H

#include <dftracer/utils/analyzers/trace.h>
#include <dftracer/utils/pipeline/pipeline.h>

#include <vector>
#include <string>
#include <cstdint>

namespace dftracer::utils::analyzers {

using namespace dftracer::utils;

struct TraceReader {
  std::vector<std::string> traces;
  std::size_t batch_size;

  TraceReader(const std::vector<std::string>& traces_, std::size_t batch_size_)
      : traces(traces_), batch_size(batch_size_) {};
  Pipeline build();
};

}  // namespace dftracer::utils::analyzers::pipeline

#endif  // DFTRACER_UTILS_ANALYZERS_PIPELINE_TRACE_READER_H
