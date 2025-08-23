#ifndef __DFTRACER_UTILS_PIPELINE_ENGINE_FILTER_H
#define __DFTRACER_UTILS_PIPELINE_ENGINE_FILTER_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include <dftracer/utils/pipeline/engine/buffer.h>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context { class ExecutionContext; }
namespace operators { class FilterOperator; }
namespace engine {

std::size_t run_filter(context::ExecutionContext& ctx,
                       const operators::FilterOperator& op,
                       ConstBuffer in,
                       MutBuffer out);
std::vector<std::byte> run_filter_alloc(context::ExecutionContext& ctx,
                                        const operators::FilterOperator& op,
                                        ConstBuffer in);

} // namespace engine
} // namespace pipeline
} // namespace utils
} // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINE_FILTER_H
