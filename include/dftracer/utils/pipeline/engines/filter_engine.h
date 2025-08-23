#ifndef __DFTRACER_UTILS_PIPELINE_ENGINES_FILTER_ENGINE_H
#define __DFTRACER_UTILS_PIPELINE_ENGINES_FILTER_ENGINE_H

#include <dftracer/utils/pipeline/engines/buffer.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {
class ExecutionContext;
}
namespace operators {
class FilterOperator;
}
namespace engine {

std::size_t run_filter(context::ExecutionContext& ctx,
                       const operators::FilterOperator& op, ConstBuffer in,
                       MutBuffer out);
std::vector<std::byte> run_filter_alloc(context::ExecutionContext& ctx,
                                        const operators::FilterOperator& op,
                                        ConstBuffer in);

}  // namespace engine
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINES_FILTER_ENGINE_H
