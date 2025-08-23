#ifndef __DFTRACER_UTILS_PIPELINE_ENGINES_FLATMAP_ENGINE_H
#define __DFTRACER_UTILS_PIPELINE_ENGINES_FLATMAP_ENGINE_H

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
class FlatMapOperator;
}
namespace engines {
std::size_t run_flatmap(context::ExecutionContext& ctx,
                        const operators::FlatMapOperator& op, ConstBuffer in,
                        MutBuffer out);

std::vector<std::byte> run_flatmap_alloc(context::ExecutionContext& ctx,
                                         const operators::FlatMapOperator& op,
                                         ConstBuffer in);

}  // namespace engines
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINES_FLATMAP_ENGINE_H
