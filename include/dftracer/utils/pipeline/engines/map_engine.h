#ifndef __DFTRACER_UTILS_PIPELINE_ENGINES_MAP_ENGINE_H
#define __DFTRACER_UTILS_PIPELINE_ENGINES_MAP_ENGINE_H

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
class MapOperator;
}
namespace engine {
void run_map(context::ExecutionContext& ctx, const operators::MapOperator& op,
             ConstBuffer in, MutBuffer out);
std::vector<std::byte> run_map_alloc(context::ExecutionContext& ctx,
                                     const operators::MapOperator& op,
                                     ConstBuffer in);

}  // namespace engine
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINES_MAP_ENGINE_H
