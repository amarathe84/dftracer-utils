#ifndef __DFTRACER_UTILS_PIPELINE_ENGINES_MAP_PARTITIONS_ENGINE_H
#define __DFTRACER_UTILS_PIPELINE_ENGINES_MAP_PARTITIONS_ENGINE_H

#include <dftracer/utils/pipeline/engines/buffer.h>
#include <dftracer/utils/pipeline/operators/map_partitions_operator.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {
class ExecutionContext;
}
namespace engines {
std::size_t run_map_partitions(context::ExecutionContext& ctx,
                               const operators::MapPartitionsOperator& op,
                               ConstBuffer in, MutBuffer out);
std::vector<std::byte> run_map_partitions_alloc(
    context::ExecutionContext& ctx, const operators::MapPartitionsOperator& op,
    ConstBuffer in);

}  // namespace engines
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINES_MAP_PARTITIONS_ENGINE_H
