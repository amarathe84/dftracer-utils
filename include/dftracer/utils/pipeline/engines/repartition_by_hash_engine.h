#ifndef __DFTRACER_UTILS_PIPELINE_ENGINES_REPARTITION_BY_HASH_ENGINE_H
#define __DFTRACER_UTILS_PIPELINE_ENGINES_REPARTITION_BY_HASH_ENGINE_H

#include <dftracer/utils/pipeline/engines/buffer.h>
#include <dftracer/utils/pipeline/engines/engine.h>
#include <dftracer/utils/pipeline/operators/repartition_by_hash_operator.h>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engines {

namespace context {
class ExecutionContext;
}
struct RepartitionResult {
  std::vector<std::byte> bytes;
  std::vector<std::size_t> offsets;
  std::vector<std::size_t> counts;
  std::size_t elem_size{0};
};

std::size_t run_repartition_by_hash(
    context::ExecutionContext& ctx,
    const operators::RepartitionByHashOperator& op, ConstBuffer in,
    MutBuffer out, std::vector<std::size_t>& offsets_out,
    std::vector<std::size_t>& counts_out);

RepartitionResult run_repartition_by_hash_alloc(
    context::ExecutionContext& ctx,
    const operators::RepartitionByHashOperator& op, ConstBuffer in);
}  // namespace engines
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINES_REPARTITION_BY_HASH_ENGINE_H
