#ifndef __DFTRACER_UTILS_PIPELINE_ENGINE_MAP_H
#define __DFTRACER_UTILS_PIPELINE_ENGINE_MAP_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include <dftracer/utils/pipeline/engine/buffer.h>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context { class ExecutionContext; }
namespace operators { class MapOperator; }
namespace engine {


// Execute a MapOperator over an input buffer into a preallocated output buffer.
// Requirements:
//  - in.elem_size  == op.in_size
//  - out.elem_size == op.out_size
//  - in.count == out.count
//  - If stride == 0, the implementation treats it as elem_size (tightly packed).
// The execution strategy (sequential/threads/MPI) is provided by `ctx`.
void run_map(context::ExecutionContext& ctx,
             const operators::MapOperator& op,
             ConstBuffer in,
             MutBuffer out);

// Allocate a tightly-packed output buffer (out.stride == out.elem_size) and
// return it as a byte vector. The number of elements is in.count and the
// element size is op.out_size.
std::vector<std::byte> run_map_alloc(context::ExecutionContext& ctx,
                                     const operators::MapOperator& op,
                                     ConstBuffer in);

} // namespace engine
} // namespace pipeline
} // namespace utils
} // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINE_MAP_H
