

#include <dftracer/utils/pipeline/engine/filter.h>
#include <dftracer/utils/pipeline/operators/filter.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>

#include <dftracer/utils/pipeline/engine/helpers.h>

#include <algorithm>
#include <cstring>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engine {

using operators::FilterOperator;

std::size_t run_filter(context::ExecutionContext& ctx,
                       const FilterOperator& op,
                       ConstBuffer in,
                       MutBuffer out) {
  if (!op.pred && !op.pred_with_state) {
    throw std::invalid_argument("run_filter: null predicate");
  }
  if (in.elem_size != op.in_size) {
    throw std::invalid_argument("run_filter: input elem_size mismatch");
  }
  if (out.elem_size != op.in_size) {
    throw std::invalid_argument("run_filter: output elem_size mismatch");
  }

  const auto in_stride  = effective_stride(in.stride,  in.elem_size);
  const auto out_stride = effective_stride(out.stride, out.elem_size);

  const auto* base_in  = static_cast<const std::byte*>(in.data);
  auto*       base_out = static_cast<std::byte*>(out.data);

  if (in.count == 0 || out.count == 0) {
    return 0;
  }

  // First pass: evaluate predicate per element (parallelizable)
  std::vector<unsigned char> keep(in.count, 0);
  std::exception_ptr eptr;
  std::mutex m;

  ctx.parallel_for(in.count, [&](std::size_t i) {
    try {
      const void* src = base_in + i * in_stride;
      bool keep_i = false;
      if (op.pred_with_state) {
        keep_i = op.pred_with_state(src, op.state);
      } else {
        keep_i = op.pred(src);
      }
      keep[i] = static_cast<unsigned char>(keep_i);
    } catch (...) {
      std::lock_guard<std::mutex> lk(m);
      if (!eptr) eptr = std::current_exception();
    }
  });

  if (eptr) std::rethrow_exception(eptr);

  // Second pass: exclusive prefix sum (sequential for determinism/simplicity)
  std::vector<std::size_t> pos(in.count, 0);
  std::size_t running = 0;
  for (std::size_t i = 0; i < in.count; ++i) {
    pos[i] = running;
    running += keep[i];
  }
  const std::size_t total_kept = running;
  const std::size_t limit = std::min<std::size_t>(total_kept, out.count);

  if (limit == 0) return 0;

  // Third pass: stable scatter (parallelizable)
  ctx.parallel_for(in.count, [&](std::size_t i) {
    if (!keep[i]) return;
    const std::size_t dst_index = pos[i];
    if (dst_index >= limit) return;  // respect output capacity

    const void* src = base_in + i * in_stride;
    void*       dst = base_out + dst_index * out_stride;
    std::memcpy(dst, src, op.in_size);
  });

  return limit;
}

std::vector<std::byte> run_filter_alloc(context::ExecutionContext& ctx,
                                        const FilterOperator& op,
                                        ConstBuffer in) {
  std::vector<std::byte> out_bytes;
  if (in.count == 0) return out_bytes;

  out_bytes.resize(in.count * op.in_size);
  MutBuffer out{ out_bytes.data(), in.count, op.in_size, 0 };

  const std::size_t kept = run_filter(ctx, op, in, out);
  out_bytes.resize(kept * op.in_size);
  return out_bytes;
}

} // namespace engine
} // namespace pipeline
} // namespace utils
} // namespace dftracer
