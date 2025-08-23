#include <dftracer/utils/pipeline/engines/flatmap_engine.h>
#include <dftracer/utils/pipeline/engines/helpers.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/operators/flatmap_operator.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engines {

using context::ExecutionContext;
using operators::FlatMapOperator;

std::size_t run_flatmap(ExecutionContext& ctx, const FlatMapOperator& op,
                        ConstBuffer in, MutBuffer out) {
  if (!op.fn && !op.fn_with_state) {
    throw std::invalid_argument("run_flatmap: null kernel");
  }
  if (in.elem_size != op.in_size) {
    throw std::invalid_argument("run_flatmap: input elem_size mismatch");
  }
  if (out.elem_size != op.out_size) {
    throw std::invalid_argument("run_flatmap: output elem_size mismatch");
  }

  if (in.count == 0 || out.count == 0) return 0;

  const auto in_stride = effective_stride(in.stride, in.elem_size);
  const auto out_stride = effective_stride(out.stride, out.elem_size);
  const auto* base_in = static_cast<const std::byte*>(in.data);
  auto* base_out = static_cast<std::byte*>(out.data);

  // 1) Per-input local byte buffers collecting produced outputs
  std::vector<std::vector<std::byte>> locals(in.count);
  std::exception_ptr eptr;
  std::mutex m;

  ctx.parallel_for(in.count, [&](std::size_t i) {
    try {
      const void* src = base_in + i * in_stride;
      auto& buf = locals[i];

      // Reserve based on expansion hint if provided
      if (op.expansion_hint > 0) {
        const double h = op.expansion_hint;  // expected outputs per input
        const std::size_t expect_elems = static_cast<std::size_t>(std::ceil(h));
        const std::size_t expect_bytes = expect_elems * op.out_size;
        buf.reserve(expect_bytes);
      }

      // Capture appends into this local buffer
      auto emit_one = [&](const void* out_elem) {
        const auto n = op.out_size;
        const auto* oe = static_cast<const std::byte*>(out_elem);
        const auto old = buf.size();
        buf.resize(old + n);
        std::memcpy(buf.data() + old, oe, n);
      };

      FlatMapOperator::Emitter em{+[](void* c, const void* oe) {
                                    (*static_cast<decltype(emit_one)*>(c))(oe);
                                  },
                                  &emit_one};

      if (op.fn_with_state) {
        op.fn_with_state(src, em, op.state);
      } else {
        op.fn(src, em);
      }
    } catch (...) {
      std::lock_guard<std::mutex> lk(m);
      if (!eptr) eptr = std::current_exception();
    }
  });

  if (eptr) std::rethrow_exception(eptr);

  // 2) Compute counts (in elements) and exclusive prefix sum to get offsets
  std::vector<std::size_t> counts(in.count, 0), offsets(in.count, 0);
  for (std::size_t i = 0; i < in.count; ++i) {
    counts[i] = locals[i].size() / op.out_size;
  }
  std::size_t total = 0;
  for (std::size_t i = 0; i < in.count; ++i) {
    offsets[i] = total;
    total += counts[i];
  }

  const std::size_t limit = std::min<std::size_t>(total, out.count);
  if (limit == 0) return 0;

  // 3) Scatter locals into the final output buffer (stable concat), capped
  ctx.parallel_for(in.count, [&](std::size_t i) {
    const std::size_t begin = offsets[i];
    const std::size_t end = begin + counts[i];
    const std::size_t capped_begin = std::min(begin, limit);
    const std::size_t capped_end = std::min(end, limit);
    if (capped_begin >= capped_end) return;

    const auto bytes = (capped_end - capped_begin) * op.out_size;
    std::memcpy(base_out + capped_begin * out_stride, locals[i].data(), bytes);
  });

  return limit;
}

std::vector<std::byte> run_flatmap_alloc(ExecutionContext& ctx,
                                         const FlatMapOperator& op,
                                         ConstBuffer in) {
  // Capacity heuristic from hint; otherwise start with input count.
  std::size_t capacity_elems = in.count;
  if (op.expansion_hint > 0) {
    const double h = op.expansion_hint;
    const double cap =
        static_cast<double>(in.count) * std::max<double>(1.0, h * 1.5);
    if (cap > static_cast<double>(capacity_elems)) {
      capacity_elems = static_cast<std::size_t>(cap);
    }
  }

  std::vector<std::byte> out_bytes(capacity_elems * op.out_size);
  MutBuffer out{out_bytes.data(), capacity_elems, op.out_size, 0};

  const std::size_t produced = run_flatmap(ctx, op, in, out);
  out_bytes.resize(produced * op.out_size);
  return out_bytes;
}

}  // namespace engines
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer
