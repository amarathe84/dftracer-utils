#include <dftracer/utils/pipeline/engine/helpers.h>
#include <dftracer/utils/pipeline/engine/map_engine.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/operators/map_operator.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engine {

using context::ExecutionContext;
using operators::MapOperator;

void run_map(ExecutionContext& ctx, const MapOperator& op, ConstBuffer in,
             MutBuffer out) {
  if (in.count != out.count) {
    throw std::invalid_argument("run_map: input and output counts differ");
  }
  if (in.elem_size != op.in_size) {
    throw std::invalid_argument("run_map: in.elem_size != op.in_size");
  }
  if (out.elem_size != op.out_size) {
    throw std::invalid_argument("run_map: out.elem_size != op.out_size");
  }
  if ((in.count > 0) && (!in.data || !out.data)) {
    throw std::invalid_argument(
        "run_map: null data pointer with non-zero count");
  }
  if (!op.fn && !op.fn_with_state) {
    throw std::invalid_argument("run_map: null map function");
  }

  const std::size_t in_stride = effective_stride(in.stride, in.elem_size);
  const std::size_t out_stride = effective_stride(out.stride, out.elem_size);

  const auto* base_in = static_cast<const std::byte*>(in.data);
  auto* base_out = static_cast<std::byte*>(out.data);

  std::exception_ptr eptr = nullptr;
  std::mutex m;

  ctx.parallel_for(in.count, [&](std::size_t i) {
    try {
      const void* src = base_in + i * in_stride;
      void* dst = base_out + i * out_stride;
      if (op.fn_with_state) {
        op.fn_with_state(src, dst, op.state);
      } else {
        op.fn(src, dst);
      }
    } catch (...) {
      // Capture first exception and keep going to allow threads to finish.
      std::lock_guard<std::mutex> lk(m);
      if (!eptr) eptr = std::current_exception();
    }
  });

  if (eptr) std::rethrow_exception(eptr);
}

std::vector<std::byte> run_map_alloc(ExecutionContext& ctx,
                                     const MapOperator& op, ConstBuffer in) {
  std::vector<std::byte> out_bytes;
  out_bytes.resize(in.count * op.out_size);

  MutBuffer out{
      out_bytes.data(), in.count, op.out_size,
      0  // tightly packed
  };

  run_map(ctx, op, in, out);
  return out_bytes;
}

}  // namespace engine
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer
