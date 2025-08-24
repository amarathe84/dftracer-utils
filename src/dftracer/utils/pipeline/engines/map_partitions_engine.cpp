#include <dftracer/utils/pipeline/engines/helpers.h>
#include <dftracer/utils/pipeline/engines/map_partitions_engine.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/operators/map_partitions_operator.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engines {

using context::ExecutionContext;
using operators::MapPartitionsOperator;

namespace {

// Pack possibly-strided input into a contiguous temporary if needed.
// Returns a pair {ptr, owned_bytes}. If owned_bytes.empty(), ptr points into
// the original buffer.
std::pair<const void*, std::vector<std::byte>> contiguous_view(ConstBuffer in) {
  const auto stride = effective_stride(in.stride, in.elem_size);
  if (stride == in.elem_size) {
    return {in.data, {}};  // already contiguous
  }
  std::vector<std::byte> owned(in.count * in.elem_size);
  const auto* base = static_cast<const std::byte*>(in.data);
  for (std::size_t i = 0; i < in.count; ++i) {
    std::memcpy(owned.data() + i * in.elem_size, base + i * stride,
                in.elem_size);
  }
  return {owned.data(), std::move(owned)};
}

// Scatter from a contiguous buffer into a possibly-strided MutBuffer.
void scatter_to_output(const void* src_contig, std::size_t produced,
                       std::size_t elem_size, MutBuffer out) {
  const auto stride = effective_stride(out.stride, out.elem_size);
  auto* base_out = static_cast<std::byte*>(out.data);
  if (stride == out.elem_size) {
    std::memcpy(base_out, src_contig, produced * elem_size);
    return;
  }
  const auto* src = static_cast<const std::byte*>(src_contig);
  for (std::size_t i = 0; i < produced; ++i) {
    std::memcpy(base_out + i * stride, src + i * elem_size, elem_size);
  }
}
}  // namespace

std::size_t run_map_partitions(ExecutionContext& ctx,
                               const MapPartitionsOperator& op, ConstBuffer in,
                               MutBuffer out) {
  if (!op.fn && !op.fn_with_state) {
    throw std::invalid_argument("run_map_partitions: null kernel");
  }
  if (in.elem_size != op.in_elem_size) {
    throw std::invalid_argument("run_map_partitions: input elem_size mismatch");
  }
  if (out.elem_size != op.out_elem_size) {
    throw std::invalid_argument(
        "run_map_partitions: output elem_size mismatch");
  }

  if (in.count == 0 || out.count == 0) return 0;

  // Default partitioning for now: single partition spanning entire input.
  // (Can be extended later to consult ctx for ranges.)
  const auto in_view = contiguous_view(in);

  MapPartitionsOperator::PartitionInfo part{};
  part.partition_index = 0;
  part.partitions_in_context = 1;  // TODO: consult ctx for ranges in future
  part.upstream_offset_elems = 0;
  part.upstream_count_elems = in.count;
  part.world_rank = ctx.rank();
  part.world_size = ctx.size();

  // Let the kernel write directly into caller's output buffer (contiguous view)
  // NOTE: *out_count is used by the trampoline both as INPUT (capacity)
  // and OUTPUT (produced). Initialize it with capacity before the call.
  std::size_t produced = out.count;
  std::vector<std::byte> out_local;
  const auto out_stride = effective_stride(out.stride, out.elem_size);
  if (out_stride == out.elem_size) {
    // Contiguous out, call directly
    if (op.fn_with_state) {
      op.fn_with_state(part, in_view.first, in.count, in.elem_size, out.data,
                       &produced, out.elem_size, op.state);
    } else {
      op.fn(part, in_view.first, in.count, in.elem_size, out.data, &produced,
            out.elem_size);
    }
    if (produced > out.count) {
      throw std::logic_error(
          "run_map_partitions: kernel produced more than provided capacity");
    }
  } else {
    // Strided out: write into a contiguous temporary and scatter
    out_local.resize(out.count * out.elem_size);
    // Initialize capacity for the trampoline
    produced = out.count;
    void* out_ptr = out_local.data();
    if (op.fn_with_state) {
      op.fn_with_state(part, in_view.first, in.count, in.elem_size, out_ptr,
                       &produced, out.elem_size, op.state);
    } else {
      op.fn(part, in_view.first, in.count, in.elem_size, out_ptr, &produced,
            out.elem_size);
    }
    if (produced > out.count) {
      throw std::logic_error(
          "run_map_partitions: kernel produced more than provided capacity");
    }
    scatter_to_output(out_local.data(), produced, out.elem_size, out);
  }

  return produced;
}

std::vector<std::byte> run_map_partitions_alloc(ExecutionContext& ctx,
                                                const MapPartitionsOperator& op,
                                                ConstBuffer in) {
  if (!op.fn && !op.fn_with_state) {
    throw std::invalid_argument("run_map_partitions_alloc: null kernel");
  }
  if (in.elem_size != op.in_elem_size) {
    throw std::invalid_argument(
        "run_map_partitions_alloc: input elem_size mismatch");
  }
  if (in.count == 0) return {};

  const auto in_view = contiguous_view(in);

  MapPartitionsOperator::PartitionInfo part{};
  part.partition_index = 0;
  part.partitions_in_context = 1;  // TODO: consult ctx for ranges in future
  part.upstream_offset_elems = 0;
  part.upstream_count_elems = in.count;
  part.world_rank = ctx.rank();
  part.world_size = ctx.size();

  // First attempt: capacity == in.count (common case when output ~ input)
  std::size_t capacity = in.count ? in.count : 1;
  std::vector<std::byte> out_bytes(capacity * op.out_elem_size);
  std::size_t produced = 0;

  auto call_kernel = [&](void* out_ptr, std::size_t cap_elems) {
    // Initialize in/out with capacity so trampolines know how much they can
    // write
    produced = cap_elems;
    if (op.fn_with_state) {
      op.fn_with_state(part, in_view.first, in.count, in.elem_size, out_ptr,
                       &produced, op.out_elem_size, op.state);
    } else {
      op.fn(part, in_view.first, in.count, in.elem_size, out_ptr, &produced,
            op.out_elem_size);
    }
    if (produced > cap_elems) {
      // Not enough capacity: signal caller to grow and retry
      return false;
    }
    return true;
  };

  if (!call_kernel(out_bytes.data(), capacity)) {
    // Grow to exactly the produced size and call again
    const std::size_t needed = produced;  // kernel told us how many it wants
    out_bytes.assign(needed * op.out_elem_size, std::byte{0});
    const bool ok = call_kernel(out_bytes.data(), needed);
    if (!ok) {
      throw std::logic_error(
          "run_map_partitions_alloc: kernel size increased between attempts");
    }
  } else {
    // shrink to fit
    out_bytes.resize(produced * op.out_elem_size);
  }

  return out_bytes;
}

}  // namespace engines
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer
