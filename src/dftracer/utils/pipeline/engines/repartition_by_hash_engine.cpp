// Repartition by hash engine implementation

#include <dftracer/utils/pipeline/engines/repartition_by_hash_engine.h>

#include <dftracer/utils/pipeline/execution_context/execution_context.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <stdexcept>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engines {

using context::ExecutionContext;
using operators::RepartitionByHashOperator;

namespace {

inline std::size_t eff_stride(std::size_t stride, std::size_t elem) {
  return stride == 0 ? elem : stride;
}

// If input is strided, build a contiguous byte view; otherwise return empty.
static std::pair<const std::byte*, std::vector<std::byte>>
contiguous_view(ConstBuffer in) {
  const auto stride = eff_stride(in.stride, in.elem_size);
  if (stride == in.elem_size) {
    return {static_cast<const std::byte*>(in.data), {}};
  }
  std::vector<std::byte> owned(in.count * in.elem_size);
  const auto* base = static_cast<const std::byte*>(in.data);
  for (std::size_t i = 0; i < in.count; ++i) {
    std::memcpy(owned.data() + i * in.elem_size, base + i * stride, in.elem_size);
  }
  return {owned.data(), std::move(owned)};
}

inline std::uint64_t mix_seed(std::uint64_t h, std::uint64_t seed) {
  // Simple, deterministic blend; can be replaced by better mixing if needed.
  return h ^ seed;
}

} // namespace

std::size_t run_repartition_by_hash(
    ExecutionContext& ctx,
    const RepartitionByHashOperator& op,
    ConstBuffer in,
    MutBuffer out,
    std::vector<std::size_t>& offsets_out,
    std::vector<std::size_t>& counts_out) {

  if (in.elem_size != op.elem_size) {
    throw std::invalid_argument("repartition_by_hash: input elem_size mismatch");
  }
  if (out.elem_size != op.elem_size) {
    throw std::invalid_argument("repartition_by_hash: output elem_size mismatch");
  }
  if (op.num_partitions == 0) {
    throw std::invalid_argument("repartition_by_hash: num_partitions must be > 0");
  }
  if (!op.hash_fn && !op.hash_fn_with_state) {
    throw std::invalid_argument("repartition_by_hash: no hash function provided");
  }

  if (in.count == 0 || out.count == 0) {
    offsets_out.assign(op.num_partitions, 0);
    counts_out.assign(op.num_partitions, 0);
    return 0;
  }

  if (out.count < in.count) {
    throw std::invalid_argument("repartition_by_hash: output buffer too small");
  }

  const auto inv = contiguous_view(in);
  const auto* base_in = inv.first;
  const std::size_t n = in.count;
  const std::size_t elem = in.elem_size;
  const std::size_t P = op.num_partitions;

  // 1) Compute bucket for each element (parallelizable)
  std::vector<std::uint32_t> bucket(n);
  ctx.parallel_for(n, [&](std::size_t i) {
    const std::byte* ptr = base_in + i * elem; // contiguous view guarantees packing
    const std::uint64_t h = op.hash_fn_with_state
                                ? op.hash_fn_with_state(ptr, op.state)
                                : op.hash_fn(ptr);
    bucket[i] = static_cast<std::uint32_t>(mix_seed(h, op.seed) % P);
  });

  // 2) Count per bucket (stable order later)
  counts_out.assign(P, 0);
  for (std::size_t i = 0; i < n; ++i) {
    ++counts_out[bucket[i]];
  }

  // 3) Offsets via prefix sum
  offsets_out.assign(P, 0);
  std::size_t acc = 0;
  for (std::size_t p = 0; p < P; ++p) {
    offsets_out[p] = acc;
    acc += counts_out[p];
  }

  // 4) Scatter into output in a stable manner
  // If output is contiguous, write directly; otherwise write into temp and scatter
  const auto out_stride = eff_stride(out.stride, out.elem_size);
  if (out_stride == out.elem_size) {
    auto* base_out = static_cast<std::byte*>(out.data);
    std::vector<std::size_t> cursor = offsets_out;
    for (std::size_t i = 0; i < n; ++i) {
      const std::size_t p = bucket[i];
      const std::size_t pos = cursor[p]++;
      std::memcpy(base_out + pos * elem, base_in + i * elem, elem);
    }
  } else {
    // Write into contiguous local then scatter to strided
    std::vector<std::byte> local(n * elem);
    std::vector<std::size_t> cursor = offsets_out;
    for (std::size_t i = 0; i < n; ++i) {
      const std::size_t p = bucket[i];
      const std::size_t pos = cursor[p]++;
      std::memcpy(local.data() + pos * elem, base_in + i * elem, elem);
    }
    // scatter to possibly-strided out
    auto* base_out = static_cast<std::byte*>(out.data);
    for (std::size_t p = 0; p < P; ++p) {
      const std::size_t start = offsets_out[p];
      const std::size_t cnt = counts_out[p];
      for (std::size_t k = 0; k < cnt; ++k) {
        const std::size_t pos = start + k;
        std::memcpy(base_out + pos * out_stride, local.data() + pos * elem, elem);
      }
    }
  }

  return n;
}

RepartitionResult run_repartition_by_hash_alloc(
    ExecutionContext& ctx,
    const RepartitionByHashOperator& op,
    ConstBuffer in) {
  if (in.elem_size != op.elem_size) {
    throw std::invalid_argument("repartition_by_hash_alloc: input elem_size mismatch");
  }
  if (op.num_partitions == 0) {
    throw std::invalid_argument("repartition_by_hash_alloc: num_partitions must be > 0");
  }
  if (!op.hash_fn && !op.hash_fn_with_state) {
    throw std::invalid_argument("repartition_by_hash_alloc: no hash function provided");
  }

  RepartitionResult res;
  res.elem_size = in.elem_size;
  res.offsets.assign(op.num_partitions, 0);
  res.counts.assign(op.num_partitions, 0);

  if (in.count == 0) {
    return res; // empty
  }

  const auto inv = contiguous_view(in);
  const auto* base_in = inv.first;
  const std::size_t n = in.count;
  const std::size_t elem = in.elem_size;
  const std::size_t P = op.num_partitions;

  // 1) Compute bucket per element (parallel)
  std::vector<std::uint32_t> bucket(n);
  ctx.parallel_for(n, [&](std::size_t i) {
    const std::byte* ptr = base_in + i * elem;
    const std::uint64_t h = op.hash_fn_with_state
                                ? op.hash_fn_with_state(ptr, op.state)
                                : op.hash_fn(ptr);
    bucket[i] = static_cast<std::uint32_t>(mix_seed(h, op.seed) % P);
  });

  // 2) counts and 3) offsets
  for (std::size_t i = 0; i < n; ++i) ++res.counts[bucket[i]];
  std::size_t acc = 0;
  for (std::size_t p = 0; p < P; ++p) {
    const std::size_t c = res.counts[p];
    res.offsets[p] = acc;
    acc += c;
  }

  // 4) allocate and stable-scatter
  res.bytes.assign(n * elem, std::byte{0});
  auto* outb = res.bytes.data();
  std::vector<std::size_t> cursor = res.offsets;
  for (std::size_t i = 0; i < n; ++i) {
    const std::size_t p = bucket[i];
    const std::size_t pos = cursor[p]++;
    std::memcpy(outb + pos * elem, base_in + i * elem, elem);
  }

  return res;
}

} // namespace engines
} // namespace pipeline
} // namespace utils
} // namespace dftracer
