#ifndef __DFTRACER_UTILS_PIPELINE_ADAPTERS_MAP_PARTITIONS_ADAPTER_H
#define __DFTRACER_UTILS_PIPELINE_ADAPTERS_MAP_PARTITIONS_ADAPTER_H

#include <dftracer/utils/pipeline/adapters/adapter.h>
#include <dftracer/utils/pipeline/operators/map_partitions_operator.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace adapters {

// This adapter lets users provide ergonomic C++17 functions for map_partitions
// and turns them into the partition-aware void* trampolines required by the
// engine. Supported user function shapes (all partition-aware):
//   1) Emitter form:
//        void(const operators::MapPartitionsOperator::PartitionInfo&,
//             const In* data, std::size_t n, auto emit)
//      where `emit(const Out&)` can be called zero or more times.
//   2) Vector form:
//        std::vector<Out>(const PartitionInfo&, const In* data, std::size_t n)
//   3) Init-list form:
//        std::initializer_list<Out>(const PartitionInfo&, const In* data, std::size_t n)
//   4) Pointer+count view:
//        std::pair<const Out*, std::size_t>(const PartitionInfo&, const In* data, std::size_t n)

// --- form detection tailored for MapPartitions (with PartitionInfo, In*, n) ---
namespace detail {
  using Part = operators::MapPartitionsOperator::PartitionInfo;

  template <class Out>
  struct EmitProbeMP {
    void operator()(const Out&) const noexcept; // declaration only
  };

  template <class Fn, class In, class Out>
  constexpr bool is_emitter_form_mp_v =
      std::is_invocable_v<Fn, const Part&, const In*, std::size_t, EmitProbeMP<Out>>;

  template <class Fn, class In, class Out>
  constexpr bool is_vector_form_mp_v =
      std::is_invocable_r_v<std::vector<Out>, Fn, const Part&, const In*, std::size_t>;

  template <class Fn, class In, class Out>
  constexpr bool is_init_list_form_mp_v =
      std::is_invocable_r_v<std::initializer_list<Out>, Fn, const Part&, const In*, std::size_t>;

  template <class Fn, class In, class Out>
  constexpr bool is_ptr_count_form_mp_v =
      std::is_invocable_r_v<std::pair<const Out*, std::size_t>, Fn, const Part&, const In*, std::size_t>;
} // namespace detail

// ---------------- Emitter state ----------------
template <class Fn, class In, class Out>
struct MPStateEmitter {
  using Part = operators::MapPartitionsOperator::PartitionInfo;
  Fn fn;

  // Trampoline matching MapPartitionsOperator::FnWithState signature
  static void tramp(const Part& part,
                    const void* in_partition,
                    std::size_t in_count,
                    std::size_t in_elem_size,
                    void* out_partition,
                    std::size_t* out_count,
                    std::size_t out_elem_size,
                    void* st_void) {
    auto* self = static_cast<MPStateEmitter*>(st_void);

    // Type checks
    if (in_elem_size != sizeof(In) || out_elem_size != sizeof(Out)) {
      *out_count = 0; return;
    }

    const In* in = static_cast<const In*>(in_partition);
    Out* out     = static_cast<Out*>(out_partition);
    const std::size_t cap = *out_count; // engine passes capacity here

    struct RealEmit {
      Out* out;
      std::size_t cap;
      std::size_t* produced;
      void operator()(const Out& y) const {
        if (*produced < cap) {
          std::memcpy(static_cast<void*>(out + *produced), &y, sizeof(Out));
        }
        ++(*produced);
      }
    } emit{out, cap, out_count};

    *out_count = 0;
    self->fn(part, in, in_count, emit);
  }
};

// ---------------- Vector state ----------------
template <class Fn, class In, class Out>
struct MPStateVector {
  using Part = operators::MapPartitionsOperator::PartitionInfo;
  Fn fn;
  static void tramp(const Part& part,
                    const void* in_partition,
                    std::size_t in_count,
                    std::size_t in_elem_size,
                    void* out_partition,
                    std::size_t* out_count,
                    std::size_t out_elem_size,
                    void* st_void) {
    auto* self = static_cast<MPStateVector*>(st_void);
    if (in_elem_size != sizeof(In) || out_elem_size != sizeof(Out)) { *out_count = 0; return; }
    const In* in = static_cast<const In*>(in_partition);
    Out* out     = static_cast<Out*>(out_partition);
    const auto ys = self->fn(part, in, in_count);
    const std::size_t need = ys.size();
    const std::size_t cap  = *out_count;
    *out_count = need;
    const std::size_t ncpy = need < cap ? need : cap;
    if (ncpy)
      std::memcpy(out, ys.data(), ncpy * sizeof(Out));
  }
};

// ---------------- Init-list state ----------------
template <class Fn, class In, class Out>
struct MPStateInitList {
  using Part = operators::MapPartitionsOperator::PartitionInfo;
  Fn fn;
  static void tramp(const Part& part,
                    const void* in_partition,
                    std::size_t in_count,
                    std::size_t in_elem_size,
                    void* out_partition,
                    std::size_t* out_count,
                    std::size_t out_elem_size,
                    void* st_void) {
    auto* self = static_cast<MPStateInitList*>(st_void);
    if (in_elem_size != sizeof(In) || out_elem_size != sizeof(Out)) { *out_count = 0; return; }
    const In* in = static_cast<const In*>(in_partition);
    Out* out     = static_cast<Out*>(out_partition);
    const auto ys = self->fn(part, in, in_count);
    const std::size_t need = ys.size();
    const std::size_t cap  = *out_count;
    *out_count = need;
    const std::size_t ncpy = need < cap ? need : cap;
    std::size_t i = 0;
    for (const auto& y : ys) {
      if (i < ncpy) std::memcpy(out + i, &y, sizeof(Out));
      ++i;
    }
  }
};

// ---------------- Ptr+count state ----------------
template <class Fn, class In, class Out>
struct MPStatePtrCount {
  using Part = operators::MapPartitionsOperator::PartitionInfo;
  Fn fn;
  static void tramp(const Part& part,
                    const void* in_partition,
                    std::size_t in_count,
                    std::size_t in_elem_size,
                    void* out_partition,
                    std::size_t* out_count,
                    std::size_t out_elem_size,
                    void* st_void) {
    auto* self = static_cast<MPStatePtrCount*>(st_void);
    if (in_elem_size != sizeof(In) || out_elem_size != sizeof(Out)) { *out_count = 0; return; }
    const In* in = static_cast<const In*>(in_partition);
    Out* out     = static_cast<Out*>(out_partition);
    const auto view = self->fn(part, in, in_count);
    const Out* p = view.first; const std::size_t need = view.second;
    const std::size_t cap = *out_count;
    *out_count = need;
    const std::size_t ncpy = need < cap ? need : cap;
    if (ncpy) std::memcpy(out, p, ncpy * sizeof(Out));
  }
};

template <class In, class Out, class Fn>
inline OpHandler<operators::MapPartitionsOperator>
make_map_partitions_op(Fn fn) {
  OpHandler<operators::MapPartitionsOperator> h{operators::MapPartitionsOperator(sizeof(In), sizeof(Out)), nullptr};

  if constexpr (detail::is_emitter_form_mp_v<Fn, In, Out>) {
    using S = MPStateEmitter<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else if constexpr (detail::is_vector_form_mp_v<Fn, In, Out>) {
    using S = MPStateVector<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else if constexpr (detail::is_init_list_form_mp_v<Fn, In, Out>) {
    using S = MPStateInitList<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else if constexpr (detail::is_ptr_count_form_mp_v<Fn, In, Out>) {
    using S = MPStatePtrCount<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else {
    // Fallback to emitter-form for generic lambdas where detection may fail.
    using S = MPStateEmitter<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  }
  return h;
}

}  // namespace adapters
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ADAPTERS_MAP_PARTITIONS_ADAPTER_H
