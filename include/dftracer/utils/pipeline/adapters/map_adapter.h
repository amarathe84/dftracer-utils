#ifndef __DFTRACER_UTILS_PIPELINE_ADAPTERS_MAP_ADAPTER_H
#define __DFTRACER_UTILS_PIPELINE_ADAPTERS_MAP_ADAPTER_H

#include <dftracer/utils/pipeline/adapters/adapter.h>
#include <dftracer/utils/pipeline/operators/map_operator.h>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace adapters {

namespace detail {
template <class Fn, class In, class Out>
struct MapStateKernel {
  Fn fn;
  static void tramp(const void* in, void* out, void* st) {
    auto* s = static_cast<MapStateKernel*>(st);
    const In& ti = *static_cast<const In*>(in);
    Out& to = *static_cast<Out*>(out);
    s->fn(ti, to);
  }
};

template <class Fn, class In, class Out>
struct MapStateTransform {
  Fn fn;
  static void tramp(const void* in, void* out, void* st) {
    auto* s = static_cast<MapStateTransform*>(st);
    const In& ti = *static_cast<const In*>(in);
    Out& to = *static_cast<Out*>(out);
    to = s->fn(ti);
  }
};
}  // namespace detail

// Function pointer (kernel form): void(const In&, Out&)
template <class In, class Out>
inline OpHandler<operators::MapOperator> make_map_op(void (*fp)(const In&,
                                                                Out&)) {
  OpHandler<operators::MapOperator> h{
      operators::MapOperator(sizeof(In), sizeof(Out)), nullptr};
  // Fast path: no state; use stateful slot to avoid extra templated trampoline
  // reuse
  using S = detail::MapStateKernel<decltype(fp), In, Out>;
  static_assert(std::is_same_v<decltype(fp), void (*)(const In&, Out&)>);
  h.op.fn_with_state = +[](const void* in, void* out, void* st) {
    auto* s =
        reinterpret_cast<S*>(st);  // st carries the function pointer value
    const In& ti = *static_cast<const In*>(in);
    Out& to = *static_cast<Out*>(out);
    s->fn(ti, to);
  };
  // Store the function pointer in a small heap object so we pass a stable
  // pointer
  auto state = std::make_shared<S>(S{fp});
  h.op.state = state.get();
  h.state = std::move(state);
  return h;
}

// Generic callable (capturing or not). Accepts either kernel or transform
// forms.
template <class In, class Out, class Fn>
inline OpHandler<operators::MapOperator> make_map_op(Fn fn) {
  OpHandler<operators::MapOperator> h{
      operators::MapOperator(sizeof(In), sizeof(Out)), nullptr};

  // Decide which trampoline to use based on invocability
  if constexpr (std::is_invocable_v<Fn, const In&, Out&>) {
    using S = detail::MapStateKernel<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else if constexpr (std::is_invocable_r_v<Out, Fn, const In&>) {
    using S = detail::MapStateTransform<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else {
    static_assert(
        sizeof(Fn) == 0,
        "make_map_op expects Fn to be void(const In&, Out&) or Out(const In&)");
  }
  return h;
}

}  // namespace adapters
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ADAPTERS_MAP_ADAPTER_H
