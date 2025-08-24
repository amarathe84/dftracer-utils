#ifndef __DFTRACER_UTILS_PIPELINE_ADAPTERS_FLATMAP_ADAPTER_H
#define __DFTRACER_UTILS_PIPELINE_ADAPTERS_FLATMAP_ADAPTER_H

#include <dftracer/utils/pipeline/adapters/adapter.h>
#include <dftracer/utils/pipeline/operators/flatmap_operator.h>

#include <initializer_list>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace adapters {

// form: fn(const In&) -> std::vector<Out>
template <class Fn, class In, class Out>
struct FlatMapStateEmitter {
  Fn fn;
  static void tramp(const void* in_elem, operators::FlatMapOperator::Emitter em,
                    void* st) {
    auto* s = static_cast<FlatMapStateEmitter*>(st);
    const In& x = *static_cast<const In*>(in_elem);
    struct RealEmit {
      operators::FlatMapOperator::Emitter em;
      void operator()(const Out& y) const {
        em.emit(em.ctx, static_cast<const void*>(&y));
      }
    } emit{em};
    s->fn(x, emit);
  }
};

// form: fn(const In&) -> std::vector<Out>
template <class Fn, class In, class Out>
struct FlatMapStateVector {
  Fn fn;
  static void tramp(const void* in_elem, operators::FlatMapOperator::Emitter em,
                    void* st) {
    auto* s = static_cast<FlatMapStateVector*>(st);
    const In& x = *static_cast<const In*>(in_elem);
    const std::vector<Out> ys = s->fn(x);
    for (const auto& y : ys) em.emit(em.ctx, static_cast<const void*>(&y));
  }
};

// form: fn(const In&) -> std::initializer_list<Out>
template <class Fn, class In, class Out>
struct FlatMapStateInitList {
  Fn fn;
  static void tramp(const void* in_elem, operators::FlatMapOperator::Emitter em,
                    void* st) {
    auto* s = static_cast<FlatMapStateInitList*>(st);
    const In& x = *static_cast<const In*>(in_elem);
    const std::initializer_list<Out> ys = s->fn(x);
    for (const auto& y : ys) em.emit(em.ctx, static_cast<const void*>(&y));
  }
};

// form: fn(const In&) -> std::pair<const Out*, std::size_t>
template <class Fn, class In, class Out>
struct FlatMapStatePtrCount {
  Fn fn;
  static void tramp(const void* in_elem, operators::FlatMapOperator::Emitter em,
                    void* st) {
    auto* s = static_cast<FlatMapStatePtrCount*>(st);
    const In& x = *static_cast<const In*>(in_elem);
    const std::pair<const Out*, std::size_t> view = s->fn(x);
    const Out* p = view.first;
    const std::size_t n = view.second;
    for (std::size_t i = 0; i < n; ++i)
      em.emit(em.ctx, static_cast<const void*>(p + i));
  }
};

template <class In, class Out, class Fn>
inline OpHandler<operators::FlatMapOperator> make_flatmap_op(
    Fn fn, double expansion_hint = -1.0) {
  OpHandler<operators::FlatMapOperator> h{
      operators::FlatMapOperator(sizeof(In), sizeof(Out)), nullptr};
  h.op.expansion_hint = expansion_hint;

  if constexpr (detail::supports_emitter_v<Fn, In, Out>) {
    using S = FlatMapStateEmitter<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else if constexpr (std::is_invocable_r_v<std::vector<Out>, Fn, const In&>) {
    using S = FlatMapStateVector<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else if constexpr (std::is_invocable_r_v<std::initializer_list<Out>, Fn,
                                             const In&>) {
    using S = FlatMapStateInitList<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else if constexpr (std::is_invocable_r_v<std::pair<const Out*, std::size_t>,
                                             Fn, const In&>) {
    using S = FlatMapStatePtrCount<Fn, In, Out>;
    auto state = std::make_shared<S>(S{std::move(fn)});
    h.op.fn_with_state = &S::tramp;
    h.op.state = state.get();
    h.state = std::move(state);
  } else {
    static_assert(sizeof(Fn) == 0,
                  "make_flatmap_op expects either emitter form void(const In&, "
                  "auto emit) or returns vector/span of Out");
  }
  return h;
}

}  // namespace adapters
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ADAPTERS_FLATMAP_ADAPTER_H
