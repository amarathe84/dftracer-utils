#ifndef __DFTRACER_UTILS_PIPELINE_ADAPTERS_FILTER_ADAPTER_H
#define __DFTRACER_UTILS_PIPELINE_ADAPTERS_FILTER_ADAPTER_H

#include <dftracer/utils/pipeline/adapters/adapter.h>
#include <dftracer/utils/pipeline/operators/filter_operator.h>

#include <memory>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace adapters {

namespace detail {
template <class Pred, class T>
struct FilterState {
  Pred pred;
  static bool tramp(const void* in, void* st) {
    auto* s = static_cast<FilterState*>(st);
    const T& ti = *static_cast<const T*>(in);
    return s->pred(ti);
  }
};
}  // namespace detail

// Function pointer predicate
template <class T>
inline OpHandler<operators::FilterOperator> make_filter_op(
    bool (*pred)(const T&)) {
  using S = detail::FilterState<decltype(pred), T>;
  auto state = std::make_shared<S>(S{pred});

  OpHandler<operators::FilterOperator> h{operators::FilterOperator(sizeof(T)),
                                         nullptr};
  h.op.pred_with_state = &S::tramp;
  h.op.state = state.get();
  h.state = std::move(state);
  return h;
}

// Generic callable (captures ok): bool(const T&)
template <class T, class Pred>
inline OpHandler<operators::FilterOperator> make_filter_op(Pred pred) {
  using S = detail::FilterState<Pred, T>;
  auto state = std::make_shared<S>(S{std::move(pred)});

  OpHandler<operators::FilterOperator> h{operators::FilterOperator(sizeof(T)),
                                         nullptr};
  h.op.pred_with_state = &S::tramp;
  h.op.state = state.get();
  h.state = std::move(state);
  return h;
}

}  // namespace adapters
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ADAPTERS_FILTER_ADAPTER_H
