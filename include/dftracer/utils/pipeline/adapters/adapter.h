#ifndef __DFTRACER_UTILS_PIPELINE_ADAPTERS_ADAPTER_H
#define __DFTRACER_UTILS_PIPELINE_ADAPTERS_ADAPTER_H

#include <memory>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace adapters {
template <class Op>
struct OpHandler {
  Op op;
  std::shared_ptr<void> state;
};


namespace detail {
template <class Out>
struct EmitProbe {
  void operator()(const Out&) const noexcept;
};

template <class Fn, class In, class Out>
constexpr bool supports_emitter_v =
    std::is_invocable_v<Fn, const In&, EmitProbe<Out>>;
}  // namespace detail
}  // namespace adapters
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ADAPTERS_ADAPTER_H
