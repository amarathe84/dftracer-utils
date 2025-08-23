#ifndef __DFTRACER_UTILS_PIPELINE_ADAPtER_ADAPTER_H
#define __DFTRACER_UTILS_PIPELINE_ADAPtER_ADAPTER_H

namespace dftracer {
namespace utils {
namespace pipeline {
namespace adapters {
template <class Op>
struct OpHandle {
  Op op;
  std::shared_ptr<void> state;
};
}  // namespace adapters
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ADAPTERS_ADAPTER_H
