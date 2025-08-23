#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H

#include <dftracer/utils/pipeline/execution_context/execution_context.h>

#include <cstddef>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {

class SequentialContext : public ExecutionContext {
 public:
  SequentialContext() = default;
  ~SequentialContext() override = default;

  std::size_t concurrency() const noexcept override { return 1; }
  void parallel_for(std::size_t n, const ForTask& task) override {
    for (std::size_t i = 0; i < n; ++i) task(i);
  }

  bool is_distributed() const noexcept override { return false; }

  std::size_t rank() const noexcept override { return 0; }

  std::size_t size() const noexcept override { return 1; }

  void barrier() override {}
};
}  // namespace context
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H
