#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_SPREAD_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_SPREAD_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <typeindex>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
template <typename T, typename Func>
class SpreadOperator : public Operator {
 private:
  Func func_;

 public:
  explicit SpreadOperator(Func spread_func)
      : func_(std::move(spread_func)) {}

  std::type_index input_type() const override {
    return std::type_index(typeid(std::vector<T>));
  }

  std::type_index output_type() const override {
    return std::type_index(typeid(std::vector<T>));
  }

  const Func& function() const { return func_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_SPREAD_H
