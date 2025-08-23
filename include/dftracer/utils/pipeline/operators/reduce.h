#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_REDUCE_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_REDUCE_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <typeindex>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
template <typename Input, typename Output, typename Func>
class ReduceOperator : public Operator {
 private:
  Func func_;

 public:
  explicit ReduceOperator(Func func) : Operator(Op::REDUCE), func_(std::move(func)) {}

  std::type_index input_type() const override {
    return std::type_index(typeid(std::vector<Input>));
  }

  std::type_index output_type() const override {
    return std::type_index(typeid(Output));
  }

  const Func& function() const { return func_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_REDUCE_H
