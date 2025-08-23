#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_FLATMAP_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_FLATMAP_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <typeindex>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
template <typename Input, typename Output, typename Func>
class FlatMapOperator : public Operator {
 private:
  Func func_;

 public:
  explicit FlatMapOperator(Func flat_map_func)
      : Operator(Op::FLATMAP), func_(std::move(flat_map_func)) {}

  std::type_index input_type() const override {
    return std::type_index(typeid(std::vector<Input>));
  }

  std::type_index output_type() const override {
    return std::type_index(typeid(std::vector<Output>));
  }

  const Func& function() const { return func_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_FLATMAP_H
