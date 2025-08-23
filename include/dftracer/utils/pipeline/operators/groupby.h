#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_GROUPBY_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_GROUPBY_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <typeindex>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
template <typename Input, typename Output, typename KeyFunc, typename AggFunc>
class GroupByOperator : public Operator {
 private:
  KeyFunc key_func_;
  AggFunc agg_func_;

 public:
  GroupByOperator(KeyFunc key_func, AggFunc agg_func)
      : Operator(Op::GROUPBY), key_func_(std::move(key_func)), agg_func_(std::move(agg_func)) {}

  std::type_index input_type() const override {
    return std::type_index(typeid(std::vector<Input>));
  }

  std::type_index output_type() const override {
    return std::type_index(typeid(std::vector<Output>));
  }

  const KeyFunc& key_function() const { return key_func_; }
  const AggFunc& agg_function() const { return agg_func_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_GROUPBY_H
