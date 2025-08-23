#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_FILTER_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_FILTER_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <typeindex>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
template <typename T, typename Pred>
class FilterOperator : public Operator {
 private:
  Pred predicate_;

 public:
  explicit FilterOperator(Pred pred) : Operator(Op::FILTER), predicate_(std::move(pred)) {}

  std::type_index input_type() const override {
    return std::type_index(typeid(std::vector<T>));
  }

  std::type_index output_type() const override {
    return std::type_index(typeid(std::vector<T>));
  }

  const Pred& predicate() const { return predicate_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_FILTER_H
