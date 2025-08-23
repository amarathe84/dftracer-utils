#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_PARTITIONS_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_PARTITIONS_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <typeindex>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
template <typename T, typename Func>
class MapPartitionsOperator : public Operator {
 private:
  Func func_;

 public:
  explicit MapPartitionsOperator(Func func) : Operator(Op::MAP_PARTITIONS), func_(std::move(func)) {}

  std::type_index input_type() const override {
    return std::type_index(typeid(std::vector<T>));
  }

  std::type_index output_type() const override {
    return std::type_index(typeid(std::vector<std::vector<T>>));
  }

  const Func& function() const { return func_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_PARTITIONS_H
