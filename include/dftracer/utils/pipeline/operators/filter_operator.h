#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_FILTER_OPERATOR_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_FILTER_OPERATOR_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <cstddef>
#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {

class FilterOperator : public Operator {
 public:
  using Predicate = bool (*)(const void* in_elem);
  using PredicateWithState = bool (*)(const void* in_elem, void* state);

  std::size_t in_size = 0;
  Predicate pred = nullptr;
  PredicateWithState pred_with_state = nullptr;
  void* state = nullptr;
  double selectivity_hint =
      -1.0;  // -1 => unknown; else expected keep ratio [0,1]

  explicit FilterOperator(std::size_t in_sz, Predicate p = nullptr,
                          const char* name = nullptr, std::uint64_t op_id = 0)
      : Operator(Op::FILTER, name, op_id),
        in_size(in_sz),
        pred(p),
        pred_with_state(nullptr),
        state(nullptr) {}
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_FILTER_OPERATOR_H
