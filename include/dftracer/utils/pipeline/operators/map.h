#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_H

#include <dftracer/utils/pipeline/operators/operator.h>
#include <cstddef>
#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
class MapOperator : public Operator {
 public:
  using Fn = void (*)(const void* in_elem, void* out_elem);
  using FnWithState  = void (*)(const void* in_elem, void* out_elem, void* state);

  std::size_t     in_size;                 // size of one input element in bytes
  std::size_t     out_size;                // size of one output element in bytes
  Fn              fn = nullptr;            // pure function (no captures)
  FnWithState     fn_with_state = nullptr; // optional stateful trampoline
  void*           state = nullptr;         // opaque pointer for state

  MapOperator(std::size_t in_sz,
              std::size_t out_sz,
              Fn stateless_fn = nullptr,
              const char* op_name = nullptr,
              std::uint64_t op_id = 0)
      : Operator(Op::MAP, op_name, op_id),
        in_size(in_sz),
        out_size(out_sz),
        fn(stateless_fn),
        fn_with_state(nullptr),
        state(nullptr) {}
};

}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_H
