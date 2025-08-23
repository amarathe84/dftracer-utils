#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_FLATMAP_OPERATOR_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_FLATMAP_OPERATOR_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <cstddef>
#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
class FlatMapOperator : public Operator {
 public:
  struct Emitter {
    void (*emit)(void* ctx, const void* out_elem) = nullptr;
    void* ctx = nullptr;
  };

  using Fn = void (*)(const void* in_elem, Emitter out);
  using FnWithState = void (*)(const void* in_elem, Emitter out, void* state);

  std::size_t in_size = 0;
  std::size_t out_size = 0;

  Fn fn = nullptr;
  FnWithState fn_with_state = nullptr;
  void* state = nullptr;

  double expansion_hint = -1.0;  // expected outputs per input; -1 => unknown

  FlatMapOperator(std::size_t in_sz, std::size_t out_sz, Fn stateless = nullptr,
                  const char* name = nullptr, std::uint64_t op_id = 0)
      : Operator(Op::FLATMAP, name, op_id),
        in_size(in_sz),
        out_size(out_sz),
        fn(stateless),
        fn_with_state(nullptr),
        state(nullptr) {}
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_FLATMAP_OPERATOR_H
