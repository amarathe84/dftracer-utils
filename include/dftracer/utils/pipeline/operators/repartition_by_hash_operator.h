#ifndef __DFTRACER_UTILS_PIPELINE_OPERATORS_REPARTITION_BY_HASH_OPERATOR_H
#define __DFTRACER_UTILS_PIPELINE_OPERATORS_REPARTITION_BY_HASH_OPERATOR_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <cstddef>
#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {

class RepartitionByHashOperator : public Operator {
 public:
  using HashFn = std::uint64_t (*)(const void* in_elem);
  using HashFnWithState = std::uint64_t (*)(const void* in_elem, void* state);
  std::size_t elem_size;
  std::size_t num_partitions;
  std::uint64_t seed;
  bool stable_within_partition;
  HashFn hash_fn = nullptr;
  HashFnWithState hash_fn_with_state = nullptr;
  void* state = nullptr;

  // 64-bit FNV-1a offset basis (canonical value)
  static constexpr std::uint64_t DEFAULT_FNV1A64_SEED = 14695981039346656037ULL;

  explicit RepartitionByHashOperator(
      std::size_t elem_sz, std::size_t n_parts,
      std::uint64_t hash_seed = DEFAULT_FNV1A64_SEED, bool stable = true,
      HashFn stateless = nullptr)
      : Operator(Op::REPARTITION_BY_HASH),
        elem_size(elem_sz),
        num_partitions(n_parts),
        seed(hash_seed),
        stable_within_partition(stable),
        hash_fn(stateless),
        hash_fn_with_state(nullptr),
        state(nullptr) {}

  RepartitionByHashOperator(std::size_t elem_sz, std::size_t n_parts,
                            std::uint64_t hash_seed, bool stable,
                            HashFnWithState stateful, void* st)
      : Operator(Op::REPARTITION_BY_HASH),
        elem_size(elem_sz),
        num_partitions(n_parts),
        seed(hash_seed),
        stable_within_partition(stable),
        hash_fn(nullptr),
        hash_fn_with_state(stateful),
        state(st) {}
};

}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATORS_REPARTITION_BY_HASH_OPERATOR_H
