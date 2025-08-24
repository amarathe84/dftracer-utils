#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_PARTITIONS_OPERATOR_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_PARTITIONS_OPERATOR_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <cstddef>
#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {

class MapPartitionsOperator : public Operator {
 public:
  struct PartitionInfo {
    std::size_t partition_index =
        0;  // index of this partition within the current context
    std::size_t partitions_in_context =
        1;  // total partitions executed in this context
    std::size_t upstream_offset_elems =
        0;  // starting element offset into the logical upstream stream
    std::size_t upstream_count_elems =
        0;                       // number of input elements in this partition
    std::size_t world_rank = 0;  // process/rank id (0 if single-process)
    std::size_t world_size =
        1;  // number of processes/ranks (1 if single-process)
  };

  using Fn = void (*)(const PartitionInfo& part, const void* in_partition,
                      std::size_t in_count, std::size_t in_elem_size,
                      void* out_partition, std::size_t* out_count,
                      std::size_t out_elem_size);

  using FnWithState = void (*)(const PartitionInfo& part,
                               const void* in_partition, std::size_t in_count,
                               std::size_t in_elem_size, void* out_partition,
                               std::size_t* out_count,
                               std::size_t out_elem_size, void* state);

  std::size_t in_elem_size;
  std::size_t out_elem_size;

  Fn fn = nullptr;
  FnWithState fn_with_state = nullptr;
  void* state = nullptr;

  MapPartitionsOperator(std::size_t in_elem_sz, std::size_t out_elem_sz,
                        Fn stateless_fn = nullptr,
                        const char* op_name = nullptr, std::uint64_t op_id = 0)
      : Operator(Op::MAP_PARTITIONS, op_name, op_id),
        in_elem_size(in_elem_sz),
        out_elem_size(out_elem_sz),
        fn(stateless_fn),
        fn_with_state(nullptr),
        state(nullptr) {}
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_MAP_PARTITIONS_OPERATOR_H
