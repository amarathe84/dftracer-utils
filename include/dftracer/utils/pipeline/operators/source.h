#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_SOURCE_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_SOURCE_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {

class SourceOperator : public Operator {
 public:
  // Payload: tightly packed byte buffer containing N elements.
  std::shared_ptr<std::vector<std::byte>> bytes;

  // Metadata
  std::size_t elem_size =
      0;                   // bytes per element; 0 => unknown (plan may supply)
  std::size_t stride = 0;  // 0 => tightly packed (== elem_size)
  std::uint64_t record_count = 0;  // 0 => infer from bytes/elem_size
  const char* schema = nullptr;    // optional schema/type label
  std::uint32_t partition_id =
      0;  // local partition index (for distributed contexts)
  std::uint32_t partitions = 1;    // total partitions in the dataset
  std::uint64_t content_hash = 0;  // optional checksum/fingerprint (0 => unset)

  explicit SourceOperator(std::shared_ptr<std::vector<std::byte>> b,
                          const char* name = nullptr, std::uint64_t id = 0)
      : Operator(Op::SOURCE, name, id), bytes(std::move(b)) {}

  SourceOperator(std::shared_ptr<std::vector<std::byte>> b, std::size_t elem_sz,
                 const char* name, std::uint64_t id, std::uint32_t part_id = 0,
                 std::uint32_t parts = 1)
      : Operator(Op::SOURCE, name, id),
        bytes(std::move(b)),
        elem_size(elem_sz),
        stride(0),
        record_count(0),
        schema(nullptr),
        partition_id(part_id),
        partitions(parts),
        content_hash(0) {}

  inline bool has_bytes() const noexcept { return static_cast<bool>(bytes); }
  inline bool is_packed() const noexcept { return stride == 0; }
  inline std::size_t stride_bytes() const noexcept {
    return stride == 0 ? elem_size : stride;
  }

  inline std::uint64_t count_inferred() const noexcept {
    if (record_count) return record_count;
    if (!bytes || elem_size == 0) return 0;
    return static_cast<std::uint64_t>(bytes->size() / elem_size);
  }
};

}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_SOURCE_H
