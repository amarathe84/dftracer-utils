#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_OPERATOR_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_OPERATOR_H

#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {
class ExecutionContext;
}
namespace operators {

enum class Op {
  SOURCE,
  MAP,
  FILTER,
  REDUCE,
  MAP_PARTITIONS,
  SPREAD,
  FLATMAP,
  REPARTITION_BY_HASH,
  REPARTITION_BY_NUM_PARTITIONS,
  REPARTITION_BY_SIZE,
  GROUPBY_AGG,
  DISTINCT,
  JOIN,
  SORT,
  SHUFFLE
};

class Operator {
 protected:
  Op op_;

 private:
  const char* name_;
  std::uint64_t id_;

 public:
  explicit Operator(Op op, const char* n = nullptr, std::uint64_t op_id = 0)
      : op_(op), name_(n), id_(op_id) {}
  virtual ~Operator() = default;

  inline Op op() const { return op_; }
  inline const char* name() const { return name_; }
  inline std::uint64_t id() const { return id_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_OPERATOR_H
