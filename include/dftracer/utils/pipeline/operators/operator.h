#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_OPERATOR_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_OPERATOR_H

#include <cstdint>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context { class ExecutionContext; }
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

 public:
  const char* name;    // optional debug/telemetry label (static or long-lived)
  std::uint64_t id;    // optional stable id for DAG nodes (0 if unused)

  explicit Operator(Op op, const char* n = nullptr, std::uint64_t op_id = 0)
      : op_(op), name(n), id(op_id) {}
  virtual ~Operator() = default;

  inline Op op() const { return op_; }
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_OPERATOR_H
