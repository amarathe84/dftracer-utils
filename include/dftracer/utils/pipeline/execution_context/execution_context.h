#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H

#include <dftracer/utils/pipeline/internal.h>

#include <dftracer/utils/pipeline/operators/filter.h>
#include <dftracer/utils/pipeline/operators/map.h>
#include <dftracer/utils/pipeline/operators/reduce.h>
#include <dftracer/utils/pipeline/operators/repartition.h>
#include <dftracer/utils/pipeline/operators/groupby.h>
#include <dftracer/utils/pipeline/operators/map_partitions.h>
#include <dftracer/utils/pipeline/operators/spread.h>
#include <dftracer/utils/pipeline/operators/flatmap.h>

#include <utility>
#include <vector>
#include <stdexcept>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {

using namespace internal;

enum class ExecutionMode { SEQUENTIAL, THREADPOOL, MPI };

class ExecutionContext {
  private:
    ExecutionMode mode_;
public:
    explicit ExecutionContext(ExecutionMode mode) : mode_(mode) {}
    virtual ~ExecutionContext() = default;

    virtual ExecutionMode mode() const { return mode_; }
    virtual size_t rank() const { return 0; }
    virtual size_t size() const { return 1; }

    virtual void execute(const operators::Operator& op, const void* input, void* output) {
        switch (op.type()) {
        case operators::Op::MAP:
            map(op, input, output);
            break;
        case operators::Op::FILTER:
            filter(op, input, output);
            break;
        case operators::Op::REDUCE:
            reduce(op, input, output);
            break;
        case operators::Op::REPARTITION_BY_HASH:
            repartition_by_hash(op, input, output);
            break;
        case operators::Op::REPARTITION_BY_NUM_PARTITIONS:
            repartition_by_num_partitions(op, input, output);
            break;
        case operators::Op::REPARTITION_BY_SIZE:
            repartition_by_size(op, input, output);
            break;
        case operators::Op::GROUPBY:
            groupby(op, input, output);
            break;
        case operators::Op::MAP_PARTITIONS:
            map_partitions(op, input, output);
            break;
        case operators::Op::SPREAD:
            spread(op, input, output);
            break;
        case operators::Op::FLATMAP:
            flatmap(op, input, output);
            break;
        default:
            throw std::runtime_error("Unknown operator type");
    }
  }

protected:
  virtual void map(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void filter(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void reduce(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void repartition_by_hash(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void repartition_by_num_partitions(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void repartition_by_size(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void groupby(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void map_partitions(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void spread(const operators::Operator& op, const void* input, void* output) = 0;
  virtual void flatmap(const operators::Operator& op, const void* input, void* output) = 0;
};
}  // namespace context
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H
