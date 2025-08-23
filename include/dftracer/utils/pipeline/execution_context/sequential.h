#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H

#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/internal.h>

#include <utility>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {

// forward declarations
class ThreadPoolContext;
class MPIContext;

using namespace internal;

class SequentialContext : public ExecutionContext {
public:
    SequentialContext() : ExecutionContext(ExecutionMode::SEQUENTIAL) {}

    friend class ThreadPoolContext;
    friend class MPIContext;

protected:
    void map(const operators::Operator& op, const void* input, void* output) override;
    void filter(const operators::Operator& op, const void* input, void* output) override;
    void reduce(const operators::Operator& op, const void* input, void* output) override;
    void repartition_by_hash(const operators::Operator& op, const void* input, void* output) override;
    void repartition_by_num_partitions(const operators::Operator& op, const void* input, void* output) override;
    void repartition_by_size(const operators::Operator& op, const void* input, void* output) override;
    void groupby(const operators::Operator& op, const void* input, void* output) override;
    void map_partitions(const operators::Operator& op, const void* input, void* output) override;
    void spread(const operators::Operator& op, const void* input, void* output) override;
    void flatmap(const operators::Operator& op, const void* input, void* output) override;
};
}  // namespace context
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H
