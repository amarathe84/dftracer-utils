#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SEQUENTIAL_EXECUTOR_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SEQUENTIAL_EXECUTOR_H

#include <dftracer/utils/pipeline/executors/executor.h>

namespace dftracer::utils {

class SequentialExecutor : public Executor {
   public:
    SequentialExecutor();
    ~SequentialExecutor() override = default;
    SequentialExecutor(const SequentialExecutor&) = delete;
    SequentialExecutor& operator=(const SequentialExecutor&) = delete;
    SequentialExecutor(SequentialExecutor&&) = default;
    SequentialExecutor& operator=(SequentialExecutor&&) = default;

    std::any execute(const Pipeline& pipeline, std::any input, bool gather = true) override;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_SEQUENTIAL_EXECUTOR_H
