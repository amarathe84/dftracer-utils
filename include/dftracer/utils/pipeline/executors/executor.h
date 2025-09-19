#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_H

#include <dftracer/utils/pipeline/executors/executor_type.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/pipeline_output.h>

#include <any>
#include <string>

namespace dftracer::utils {
class Executor {
   protected:
    Executor(ExecutorType type) : type_(type) {}

   public:
    virtual ~Executor() = default;
    virtual PipelineOutput execute(const Pipeline& pipeline, std::any input) = 0;
    inline ExecutorType type() const { return type_; }

   public:
    const ExecutorType type_;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_H
