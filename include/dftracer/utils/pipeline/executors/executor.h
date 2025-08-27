#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_H

#include <any>
#include <string>

#include <dftracer/utils/pipeline/executors/executor_type.h>
#include <dftracer/utils/pipeline/pipeline.h>

namespace dftracer::utils {
class Executor {
protected:
    Executor(ExecutorType type) : type_(type) {}

public:
    virtual ~Executor() = default;
    virtual std::any execute(const Pipeline& pipeline, std::any input, bool gather = true) = 0;
    inline ExecutorType type() const { return type_; }
public:
    const ExecutorType type_;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_H
