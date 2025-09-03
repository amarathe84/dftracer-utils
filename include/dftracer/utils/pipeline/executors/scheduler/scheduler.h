#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/tasks/task.h>

#include <any>
#include <functional>
#include <memory>

namespace dftracer::utils {
class Pipeline;

class Scheduler {
   public:
    virtual ~Scheduler() = default;
    virtual std::any execute(const Pipeline& pipeline, std::any input) = 0;
    virtual void submit(TaskIndex task_id, std::any input,
                        std::function<void(std::any)> completion_callback) = 0;
    virtual void submit(TaskIndex task_id, Task* task_ptr, std::any input,
                        std::function<void(std::any)> completion_callback) = 0;
    virtual void signal_task_completion() = 0;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H
