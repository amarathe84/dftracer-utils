#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H

#include <dftracer/utils/pipeline/tasks/task.h>
#include <dftracer/utils/common/typedefs.h>

#include <any>
#include <functional>
#include <memory>

namespace dftracer::utils {

// Forward declaration
class Pipeline;

/**
 * Abstract interface for task schedulers
 * Uses the same API as the existing thread scheduler for consistency
 */
class Scheduler {
public:
    virtual ~Scheduler() = default;
    
    /**
     * Execute a pipeline with the given input
     */
    virtual std::any execute(const Pipeline& pipeline, std::any input) = 0;
    
    /**
     * Submit a task to the scheduler
     */
    virtual void submit(TaskIndex task_id, std::any input,
                       std::function<void(std::any)> completion_callback) = 0;
                           
    /**
     * Submit a task with direct Task pointer (for dynamic tasks)
     */
    virtual void submit(TaskIndex task_id, Task* task_ptr, std::any input,
                       std::function<void(std::any)> completion_callback) = 0;
    
    /**
     * Signal that a task has completed (for dynamic task completion tracking)
     */
    virtual void signal_task_completion() = 0;
};

} // namespace dftracer::utils

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H
