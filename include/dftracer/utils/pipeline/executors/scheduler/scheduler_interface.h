#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_INTERFACE_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_INTERFACE_H

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
class SchedulerInterface {
public:
    virtual ~SchedulerInterface() = default;
    
    /**
     * Execute a pipeline with the given input
     */
    virtual std::any execute_pipeline(const Pipeline& pipeline, std::any input) = 0;
    
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
     * Set pipeline for current execution
     */
    virtual void set_pipeline(const Pipeline* pipeline) = 0;
};

/**
 * Factory for creating schedulers
 */
class SchedulerFactory {
public:
    enum class Type {
        SEQUENTIAL,  // Single-threaded scheduler
        THREAD_POOL  // Multi-threaded work-stealing scheduler
    };
    
    static std::unique_ptr<SchedulerInterface> create(Type type, std::size_t num_threads = 1);
};

} // namespace dftracer::utils

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_INTERFACE_H