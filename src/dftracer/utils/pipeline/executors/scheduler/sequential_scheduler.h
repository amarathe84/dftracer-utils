#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_SEQUENTIAL_SCHEDULER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_SEQUENTIAL_SCHEDULER_H

#include <dftracer/utils/pipeline/executors/scheduler/scheduler.h>
#include <dftracer/utils/common/typedefs.h>

#include <any>
#include <functional>
#include <queue>
#include <unordered_map>

namespace dftracer::utils {

// Forward declarations
class Pipeline;
class ExecutorContext;

/**
 * Simple sequential scheduler for single-threaded execution
 * Handles dynamic task emission by maintaining a task queue
 */
class SequentialScheduler : public Scheduler {
private:
    // Task queue for dynamically emitted tasks
    struct TaskItem {
        TaskIndex task_id;
        Task* task_ptr;
        std::any input;
        std::function<void(std::any)> completion_callback;
        
        TaskItem(TaskIndex id, Task* ptr, std::any inp, std::function<void(std::any)> callback)
            : task_id(id), task_ptr(ptr), input(std::move(inp)), completion_callback(std::move(callback)) {}
    };
    
    std::queue<TaskItem> task_queue_;
    std::unordered_map<TaskIndex, std::any> task_outputs_;
    ExecutorContext* current_execution_context_;  // Active execution context during pipeline execution
    
public:
    SequentialScheduler();
    ~SequentialScheduler() = default;
    
    // Scheduler implementation
    std::any execute(const Pipeline& pipeline, std::any input) override;
    void submit(TaskIndex task_id, std::any input,
               std::function<void(std::any)> completion_callback) override;
    void submit(TaskIndex task_id, Task* task_ptr, std::any input,
               std::function<void(std::any)> completion_callback) override;
    void signal_task_completion() override { /* no-op for sequential */ }
    
    // Process all queued tasks sequentially
    void process_queued_tasks();
};

} // namespace dftracer::utils

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_SEQUENTIAL_SCHEDULER_H