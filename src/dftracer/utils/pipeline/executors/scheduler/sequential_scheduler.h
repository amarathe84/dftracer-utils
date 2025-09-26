#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_SEQUENTIAL_SCHEDULER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_SEQUENTIAL_SCHEDULER_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/executors/scheduler/scheduler.h>

#include <any>
#include <functional>
#include <queue>
#include <unordered_map>

namespace dftracer::utils {
class Pipeline;
class ExecutorContext;

class SequentialScheduler : public Scheduler {
   private:
    struct TaskItem {
        TaskIndex task_id;
        Task* task_ptr;
        std::any input;
        std::function<void(std::any)> completion_callback;

        TaskItem(TaskIndex id, Task* ptr, std::any inp,
                 std::function<void(std::any)> callback)
            : task_id(id),
              task_ptr(ptr),
              input(std::move(inp)),
              completion_callback(std::move(callback)) {}
    };

    std::queue<TaskItem> task_queue_;
    std::unordered_map<TaskIndex, bool>
        task_completed_;    // Track task completion status
    std::unordered_map<TaskIndex, std::size_t>
        dependency_count_;  // Track remaining dependencies per task
    ExecutorContext* current_execution_context_;  // Active execution context
                                                  // during pipeline execution

   public:
    SequentialScheduler();
    ~SequentialScheduler() = default;

    PipelineOutput execute(const Pipeline& pipeline, std::any input) override;
    void submit(TaskIndex task_id, std::any input,
                std::function<void(std::any)> completion_callback) override;
    void submit(TaskIndex task_id, Task* task_ptr, std::any input,
                std::function<void(std::any)> completion_callback) override;
    void signal_task_completion() override { /* no-op */ }

   private:
    void execute_task_with_dependencies(ExecutorContext& execution_context,
                                        TaskIndex task_id, std::any input);
    void process_all_tasks();
    void process_remaining_dynamic_tasks();
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_SEQUENTIAL_SCHEDULER_H
