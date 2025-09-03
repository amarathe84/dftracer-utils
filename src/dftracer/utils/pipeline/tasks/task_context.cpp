#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.h>
#include <dftracer/utils/pipeline/executors/scheduler/thread_scheduler.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>

namespace dftracer::utils {

void TaskContext::add_dependency(TaskIndex from, TaskIndex to) {
    if (!execution_context_) {
        throw std::runtime_error("TaskContext: No execution context available");
    }

    // ExecutorContext handles both static and dynamic dependencies
    execution_context_->add_dynamic_dependency(from, to);
}

void TaskContext::schedule(TaskIndex task_id, std::any input,
                           TaskIndex depends_on) {
    if (!scheduler_) {
        return;
    }

    // Submit all dynamic tasks - the scheduler will handle dependency
    // resolution
    auto completion_callback = [scheduler = scheduler_](std::any result) {
        // Signal completion for dynamic tasks (matching pipeline task behavior)
        if (scheduler) {
            scheduler->signal_task_completion();
        }
    };

    Task* task_ptr = execution_context_->get_task(task_id);
    if (!task_ptr) {
        return;
    }

    scheduler_->submit(task_id, task_ptr, std::move(input),
                       completion_callback);
}

}  // namespace dftracer::utils
