#include <dftracer/utils/pipeline/tasks/task_context.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/executors/thread/scheduler.h>
#include <dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.h>

namespace dftracer::utils {

// Thread-local scheduler registry
thread_local SchedulerInterface* current_scheduler = nullptr;

// Helper function to get the current scheduler
static SchedulerInterface* get_current_scheduler() {
    // First try thread-local scheduler (for sequential execution)
    if (current_scheduler) {
        return current_scheduler;
    }
    
    // Fallback to GlobalScheduler (for threaded execution)
    return GlobalScheduler::get_instance();
}

// Function to set the current scheduler for this thread
void set_current_scheduler(SchedulerInterface* scheduler) {
    current_scheduler = scheduler;
}

void TaskContext::add_dependency(TaskIndex from, TaskIndex to) {
    if (!pipeline_) {
        // Dummy context for dynamic tasks - don't allow dependency changes
        throw std::runtime_error("TaskContext: Dynamic task attempted to modify dependencies (not allowed)");
    }
    pipeline_->add_dependency(from, to);
}

TaskIndex TaskContext::emit_internal(std::unique_ptr<Task> task, TaskIndex depends_on) {
    if (!pipeline_) {
        // Dummy context for dynamic tasks - don't allow emission to prevent recursion
        throw std::runtime_error("TaskContext: Dynamic task attempted to emit another task (not allowed to prevent infinite recursion)");
    }
    return pipeline_->safe_add_task(std::move(task), depends_on);
}

void TaskContext::schedule(TaskIndex task_id, std::any input, TaskIndex depends_on) {
    auto* scheduler = get_current_scheduler();
    if (!scheduler) {
        // If no scheduler available, skip scheduling
        // This shouldn't happen in normal execution
        return;
    }
    
    if (depends_on == -1) {
        // Independent task - schedule immediately
        auto completion_callback = [scheduler](std::any result) {
            // Signal completion for independent tasks (only for GlobalScheduler)
            auto* global_scheduler = dynamic_cast<GlobalScheduler*>(scheduler);
            if (global_scheduler) {
                global_scheduler->signal_task_completion();
            }
            // Sequential scheduler doesn't need special completion signaling
        };
        
        scheduler->submit(task_id, std::move(input), completion_callback);
    } else {
        // Dependent task - will be scheduled when dependency completes
        // The scheduler's dependency tracking will handle this
    }
}

} // namespace dftracer::utils
