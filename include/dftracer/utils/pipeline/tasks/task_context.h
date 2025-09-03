#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H

#include <dftracer/utils/common/typedefs.h>

#include <functional>
#include <typeindex>
#include <memory>
#include <any>
#include <stdexcept>

namespace dftracer::utils {

// Forward declarations
class Pipeline;
class Task;
class TaskContext;
class SchedulerInterface;

// Scheduler registry functions for TaskContext
void set_current_scheduler(SchedulerInterface* scheduler);

// Forward declaration for make_task
template<typename I, typename O>
class FunctionTask;
template<typename I, typename O>
std::unique_ptr<FunctionTask<I, O>> make_task(
    std::function<O(I, TaskContext&)> func);

/**
 * TaskContext provides a coroutine-style interface for task emission
 * Allows tasks to dynamically emit new tasks during execution
 */
class TaskContext {
private:
    Pipeline* pipeline_;
    TaskIndex current_task_id_;
    
    
public:
    TaskContext(Pipeline* pipeline, TaskIndex current_task_id)
        : pipeline_(pipeline), current_task_id_(current_task_id) {}
    
    /**
     * Emit a new typed task with input data and optional dependency
     * Returns TaskIndex that can be used for dependency tracking
     * @param func The function to execute
     * @param input Input data for the task (only used if depends_on == -1)
     * @param depends_on Task ID this task depends on (-1 for independent)
     */
    template<typename I, typename O>
    TaskIndex emit(std::function<O(I, TaskContext&)> func, I input, TaskIndex depends_on = -1) {
        auto task = make_task<I, O>(std::move(func));
        
        // Type validation: if task depends on another, validate types match
        if (depends_on >= 0) {
            auto* dep_task = pipeline_->get_task(depends_on);
            if (dep_task && dep_task->get_output_type() != task->get_input_type()) {
                throw std::invalid_argument(
                    "Type mismatch: dependency output type " +
                    std::string(dep_task->get_output_type().name()) +
                    " doesn't match task input type " +
                    std::string(task->get_input_type().name()));
            }
        }
        
        TaskIndex task_id = emit_internal(std::move(task), depends_on);
        
        // Schedule for execution
        schedule(task_id, std::move(input), depends_on);
        
        return task_id;
    }
    
    /**
     * Emit a new typed task without input data - will receive input from dependency
     * @param func The function to execute
     * @param depends_on Task ID this task depends on (-1 for independent)
     */
    template<typename I, typename O>
    TaskIndex emit(std::function<O(I, TaskContext&)> func, TaskIndex depends_on = -1) {
        auto task = make_task<I, O>(std::move(func));
        
        // Type validation: if task depends on another, validate types match
        if (depends_on >= 0) {
            auto* dep_task = pipeline_->get_task(depends_on);
            if (dep_task && dep_task->get_output_type() != task->get_input_type()) {
                throw std::invalid_argument(
                    "Type mismatch: dependency output type " +
                    std::string(dep_task->get_output_type().name()) +
                    " doesn't match task input type " +
                    std::string(task->get_input_type().name()));
            }
        }
        
        TaskIndex task_id = emit_internal(std::move(task), depends_on);
        
        // Schedule for execution (input will come from dependency)
        schedule(task_id, std::any{}, depends_on);
        
        return task_id;
    }

    TaskIndex current() const { return current_task_id_; }
    void add_dependency(TaskIndex from, TaskIndex to);
    Pipeline* get_pipeline() const { return pipeline_; }

private:
    // Internal helper for task creation with atomic dependency setup
    TaskIndex emit_internal(std::unique_ptr<Task> task, TaskIndex depends_on = -1);
    
    // Schedule emitted task with scheduler integration
    void schedule(TaskIndex task_id, std::any input, TaskIndex depends_on);
    
};

} // namespace dftracer::utils

#endif // DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H
