#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/pipeline/tasks/task_result.h>
#include <dftracer/utils/pipeline/tasks/task_tag.h>

#include <any>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <typeindex>

namespace dftracer::utils {

class Task;
class TaskContext;
class Scheduler;

template <typename I, typename O>
class FunctionTask;
template <typename I, typename O>
std::unique_ptr<FunctionTask<I, O>> make_task(
    std::function<O(I, TaskContext&)> func);

class TaskContext {
   private:
    Scheduler* scheduler_;
    ExecutorContext* execution_context_;
    TaskIndex current_task_id_;

   public:
    TaskContext(Scheduler* scheduler, ExecutorContext* execution_context,
                TaskIndex current_task_id)
        : scheduler_(scheduler),
          execution_context_(execution_context),
          current_task_id_(current_task_id) {}

    template <typename I, typename O>
    TaskResult<O> emit(std::function<O(I, TaskContext&)> func,
                       const Input<I>& input) {
        auto [wrapped_func, future] =
            wrap_function_with_promise<I, O>(std::move(func));
        auto task = make_task<I, O>(std::move(wrapped_func));
        TaskIndex task_id =
            execution_context_->add_dynamic_task(std::move(task), -1);
        schedule(task_id, std::move(input.value));
        return TaskResult<O>{task_id, std::move(future)};
    }

    template <typename I, typename O>
    TaskResult<O> emit(std::function<O(I, TaskContext&)> func,
                       DependsOn depends_on) {
        auto [wrapped_func, future] =
            wrap_function_with_promise<I, O>(std::move(func));

        auto task = make_task<I, O>(std::move(wrapped_func));
        if (depends_on.id >= 0) {
            Task* dep_task = execution_context_->get_task(depends_on.id);
            if (dep_task && task->get_input_type() != typeid(std::any) &&
                dep_task->get_output_type() != task->get_input_type()) {
                throw std::invalid_argument(
                    "Type mismatch: dependency output type " +
                    std::string(dep_task->get_output_type().name()) +
                    " doesn't match task input type " +
                    std::string(task->get_input_type().name()));
            }
        }

        TaskIndex task_id = execution_context_->add_dynamic_task(
            std::move(task), depends_on.id);
        return TaskResult<O>{task_id, std::move(future)};
    }

    template <typename I, typename O>
    TaskResult<O> emit(std::function<O(I, TaskContext&)> func,
                       const Input<I>& input, DependsOn depends_on) {
        auto [wrapped_func, future] =
            wrap_function_with_promise<I, O>(std::move(func));
        auto task = make_task<I, O>(std::move(wrapped_func));
        TaskIndex task_id = execution_context_->add_dynamic_task(
            std::move(task), depends_on.id);

        schedule(task_id, std::move(input.value));
        return TaskResult<O>{task_id, std::move(future)};
    }

    TaskIndex current() const { return current_task_id_; }
    void add_dependency(TaskIndex from, TaskIndex to);
    ExecutorContext* get_execution_context() const {
        return execution_context_;
    }

   private:
    void schedule(TaskIndex task_id, std::any input);
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H
