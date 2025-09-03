#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_CONTEXT_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>

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

template <typename T>
struct Input {
    T value;
    explicit Input(T val) : value(std::move(val)) {}
};

struct DependsOn {
    TaskIndex id;
    explicit DependsOn(TaskIndex task_id) : id(task_id) {}
};

template <typename O>
struct TaskResult {
    TaskIndex id;
    std::future<O> future;

    TaskResult(TaskIndex task_id, std::future<O> task_future)
        : id(task_id), future(std::move(task_future)) {}

    // Move-only semantics
    TaskResult(const TaskResult&) = delete;
    TaskResult& operator=(const TaskResult&) = delete;
    TaskResult(TaskResult&&) = default;
    TaskResult& operator=(TaskResult&&) = default;
};

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
        // Create typed promise/future pair
        auto typed_promise = std::make_shared<std::promise<O>>();
        auto typed_future = typed_promise->get_future();

        // Wrap the original function to handle promise fulfillment
        auto wrapped_func = [typed_promise, func = std::move(func)](
                                I task_input, TaskContext& ctx) -> O {
            try {
                O result = func(task_input, ctx);
                typed_promise->set_value(result);
                return result;
            } catch (...) {
                typed_promise->set_exception(std::current_exception());
                throw;
            }
        };

        // Create task with wrapped function
        auto task = make_task<I, O>(std::move(wrapped_func));
        TaskIndex task_id =
            execution_context_->add_dynamic_task(std::move(task), -1);

        // Store promise in ExecutorContext (create any_promise for
        // compatibility)
        auto any_promise = std::make_shared<std::promise<std::any>>();
        execution_context_->set_task_promise(task_id, any_promise);

        // Schedule task
        schedule(task_id, std::move(input.value));

        // Return TaskResult with typed future
        return TaskResult<O>{task_id, std::move(typed_future)};
    }

    template <typename I, typename O>
    TaskResult<O> emit(std::function<O(I, TaskContext&)> func,
                       DependsOn depends_on) {
        // Create typed promise/future pair
        auto typed_promise = std::make_shared<std::promise<O>>();
        auto typed_future = typed_promise->get_future();

        // Wrap the original function to handle promise fulfillment
        auto wrapped_func = [typed_promise, func = std::move(func)](
                                I task_input, TaskContext& ctx) -> O {
            try {
                O result = func(task_input, ctx);
                typed_promise->set_value(result);
                return result;
            } catch (...) {
                typed_promise->set_exception(std::current_exception());
                throw;
            }
        };

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

        // Store promise in ExecutorContext (create any_promise for
        // compatibility)
        auto any_promise = std::make_shared<std::promise<std::any>>();
        execution_context_->set_task_promise(task_id, any_promise);

        // Don't schedule dependent tasks immediately - they will be handled by
        // scheduler's dependency resolution
        return TaskResult<O>{task_id, std::move(typed_future)};
    }

    template <typename I, typename O>
    TaskResult<O> emit(std::function<O(I, TaskContext&)> func,
                       const Input<I>& input, DependsOn depends_on) {
        // Create typed promise/future pair
        auto typed_promise = std::make_shared<std::promise<O>>();
        auto typed_future = typed_promise->get_future();

        // Wrap the original function to handle promise fulfillment
        auto wrapped_func = [typed_promise, func = std::move(func)](
                                I task_input, TaskContext& ctx) -> O {
            try {
                O result = func(task_input, ctx);
                typed_promise->set_value(result);
                return result;
            } catch (...) {
                typed_promise->set_exception(std::current_exception());
                throw;
            }
        };

        auto task = make_task<I, O>(std::move(wrapped_func));
        TaskIndex task_id = execution_context_->add_dynamic_task(
            std::move(task), depends_on.id);

        // Store promise in ExecutorContext (create any_promise for
        // compatibility)
        auto any_promise = std::make_shared<std::promise<std::any>>();
        execution_context_->set_task_promise(task_id, any_promise);

        schedule(task_id, std::move(input.value));
        return TaskResult<O>{task_id, std::move(typed_future)};
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
