#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_RESULT_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_RESULT_H

#include <dftracer/utils/common/typedefs.h>

#include <functional>
#include <future>
#include <memory>

namespace dftracer::utils {

class TaskContext;

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

// Utility function to wrap any function with promise fulfillment
template <typename I, typename O>
std::pair<std::function<O(I, TaskContext&)>, std::future<O>>
wrap_function_with_promise(std::function<O(I, TaskContext&)> func) {
    auto typed_promise = std::make_shared<std::promise<O>>();
    auto typed_future = typed_promise->get_future();

    auto wrapped_func = [typed_promise, func = std::move(func)](
                            I task_input, TaskContext& ctx) -> O {
        try {
            O result = func(task_input, ctx);
            try {
                typed_promise->set_value(result);
            } catch (const std::future_error& e) {
                // Promise already set, ignore
            }
            return result;
        } catch (...) {
            try {
                typed_promise->set_exception(std::current_exception());
            } catch (const std::future_error& e) {
                // Promise already set, ignore
            }
            throw;
        }
    };

    return {std::move(wrapped_func), std::move(typed_future)};
}

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_TASK_RESULT_H
