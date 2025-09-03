#ifndef DFTRACER_UTILS_PIPELINE_PIPELINE_H
#define DFTRACER_UTILS_PIPELINE_PIPELINE_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <dftracer/utils/pipeline/tasks/task.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>
#include <dftracer/utils/pipeline/tasks/task_result.h>
#include <dftracer/utils/pipeline/tasks/task_tag.h>

#include <any>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace dftracer::utils {
class Pipeline {
   protected:
    std::vector<std::unique_ptr<Task>> nodes_;
    std::vector<std::vector<TaskIndex>>
        dependencies_;  // who depends on this task
    std::vector<std::vector<TaskIndex>>
        dependents_;    // who this task depends on

   public:
    Pipeline() = default;
    virtual ~Pipeline() = default;

    // Move-only class
    Pipeline(const Pipeline&) = delete;
    Pipeline& operator=(const Pipeline&) = delete;
    Pipeline(Pipeline&&) = default;
    Pipeline& operator=(Pipeline&&) = default;

    void add_dependency(TaskIndex from, TaskIndex to);

    template <typename I, typename O>
    TaskResult<O> add_task(std::function<O(I, TaskContext&)> func,
                           TaskIndex depends_on = -1) {
        auto [wrapped_func, future] =
            wrap_function_with_promise<I, O>(std::move(func));
        auto task = make_task<I, O>(std::move(wrapped_func));
        TaskIndex task_id = add_task(std::move(task), depends_on);
        return TaskResult<O>{task_id, std::move(future)};
    }

    void chain(Pipeline&& other);

    std::size_t size() const { return nodes_.size(); }
    bool empty() const { return nodes_.empty(); }

    inline const std::vector<std::unique_ptr<Task>>& get_nodes() const {
        return nodes_;
    }
    inline const std::vector<std::vector<TaskIndex>>& get_dependencies() const {
        return dependencies_;
    }
    inline const std::vector<std::vector<TaskIndex>>& get_dependents() const {
        return dependents_;
    }
    inline Task* get_task(TaskIndex index) const {
        if (index < 0) return nullptr;
        return index < static_cast<TaskIndex>(nodes_.size())
                   ? nodes_[index].get()
                   : nullptr;
    }
    inline const std::vector<TaskIndex>& get_task_dependencies(
        TaskIndex index) const {
        return dependencies_[index];
    }
    inline const std::vector<TaskIndex>& get_task_dependents(
        TaskIndex index) const {
        return dependents_[index];
    }

    bool validate_types() const;
    bool has_cycles() const;
    std::vector<TaskIndex> topological_sort() const;

   protected:
    TaskIndex add_task(std::unique_ptr<Task> task, TaskIndex depends_on = -1);
};
}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_PIPELINE_H
