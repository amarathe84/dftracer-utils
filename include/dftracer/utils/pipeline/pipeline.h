#ifndef DFTRACER_UTILS_PIPELINE_PIPELINE_H
#define DFTRACER_UTILS_PIPELINE_PIPELINE_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/task.h>

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
        dependents_;  // who this task depends on
    std::unordered_map<TaskIndex, std::any> task_outputs_;
    std::unordered_map<TaskIndex, std::atomic<bool>> task_completed_;
    std::unordered_map<TaskIndex, int> dependency_count_;

   public:
    Pipeline() = default;
    virtual ~Pipeline() = default;

    // Move-only class
    Pipeline(const Pipeline&) = delete;
    Pipeline& operator=(const Pipeline&) = delete;
    Pipeline(Pipeline&&) = default;
    Pipeline& operator=(Pipeline&&) = default;

    TaskIndex add_task(std::unique_ptr<Task> task);
    void add_dependency(TaskIndex from, TaskIndex to);

    size_t size() const { return nodes_.size(); }
    bool empty() const { return nodes_.empty(); }

    // Inline accessor methods for executors
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
        return index < nodes_.size() ? nodes_[index].get() : nullptr;
    }
    inline const std::vector<TaskIndex>& get_task_dependencies(
        TaskIndex index) const {
        return dependencies_[index];
    }
    inline const std::vector<TaskIndex>& get_task_dependents(
        TaskIndex index) const {
        return dependents_[index];
    }

    // Validation methods (now public for executors)
    bool validate_types() const;
    bool has_cycles() const;
    std::vector<TaskIndex> topological_sort() const;

   protected:
};
}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_PIPELINE_H
