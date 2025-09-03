#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_CONTEXT_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_CONTEXT_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/tasks/task.h>

#include <any>
#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

namespace dftracer::utils {
class Pipeline;

class ExecutorContext {
   private:
    const Pipeline* pipeline_;

   public:
    ExecutorContext(const Pipeline* pipeline) : pipeline_(pipeline) {}
    ~ExecutorContext() = default;

    ExecutorContext(const ExecutorContext&) = delete;
    ExecutorContext& operator=(const ExecutorContext&) = delete;
    ExecutorContext(ExecutorContext&&) = default;
    ExecutorContext& operator=(ExecutorContext&&) = default;

    Task* get_task(TaskIndex index) const;
    const std::vector<TaskIndex>& get_task_dependencies(TaskIndex index) const;
    const std::vector<TaskIndex>& get_task_dependents(TaskIndex index) const;

    TaskIndex add_dynamic_task(std::unique_ptr<Task> task,
                               TaskIndex depends_on = -1);
    void add_dynamic_dependency(TaskIndex from, TaskIndex to);

    Task* get_dynamic_task(TaskIndex index) const;
    const std::vector<TaskIndex>& get_dynamic_dependencies(
        TaskIndex index) const;
    const std::vector<TaskIndex>& get_dynamic_dependents(TaskIndex index) const;

    void set_task_output(TaskIndex index, std::any output);
    std::any get_task_output(TaskIndex index) const;
    void set_task_completed(TaskIndex index, bool completed);
    bool is_task_completed(TaskIndex index) const;

    void set_dependency_count(TaskIndex index, int count);
    int get_dependency_count(TaskIndex index) const;
    void decrement_dependency_count(TaskIndex index);

    void reset();

    std::size_t dynamic_task_count() const { return dynamic_tasks_.size(); }

    const Pipeline* get_pipeline() const { return pipeline_; }

    bool validate() const;
    bool is_empty() const;
    bool has_cycles() const;

   private:
    std::vector<std::unique_ptr<Task>> dynamic_tasks_;
    std::vector<std::vector<TaskIndex>>
        dynamic_dependencies_;  // who depends on this task
    std::vector<std::vector<TaskIndex>>
        dynamic_dependents_;    // who this task depends on

    std::unordered_map<TaskIndex, std::any> task_outputs_;
    std::unordered_map<TaskIndex, std::atomic<bool>> task_completed_;
    std::unordered_map<TaskIndex, int> dependency_count_;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_CONTEXT_H
