#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/pipeline/pipeline.h>

namespace dftracer::utils {

// Unified task access methods
Task* ExecutorContext::get_task(TaskIndex index) const {
    if (index < static_cast<TaskIndex>(pipeline_->size())) {
        return pipeline_->get_task(index);
    } else {
        return get_dynamic_task(index);
    }
}

const std::vector<TaskIndex>& ExecutorContext::get_task_dependencies(
    TaskIndex index) const {
    if (index < static_cast<TaskIndex>(pipeline_->size())) {
        return pipeline_->get_task_dependencies(index);
    } else {
        return get_dynamic_dependencies(index);
    }
}

const std::vector<TaskIndex>& ExecutorContext::get_task_dependents(
    TaskIndex index) const {
    if (index < static_cast<TaskIndex>(pipeline_->size())) {
        return pipeline_->get_task_dependents(index);
    } else {
        return get_dynamic_dependents(index);
    }
}

TaskIndex ExecutorContext::add_dynamic_task(std::unique_ptr<Task> task,
                                            TaskIndex depends_on) {
    TaskIndex task_id =
        static_cast<TaskIndex>(pipeline_->size() + dynamic_tasks_.size());

    dynamic_tasks_.push_back(std::move(task));

    while (dynamic_dependencies_.size() <=
           static_cast<size_t>(task_id - pipeline_->size())) {
        dynamic_dependencies_.emplace_back();
        dynamic_dependents_.emplace_back();
    }

    if (depends_on >= 0) {
        add_dynamic_dependency(depends_on, task_id);
    }

    task_completed_[task_id] = false;
    dependency_count_[task_id] = (depends_on >= 0) ? 1 : 0;

    return task_id;
}

void ExecutorContext::add_dynamic_dependency(TaskIndex from, TaskIndex to) {
    if (from < static_cast<TaskIndex>(pipeline_->size()) &&
        to < static_cast<TaskIndex>(pipeline_->size())) {
        return;
    }

    std::size_t from_idx =
        (from >= static_cast<TaskIndex>(pipeline_->size()))
            ? static_cast<std::size_t>(
                  from - static_cast<TaskIndex>(pipeline_->size()))
            : 0;
    std::size_t to_idx =
        (to >= static_cast<TaskIndex>(pipeline_->size()))
            ? static_cast<std::size_t>(
                  to - static_cast<TaskIndex>(pipeline_->size()))
            : 0;

    if (from >= static_cast<TaskIndex>(pipeline_->size())) {
        while (dynamic_dependents_.size() <= from_idx) {
            dynamic_dependents_.emplace_back();
        }
        dynamic_dependents_[from_idx].push_back(to);
    }

    if (to >= static_cast<TaskIndex>(pipeline_->size())) {
        while (dynamic_dependencies_.size() <= to_idx) {
            dynamic_dependencies_.emplace_back();
        }
        dynamic_dependencies_[to_idx].push_back(from);
    }

    dependency_count_[to]++;
}

Task* ExecutorContext::get_dynamic_task(TaskIndex index) const {
    if (index < static_cast<TaskIndex>(pipeline_->size())) return nullptr;

    size_t task_idx = static_cast<size_t>(index - pipeline_->size());
    if (task_idx >= dynamic_tasks_.size()) return nullptr;

    return dynamic_tasks_[task_idx].get();
}

const std::vector<TaskIndex>& ExecutorContext::get_dynamic_dependencies(
    TaskIndex index) const {
    static const std::vector<TaskIndex> empty;
    if (index < static_cast<TaskIndex>(pipeline_->size())) return empty;

    size_t task_idx = static_cast<size_t>(index - pipeline_->size());
    if (task_idx >= dynamic_dependencies_.size()) return empty;

    return dynamic_dependencies_[task_idx];
}

const std::vector<TaskIndex>& ExecutorContext::get_dynamic_dependents(
    TaskIndex index) const {
    static const std::vector<TaskIndex> empty;
    if (index < static_cast<TaskIndex>(pipeline_->size())) return empty;

    size_t task_idx = static_cast<size_t>(index - pipeline_->size());
    if (task_idx >= dynamic_dependents_.size()) return empty;

    return dynamic_dependents_[task_idx];
}

void ExecutorContext::set_task_output(TaskIndex index, std::any output) {
    task_outputs_[index] = std::move(output);
}

std::any ExecutorContext::get_task_output(TaskIndex index) const {
    auto it = task_outputs_.find(index);
    return (it != task_outputs_.end()) ? it->second : std::any{};
}

void ExecutorContext::set_task_completed(TaskIndex index, bool completed) {
    task_completed_[index] = completed;
}

bool ExecutorContext::is_task_completed(TaskIndex index) const {
    auto it = task_completed_.find(index);
    return (it != task_completed_.end()) ? it->second.load() : false;
}

void ExecutorContext::set_dependency_count(TaskIndex index, int count) {
    dependency_count_[index] = count;
}

int ExecutorContext::get_dependency_count(TaskIndex index) const {
    auto it = dependency_count_.find(index);
    return (it != dependency_count_.end()) ? it->second : 0;
}

void ExecutorContext::decrement_dependency_count(TaskIndex index) {
    if (dependency_count_.find(index) != dependency_count_.end()) {
        dependency_count_[index]--;
    }
}

void ExecutorContext::reset() {
    // Clear all dynamic state
    dynamic_tasks_.clear();
    dynamic_dependencies_.clear();
    dynamic_dependents_.clear();

    // Clear all execution state
    task_outputs_.clear();
    task_completed_.clear();
    dependency_count_.clear();
}

bool ExecutorContext::validate() const {
    // Check if pipeline is empty
    if (is_empty()) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline is empty");
        return false;
    }

    // Check for cycles
    if (has_cycles()) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline contains cycles");
        return false;
    }

    // Validate types with executor-aware logic
    for (TaskIndex i = 0; i < static_cast<TaskIndex>(pipeline_->size()); ++i) {
        const auto& task_dependencies = get_task_dependencies(i);

        if (task_dependencies.empty()) {
            // No dependencies - entry task (input comes from pipeline input)
            continue;
        } else if (task_dependencies.size() == 1) {
            // Single dependency - direct output-to-input connection
            TaskIndex dep = task_dependencies[0];
            Task* dep_task = get_task(dep);
            Task* current_task = get_task(i);

            if (dep_task->get_output_type() != current_task->get_input_type()) {
                DFTRACER_UTILS_LOG_ERROR(
                    "Type mismatch between task %d (output: %s) and task %d "
                    "(expected input: %s)",
                    dep, dep_task->get_output_type().name(), i,
                    current_task->get_input_type().name());
                return false;
            }
        } else {
            // Multiple dependencies - executor combines into
            // std::vector<std::any> Task must expect std::vector<std::any> as
            // input
            Task* current_task = get_task(i);
            if (current_task->get_input_type() !=
                typeid(std::vector<std::any>)) {
                DFTRACER_UTILS_LOG_ERROR(
                    "Task %d has %zu dependencies but expects input type %s "
                    "instead of std::vector<std::any>",
                    i, task_dependencies.size(),
                    current_task->get_input_type().name());
                return false;
            }
        }
    }
    return true;
}

bool ExecutorContext::is_empty() const { return pipeline_->empty(); }

bool ExecutorContext::has_cycles() const { return pipeline_->has_cycles(); }

}  // namespace dftracer::utils
