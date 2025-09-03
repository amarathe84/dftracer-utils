#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/pipeline/pipeline.h>

namespace dftracer::utils {

// Unified task access methods
Task* ExecutorContext::get_task(TaskIndex index) const {
    if (index < pipeline_->size()) {
        // Static task from pipeline
        return pipeline_->get_task(index);
    } else {
        // Dynamic task
        return get_dynamic_task(index);
    }
}

const std::vector<TaskIndex>& ExecutorContext::get_task_dependencies(TaskIndex index) const {
    if (index < pipeline_->size()) {
        // Static task dependencies from pipeline
        return pipeline_->get_task_dependencies(index);
    } else {
        // Dynamic task dependencies
        return get_dynamic_dependencies(index);
    }
}

const std::vector<TaskIndex>& ExecutorContext::get_task_dependents(TaskIndex index) const {
    if (index < pipeline_->size()) {
        // Static task dependents from pipeline
        return pipeline_->get_task_dependents(index);
    } else {
        // Dynamic task dependents
        return get_dynamic_dependents(index);
    }
}

TaskIndex ExecutorContext::add_dynamic_task(std::unique_ptr<Task> task, TaskIndex depends_on) {
    // Dynamic task IDs start right after static tasks
    TaskIndex task_id = pipeline_->size() + dynamic_tasks_.size();
    
    // Add task
    dynamic_tasks_.push_back(std::move(task));
    
    // Ensure dependency vectors are sized correctly
    while (dynamic_dependencies_.size() <= static_cast<size_t>(task_id - pipeline_->size())) {
        dynamic_dependencies_.emplace_back();
        dynamic_dependents_.emplace_back();
    }
    
    // Add dependency if specified
    if (depends_on >= 0) {
        add_dynamic_dependency(depends_on, task_id);
    }
    
    // Initialize execution state
    task_completed_[task_id] = false;
    dependency_count_[task_id] = (depends_on >= 0) ? 1 : 0;
    
    return task_id;
}

void ExecutorContext::add_dynamic_dependency(TaskIndex from, TaskIndex to) {
    // Only handle dependencies where at least one task is dynamic
    if (from < pipeline_->size() && to < pipeline_->size()) {
        // Both static - this shouldn't happen in normal usage, but ignore
        return;
    }
    
    // Calculate indices for dynamic task vectors
    size_t from_idx = (from >= pipeline_->size()) ? static_cast<size_t>(from - pipeline_->size()) : 0;
    size_t to_idx = (to >= pipeline_->size()) ? static_cast<size_t>(to - pipeline_->size()) : 0;
    
    // Ensure vectors are sized correctly for dynamic tasks
    if (from >= pipeline_->size()) {
        while (dynamic_dependents_.size() <= from_idx) {
            dynamic_dependents_.emplace_back();
        }
        dynamic_dependents_[from_idx].push_back(to);
    }
    
    if (to >= pipeline_->size()) {
        while (dynamic_dependencies_.size() <= to_idx) {
            dynamic_dependencies_.emplace_back();
        }
        dynamic_dependencies_[to_idx].push_back(from);
    }
    
    // Update dependency count
    dependency_count_[to]++;
}

Task* ExecutorContext::get_dynamic_task(TaskIndex index) const {
    if (index < pipeline_->size()) return nullptr;  // Not a dynamic task
    
    size_t task_idx = static_cast<size_t>(index - pipeline_->size());
    if (task_idx >= dynamic_tasks_.size()) return nullptr;
    
    return dynamic_tasks_[task_idx].get();
}

const std::vector<TaskIndex>& ExecutorContext::get_dynamic_dependencies(TaskIndex index) const {
    static const std::vector<TaskIndex> empty;
    if (index < pipeline_->size()) return empty;
    
    size_t task_idx = static_cast<size_t>(index - pipeline_->size());
    if (task_idx >= dynamic_dependencies_.size()) return empty;
    
    return dynamic_dependencies_[task_idx];
}

const std::vector<TaskIndex>& ExecutorContext::get_dynamic_dependents(TaskIndex index) const {
    static const std::vector<TaskIndex> empty;
    if (index < pipeline_->size()) return empty;
    
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

} // namespace dftracer::utils