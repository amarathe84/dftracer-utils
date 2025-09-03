#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <mutex>

namespace dftracer::utils {

TaskIndex Pipeline::add_task(std::unique_ptr<Task> task, TaskIndex depends_on) {
    TaskIndex index = nodes_.size();
    nodes_.push_back(std::move(task));
    dependencies_.push_back({});
    dependents_.push_back({});
    task_completed_[index] = false;
    dependency_count_[index] = 0;
    if (depends_on >= 0) {
        add_dependency(depends_on, index);
    }
    return index;
}

TaskIndex Pipeline::safe_add_task(std::unique_ptr<Task> task, TaskIndex depends_on) {
    static std::mutex task_addition_mutex;
    std::lock_guard<std::mutex> lock(task_addition_mutex);

    TaskIndex index = add_task(std::move(task), depends_on);
    return index;
}


void Pipeline::add_dependency(TaskIndex from, TaskIndex to) {
    if (from >= nodes_.size() || to >= nodes_.size()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Invalid task index");
    }

    // For edge: from -> to
    dependencies_[to].push_back(from);  // "to" depends on "from"
    dependents_[from].push_back(to);    // "from" has dependent "to"
    dependency_count_[to]++;
}

void Pipeline::safe_add_dependency(TaskIndex from, TaskIndex to) {
    static std::mutex dependency_addition_mutex;
    std::lock_guard<std::mutex> lock(dependency_addition_mutex);
    add_dependency(from, to);
}

bool Pipeline::validate_types() const {
    for (TaskIndex i = 0; i < nodes_.size(); ++i) {
        for (TaskIndex dependent : dependencies_[i]) {
            if (nodes_[dependent]->get_output_type() !=
                nodes_[i]->get_input_type()) {
                DFTRACER_UTILS_LOG_ERROR(
                    "Type mismatch between task %d (output: %s) and task %d "
                    "(expected input: %s)",
                    dependent, nodes_[dependent]->get_output_type().name(), i,
                    nodes_[i]->get_input_type().name());
                return false;
            }
        }
    }
    return true;
}

bool Pipeline::has_cycles() const {
    std::vector<int> in_degree(nodes_.size(), 0);
    for (TaskIndex i = 0; i < nodes_.size(); ++i) {
        in_degree[i] = dependency_count_.at(i);
    }

    std::queue<TaskIndex> queue;
    for (TaskIndex i = 0; i < nodes_.size(); ++i) {
        if (in_degree[i] == 0) {
            queue.push(i);
        }
    }

    std::size_t processed = 0;
    while (!queue.empty()) {
        TaskIndex current = queue.front();
        queue.pop();
        processed++;

        for (TaskIndex dependent : dependents_[current]) {
            in_degree[dependent]--;
            if (in_degree[dependent] == 0) {
                queue.push(dependent);
            }
        }
    }

    return processed != nodes_.size();
}

std::vector<TaskIndex> Pipeline::topological_sort() const {
    std::vector<TaskIndex> result;
    std::vector<int> in_degree(nodes_.size(), 0);

    for (TaskIndex i = 0; i < nodes_.size(); ++i) {
        in_degree[i] = dependency_count_.at(i);
    }

    std::queue<TaskIndex> queue;
    for (TaskIndex i = 0; i < nodes_.size(); ++i) {
        if (in_degree[i] == 0) {
            queue.push(i);
        }
    }

    while (!queue.empty()) {
        TaskIndex current = queue.front();
        queue.pop();
        result.push_back(current);

        for (TaskIndex dependent : dependents_[current]) {
            in_degree[dependent]--;
            if (in_degree[dependent] == 0) {
                queue.push(dependent);
            }
        }
    }

    return result;
}

void Pipeline::chain(Pipeline&& other) {
    if (other.empty()) {
        return;
    }

    TaskIndex offset = nodes_.size();

    for (auto& task : other.nodes_) {
        nodes_.push_back(std::move(task));
        dependencies_.push_back({});
        dependents_.push_back({});
        task_completed_[nodes_.size() - 1] = false;
        dependency_count_[nodes_.size() - 1] = 0;
    }

    for (TaskIndex i = 0; i < other.dependencies_.size(); ++i) {
        TaskIndex new_index = offset + i;
        for (TaskIndex dep : other.dependencies_[i]) {
            TaskIndex new_dep = offset + dep;
            dependencies_[new_index].push_back(new_dep);
            dependents_[new_dep].push_back(new_index);
            dependency_count_[new_index]++;
        }
    }
}

TaskContext Pipeline::create_context(TaskIndex task_id) {
    return TaskContext(this, task_id);
}

}  // namespace dftracer::utils
