#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>

namespace dftracer::utils {

SequentialScheduler::SequentialScheduler()
    : current_execution_context_(nullptr) {}

std::any SequentialScheduler::execute(const Pipeline& pipeline,
                                      std::any input) {
    ExecutorContext execution_context(&pipeline);
    current_execution_context_ = &execution_context;

    if (!execution_context.validate()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline validation failed");
    }

    // Clear previous state
    task_outputs_.clear();
    task_completed_.clear();
    dependency_count_.clear();
    while (!task_queue_.empty()) {
        task_queue_.pop();
    }

    TaskIndex initial_pipeline_size = static_cast<TaskIndex>(pipeline.size());

    // Initialize dependency counts and completion status
    for (TaskIndex i = 0; i < initial_pipeline_size; ++i) {
        task_completed_[i] = false;
        dependency_count_[i] =
            execution_context.get_task_dependencies(i).size();
    }

    // Execute tasks with no dependencies first (like ThreadScheduler)
    for (TaskIndex i = 0; i < initial_pipeline_size; ++i) {
        if (execution_context.get_task_dependencies(i).empty()) {
            execute_task_with_dependencies(execution_context, i, input);
            process_all_tasks();
        }
    }

    // Find the terminal tasks (those that no other tasks depend on)
    std::vector<TaskIndex> terminal_tasks;
    for (TaskIndex i = 0; i < static_cast<TaskIndex>(pipeline.size()); ++i) {
        if (execution_context.get_task_dependents(i).empty()) {
            terminal_tasks.push_back(i);
        }
    }

    current_execution_context_ = nullptr;

    if (!terminal_tasks.empty()) {
        return task_outputs_[terminal_tasks.back()];
    }

    return input;
}

void SequentialScheduler::submit(
    TaskIndex task_id, std::any input,
    std::function<void(std::any)> completion_callback) {
    submit(task_id, nullptr, std::move(input), std::move(completion_callback));
}

void SequentialScheduler::submit(
    TaskIndex task_id, Task* task_ptr, std::any input,
    std::function<void(std::any)> completion_callback) {
    task_queue_.emplace(task_id, task_ptr, std::move(input),
                        std::move(completion_callback));
    DFTRACER_UTILS_LOG_DEBUG("SequentialScheduler: Queued task %d", task_id);
}

void SequentialScheduler::execute_task_with_dependencies(
    ExecutorContext& execution_context, TaskIndex task_id, std::any input) {
    auto completion_callback = [this, &execution_context,
                                task_id](std::any result) {
        task_outputs_[task_id] = std::move(result);
        task_completed_[task_id] = true;

        for (TaskIndex dependent :
             execution_context.get_task_dependents(task_id)) {
            if (--dependency_count_[dependent] == 0) {
                // All dependencies satisfied - submit the dependent task
                std::any dependent_input;

                if (execution_context.get_task_dependencies(dependent).size() ==
                    1) {
                    // Single dependency
                    dependent_input = task_outputs_[task_id];
                } else {
                    // Multiple dependencies - combine inputs
                    std::vector<std::any> combined_inputs;
                    for (TaskIndex dep :
                         execution_context.get_task_dependencies(dependent)) {
                        combined_inputs.push_back(task_outputs_[dep]);
                    }
                    dependent_input = combined_inputs;
                }

                // Submit dependent task with recursive callback
                execute_task_with_dependencies(execution_context, dependent,
                                               std::move(dependent_input));
            }
        }
    };

    Task* task_ptr = execution_context.get_task(task_id);
    submit(task_id, task_ptr, std::move(input), completion_callback);
}

void SequentialScheduler::process_all_tasks() {
    while (!task_queue_.empty()) {
        TaskItem task = std::move(task_queue_.front());
        task_queue_.pop();

        try {
            std::any result;

            if (task.task_ptr) {
                if (task.task_ptr->needs_context()) {
                    TaskContext task_context(this, current_execution_context_,
                                             task.task_id);
                    task.task_ptr->setup_context(&task_context);
                }

                DFTRACER_UTILS_LOG_INFO(
                    "SequentialScheduler: Executing task %d, input type %s",
                    task.task_id, task.input.type().name());
                result = task.task_ptr->execute(task.input);
                DFTRACER_UTILS_LOG_DEBUG(
                    "SequentialScheduler: Executed task %d", task.task_id);
            } else {
                DFTRACER_UTILS_LOG_WARN(
                    "SequentialScheduler: No task pointer for task %d, using "
                    "input as result",
                    task.task_id);
                result = task.input;
            }

            task_outputs_[task.task_id] = result;

            if (task.completion_callback) {
                task.completion_callback(result);
            }

        } catch (const std::exception& e) {
            DFTRACER_UTILS_LOG_ERROR(
                "SequentialScheduler: Exception executing task %d: %s",
                task.task_id, e.what());

            // Still call callback to avoid hanging
            if (task.completion_callback) {
                task.completion_callback(std::any{});
            }
        }
    }

    // Process any dynamic tasks that became ready during execution
    process_remaining_dynamic_tasks();
}

void SequentialScheduler::process_remaining_dynamic_tasks() {
    if (!current_execution_context_) return;

    std::size_t dynamic_count =
        current_execution_context_->dynamic_task_count();
    std::size_t pipeline_size =
        current_execution_context_->get_pipeline()->size();

    for (std::size_t idx = 0; idx < dynamic_count; ++idx) {
        TaskIndex task_id = static_cast<TaskIndex>(pipeline_size + idx);
        // Skip if already executed
        if (task_outputs_.find(task_id) != task_outputs_.end()) {
            continue;
        }

        Task* task_ptr = current_execution_context_->get_dynamic_task(task_id);
        if (!task_ptr) continue;

        auto dependencies =
            current_execution_context_->get_dynamic_dependencies(task_id);

        bool all_deps_ready = true;
        for (TaskIndex dep_id : dependencies) {
            if (task_outputs_.find(dep_id) == task_outputs_.end()) {
                all_deps_ready = false;
                break;
            }
        }

        if (!all_deps_ready) continue;

        std::any task_input;

        if (dependencies.empty()) {
            task_input = std::any{};  // Independent task
        } else if (dependencies.size() == 1) {
            task_input = task_outputs_[dependencies[0]];
        } else {
            std::vector<std::any> combined_inputs;
            for (TaskIndex dep_id : dependencies) {
                combined_inputs.push_back(task_outputs_[dep_id]);
            }
            task_input = combined_inputs;
        }

        auto completion_callback = [this, task_id](std::any result) {
            task_outputs_[task_id] = std::move(result);
        };

        submit(task_id, task_ptr, std::move(task_input), completion_callback);
    }
}

}  // namespace dftracer::utils
