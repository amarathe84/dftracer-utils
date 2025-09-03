#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/pipeline/executors/scheduler/thread_scheduler.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <thread>

namespace dftracer::utils {
ThreadScheduler::ThreadScheduler() : current_execution_context_(nullptr) {}

ThreadScheduler::~ThreadScheduler() { shutdown(); }

void ThreadScheduler::initialize(std::size_t num_threads) {
    shutdown();

    should_terminate_ = false;
    workers_ready_ = false;
    active_tasks_ = 0;

    // Reset all state variables after shutdown
    {
        std::lock_guard<std::mutex> lock(cv_mutex_);
        task_outputs_.clear();
        task_completed_.clear();
        dependency_count_.clear();
        current_execution_context_ = nullptr;
    }

    queues_.clear();
    queues_.reserve(num_threads);

    for (std::size_t i = 0; i < num_threads; ++i) {
        auto queue = std::make_unique<TaskQueue>();
        queues_.push_back(std::move(queue));
    }

    for (std::size_t i = 0; i < queues_.size(); ++i) {
        if (!queues_[i]) {
            throw std::runtime_error("Failed to create TaskQueue " +
                                     std::to_string(i));
        }
    }

    workers_.clear();
    workers_.reserve(num_threads);

    for (std::size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&ThreadScheduler::worker_thread, this, i);
    }

    // Signal workers that they can start processing
    workers_ready_ = true;
    cv_.notify_all();
}

void ThreadScheduler::shutdown() {
    {
        std::lock_guard<std::mutex> lock(cv_mutex_);
        should_terminate_ = true;
        workers_ready_ = false;
    }
    cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    workers_.clear();
    queues_.clear();

    // Reset counters after threads are joined
    {
        std::lock_guard<std::mutex> lock(cv_mutex_);
        active_tasks_ = 0;
        task_outputs_.clear();
        task_completed_.clear();
        dependency_count_.clear();
        current_execution_context_ = nullptr;
    }

    DFTRACER_UTILS_LOG_INFO("GlobalScheduler shutdown complete");
}

void ThreadScheduler::submit(
    TaskIndex task_id, std::any input,
    std::function<void(std::any)> completion_callback) {
    submit(task_id, nullptr, std::move(input), std::move(completion_callback));
}

void ThreadScheduler::submit(
    TaskIndex task_id, Task* task_ptr, std::any input,
    std::function<void(std::any)> completion_callback) {
    if (queues_.empty()) {
        throw std::runtime_error(
            "GlobalScheduler not initialized - no task queues available");
    }

    TaskItem task(task_id, task_ptr, std::move(input),
                  std::move(completion_callback));

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<std::size_t> dist(0, queues_.size() - 1);
    std::size_t queue_id = dist(gen);

    queues_[queue_id]->push(std::move(task));

    active_tasks_++;

    // Notify one waiting thread
    cv_.notify_one();
}

std::any ThreadScheduler::execute(const Pipeline& pipeline, std::any input) {
    ExecutorContext execution_context(&pipeline);
    current_execution_context_ = &execution_context;

    if (!execution_context.validate()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline validation failed");
    }

    task_outputs_.clear();
    task_completed_.clear();
    dependency_count_.clear();

    TaskIndex initial_pipeline_size = static_cast<TaskIndex>(pipeline.size());

    for (TaskIndex i = 0; i < initial_pipeline_size; ++i) {
        task_completed_[i] = false;
        dependency_count_[i] =
            execution_context.get_task_dependencies(i).size();
    }

    for (TaskIndex i = 0; i < initial_pipeline_size; ++i) {
        if (execution_context.get_task_dependencies(i).empty()) {
            // Submit immediately with original input
            auto completion_callback = [this, &execution_context,
                                        i](std::any result) {
                task_outputs_[i] = std::move(result);
                task_completed_[i] = true;

                // Process dependent tasks
                for (TaskIndex dependent :
                     execution_context.get_task_dependents(i)) {
                    if (--dependency_count_[dependent] == 0) {
                        // All dependencies satisfied - submit the dependent
                        // task
                        std::any dependent_input;

                        if (execution_context.get_task_dependencies(dependent)
                                .size() == 1) {
                            // Single dependency
                            dependent_input = task_outputs_[i];
                        } else {
                            // Multiple dependencies - combine inputs
                            std::vector<std::any> combined_inputs;
                            for (TaskIndex dep :
                                 execution_context.get_task_dependencies(
                                     dependent)) {
                                combined_inputs.push_back(task_outputs_[dep]);
                            }
                            dependent_input = combined_inputs;
                        }

                        // Submit dependent task with recursive callback
                        submit_with_dependency_handling(
                            execution_context, dependent,
                            std::move(dependent_input));
                    }
                }

                // Decrement active tasks counter
                active_tasks_--;
                if (active_tasks_ == 0) {
                    cv_.notify_all();
                }
            };

            Task* task_ptr = execution_context.get_task(i);
            submit(i, task_ptr, input, completion_callback);
        }
    }

    // Wait for ALL tasks (main + dynamically emitted) to complete
    // The key insight: we need to wait until both conditions are stable:
    // 1. No active tasks being processed (active_tasks_ == 0)
    // 2. No queued tasks waiting to be processed (all queues empty)

    while (true) {
        // Wait for current active tasks to complete
        wait_for_completion();

        // Check if any new tasks were emitted during execution
        if (!queues_empty() && active_tasks_ == 0) {
            // Wake up workers to process queued tasks
            cv_.notify_all();
        } else if (queues_empty() && active_tasks_ == 0) {
            // No work left - we're done
            break;
        }
    }

    // Find the terminal tasks (those that no other tasks depend on)
    std::vector<TaskIndex> terminal_tasks;
    for (TaskIndex i = 0; i < static_cast<TaskIndex>(pipeline.size()); ++i) {
        if (execution_context.get_task_dependents(i).empty()) {
            terminal_tasks.push_back(i);
        }
    }

    // Clean up execution context reference
    current_execution_context_ = nullptr;

    // Return output of the last terminal task
    if (!terminal_tasks.empty()) {
        return task_outputs_[terminal_tasks.back()];
    }

    return input;
}

void ThreadScheduler::worker_thread(size_t thread_id) {
    {
        std::unique_lock<std::mutex> lock(cv_mutex_);
        cv_.wait(lock, [this] { return workers_ready_ || should_terminate_; });
    }

    if (should_terminate_) {
        return;
    }

    // Random number generator for selecting queues to steal from
    std::random_device rd;
    std::mt19937 gen(rd());

    while (!should_terminate_) {
        TaskItem task;
        bool found_task = false;

        found_task = queues_[thread_id]->pop(task);

        // If no task in own queue, try to steal from other queues
        if (!found_task) {
            std::vector<std::size_t> queue_indices(queues_.size());
            std::iota(queue_indices.begin(), queue_indices.end(), 0);
            std::shuffle(queue_indices.begin(), queue_indices.end(), gen);

            for (std::size_t i : queue_indices) {
                if (i != thread_id && queues_[i]->steal(task)) {
                    found_task = true;
                    break;
                }
            }
        }

        if (found_task) {
            try {
                TaskIndex task_id = task.task_id;
                std::any result;

                if (task.task_ptr) {
                    if (task.task_ptr->needs_context()) {
                        TaskContext task_context(
                            this, current_execution_context_, task_id);
                        task.task_ptr->setup_context(&task_context);
                    }
                    result = task.task_ptr->execute(task.input);
                    DFTRACER_UTILS_LOG_DEBUG("Worker %zu executed task %d",
                                             thread_id, task_id);
                } else {
                    // Fallback for tasks without pointer (shouldn't happen in
                    // normal execution)
                    DFTRACER_UTILS_LOG_WARN(
                        "Worker %zu: No task pointer for task %d, using input "
                        "as result",
                        thread_id, task_id);
                    result = task.input;
                }

                {
                    std::lock_guard<std::mutex> lock(results_mutex_);
                    task_outputs_[task_id] = result;
                }

                if (task.completion_callback) {
                    task.completion_callback(result);
                }
            } catch (const std::exception& e) {
                DFTRACER_UTILS_LOG_ERROR(
                    "Exception in worker thread %zu executing task %d: %s",
                    thread_id, task.task_id, e.what());
                // Still call callback to avoid hanging the pipeline
                if (task.completion_callback) {
                    task.completion_callback(std::any{});
                }
            }
        } else {
            // No task found, wait for notification
            std::unique_lock<std::mutex> lock(cv_mutex_);
            if (!should_terminate_) {
                // Wait for new tasks to be submitted or termination signal
                cv_.wait(lock, [this]() {
                    return should_terminate_ || !queues_empty();
                });
            } else {
                break;
            }
        }
    }

    DFTRACER_UTILS_LOG_INFO("Worker thread %zu terminated", thread_id);
}

void ThreadScheduler::wait_for_completion() {
    std::unique_lock<std::mutex> lock(cv_mutex_);
    cv_.wait(lock, [this]() { return active_tasks_ == 0 && queues_empty(); });
}

void ThreadScheduler::signal_task_completion() {
    active_tasks_--;
    // Always notify to wake up main thread for completion check
    cv_.notify_all();
}

void ThreadScheduler::submit_with_dependency_handling(
    ExecutorContext& execution_context, TaskIndex task_id, std::any input) {
    auto completion_callback = [this, &execution_context,
                                task_id](std::any result) {
        task_outputs_[task_id] = std::move(result);
        task_completed_[task_id] = true;

        // Process dependent tasks recursively
        for (TaskIndex dependent :
             execution_context.get_task_dependents(task_id)) {
            if (--dependency_count_[dependent] == 0) {
                // All dependencies satisfied - prepare input
                std::any dependent_input;

                if (execution_context.get_task_dependencies(dependent).size() ==
                    1) {
                    // Single dependency
                    dependent_input = task_outputs_[task_id];
                } else {
                    // Multiple dependencies
                    std::vector<std::any> combined_inputs;
                    for (TaskIndex dep :
                         execution_context.get_task_dependencies(dependent)) {
                        combined_inputs.push_back(task_outputs_[dep]);
                    }
                    dependent_input = combined_inputs;
                }

                // Submit dependent task recursively
                submit_with_dependency_handling(execution_context, dependent,
                                                std::move(dependent_input));
            }
        }

        active_tasks_--;
        // Always notify to check for completion
        cv_.notify_all();
    };

    Task* task_ptr = execution_context.get_task(task_id);
    submit(task_id, task_ptr, std::move(input), completion_callback);
}

bool ThreadScheduler::queues_empty() const {
    for (const auto& queue : queues_) {
        if (!queue->empty()) {
            return false;
        }
    }
    return true;
}

}  // namespace dftracer::utils
