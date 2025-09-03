#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/scheduler/thread_scheduler.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>

#include <algorithm>
#include <chrono>
#include <random>
#include <thread>
#include <iostream>

namespace dftracer::utils {
ThreadScheduler::ThreadScheduler() : current_execution_context_(nullptr) {}

ThreadScheduler::~ThreadScheduler() {
    shutdown();
}

void ThreadScheduler::initialize(std::size_t num_threads) {
    // Ensure clean state
    shutdown();
    
    should_terminate_ = false;
    workers_ready_ = false;  // Workers should wait
    active_tasks_ = 0;
    
    // Create task queues for each worker thread
    queues_.clear();
    queues_.reserve(num_threads);  // Reserve space to avoid reallocation
    
    for (std::size_t i = 0; i < num_threads; ++i) {
        auto queue = std::make_unique<TaskQueue>();
        queues_.push_back(std::move(queue));
    }
    
    // Verify all queues are valid before starting worker threads
    for (std::size_t i = 0; i < queues_.size(); ++i) {
        if (!queues_[i]) {
            throw std::runtime_error("Failed to create TaskQueue " + std::to_string(i));
        }
    }
    
    // Start worker threads AFTER all queues are verified
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
    // Signal all threads to terminate
    should_terminate_ = true;
    
    // Wake up all waiting threads
    cv_.notify_all();
    
    // Join all worker threads
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    
    workers_.clear();
    queues_.clear();
    
    DFTRACER_UTILS_LOG_INFO("GlobalScheduler shutdown complete");
}

void ThreadScheduler::submit(TaskIndex task_id, std::any input,
                             std::function<void(std::any)> completion_callback) {
    // This method should be called with task pointer directly
    submit(task_id, nullptr, std::move(input), std::move(completion_callback));
}

void ThreadScheduler::submit(TaskIndex task_id, Task* task_ptr, std::any input,
                             std::function<void(std::any)> completion_callback) {
    // Safety check: ensure queues are initialized
    if (queues_.empty()) {
        throw std::runtime_error("GlobalScheduler not initialized - no task queues available");
    }
    
    TaskItem task(task_id, task_ptr, std::move(input), std::move(completion_callback));
    
    // Choose a random queue to submit the task
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<std::size_t> dist(0, queues_.size() - 1);
    std::size_t queue_id = dist(gen);

    queues_[queue_id]->push(std::move(task));
    
    // Increment active tasks counter
    active_tasks_++;
    
    // Notify one waiting thread
    cv_.notify_one();
}

std::any ThreadScheduler::execute(const Pipeline& pipeline, std::any input) {
    
    // Create ExecutorContext to manage runtime state
    ExecutorContext execution_context(&pipeline);
    current_execution_context_ = &execution_context;
    
    if (pipeline.empty()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline is empty");
    }
    if (!pipeline.validate_types()) {
        throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                            "Pipeline type validation failed");
    }
    if (pipeline.has_cycles()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline contains cycles");
    }
    
    // Clear previous execution state
    task_outputs_.clear();
    task_completed_.clear();
    dependency_count_.clear();
    
    // Capture initial pipeline size to avoid submitting dynamically created tasks as entry tasks
    TaskIndex initial_pipeline_size = static_cast<TaskIndex>(pipeline.size());
    
    // Initialize dependency counters for initial tasks only
    for (TaskIndex i = 0; i < initial_pipeline_size; ++i) {
        task_completed_[i] = false;
        dependency_count_[i] = execution_context.get_task_dependencies(i).size();
    }
    
    // Submit all initial entry tasks (those with no dependencies) - let thread pool handle everything!
    for (TaskIndex i = 0; i < initial_pipeline_size; ++i) {
        if (execution_context.get_task_dependencies(i).empty()) {
            // Entry task - submit immediately with original input
            auto completion_callback = [this, &execution_context, i](std::any result) {
                // Store the result
                task_outputs_[i] = std::move(result);
                task_completed_[i] = true;
                
                // Process dependent tasks
                for (TaskIndex dependent : execution_context.get_task_dependents(i)) {
                    if (--dependency_count_[dependent] == 0) {
                        // All dependencies satisfied - submit the dependent task
                        std::any dependent_input;
                        
                        if (execution_context.get_task_dependencies(dependent).size() == 1) {
                            // Single dependency
                            dependent_input = task_outputs_[i];
                        } else {
                            // Multiple dependencies - combine inputs
                            std::vector<std::any> combined_inputs;
                            for (TaskIndex dep : execution_context.get_task_dependencies(dependent)) {
                                combined_inputs.push_back(task_outputs_[dep]);
                            }
                            dependent_input = combined_inputs;
                        }
                        
                        // Submit dependent task with recursive callback
                        submit_with_dependency_handling(execution_context, dependent, std::move(dependent_input));
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
    //
    // The race condition was: we'd wait for active_tasks_ to become 0, but right
    // after that check, a task that was finishing might emit new tasks to queues.
    
    // Use a stabilization approach: wait until we have multiple consecutive checks
    // showing both no active tasks AND no queued tasks
    int stable_iterations = 0;
    const int required_stable_iterations = 5;
    
    while (true) {
        // Wait for current active tasks to complete
        wait_for_completion();
        
        // Give a brief moment for any tasks that just completed to emit new tasks
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        
        // Check if any new tasks were emitted during execution
        bool has_queued_tasks = false;
        for (const auto& queue : queues_) {
            if (!queue->empty()) {
                has_queued_tasks = true;
                break;
            }
        }
        
        // Double-check active tasks haven't increased due to new task submissions
        bool has_active_tasks = (active_tasks_ > 0);
        
        if (!has_queued_tasks && !has_active_tasks) {
            stable_iterations++;
            
            if (stable_iterations >= required_stable_iterations) {
                break;
            }
            
            // Wait a bit longer between stability checks to ensure tasks have time to emit
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        } else {
            // Reset stability counter if we found work
            stable_iterations = 0;
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
    
    // Wait until scheduler signals workers are ready
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
        
        // First try to get a task from this thread's queue
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
        
        // If task found, execute it
        if (found_task) {
            try {
                TaskIndex task_id = task.task_id;
                std::any result;
                
                if (task.task_ptr) {
                    // Setup context for tasks that need it
                    if (task.task_ptr->needs_context()) {
                        TaskContext task_context(this, current_execution_context_, task_id);
                        task.task_ptr->setup_context(&task_context);
                    }
                    
                    // Execute the actual task
                    result = task.task_ptr->execute(task.input);
                    DFTRACER_UTILS_LOG_DEBUG("Worker %zu executed task %d", thread_id, task_id);
                } else {
                    // Fallback for tasks without pointer (shouldn't happen in normal execution)
                    DFTRACER_UTILS_LOG_WARN("Worker %zu: No task pointer for task %d, using input as result", thread_id, task_id);
                    result = task.input;
                }
                
                // Store result for dependent tasks
                {
                    std::lock_guard<std::mutex> lock(results_mutex_);
                    task_outputs_[task_id] = result;
                }
                
                // Call the completion callback with the result
                if (task.completion_callback) {
                    task.completion_callback(result);
                }
            } catch (const std::exception& e) {
                DFTRACER_UTILS_LOG_ERROR("Exception in worker thread %zu executing task %d: %s", thread_id, task.task_id, e.what());
                
                // Still call callback to avoid hanging the pipeline
                if (task.completion_callback) {
                    task.completion_callback(std::any{});
                }
            }
        } else {
            // No task found, wait for notification or check again after a delay
            std::unique_lock<std::mutex> lock(cv_mutex_);
            if (!should_terminate_ && active_tasks_ > 0) {
                // Wait with timeout to avoid deadlocks
                cv_.wait_for(lock, std::chrono::milliseconds(10));
            } else if (active_tasks_ == 0) {
                // No active tasks, wait for new tasks indefinitely
                cv_.wait(lock, [this]() {
                    return should_terminate_ || active_tasks_ > 0;
                });
            } else {
                // No tasks but should terminate
                break;
            }
        }
    }
    
    DFTRACER_UTILS_LOG_INFO("Worker thread %zu terminated", thread_id);
}

TaskQueue* ThreadScheduler::get_queue(std::size_t thread_id) {
    if (thread_id < queues_.size()) {
        return queues_[thread_id].get();
    }
    return nullptr;
}

bool ThreadScheduler::is_execution_complete() {
    return active_tasks_ == 0;
}

void ThreadScheduler::wait_for_completion() {
    std::unique_lock<std::mutex> lock(cv_mutex_);
    cv_.wait(lock, [this]() {
        return active_tasks_ == 0;
    });
}

void ThreadScheduler::signal_task_completion() {
    active_tasks_--;
    if (active_tasks_ == 0) {
        cv_.notify_all();
    }
}

void ThreadScheduler::process_all_queued_tasks() {
    // Process all dynamically emitted tasks until queues are empty AND no active tasks
    // This mimics SequentialExecutor's process_queued_tasks() but with work stealing
    
    bool has_work = true;
    while (has_work) {
        // Check if any queue has tasks OR if there are active tasks being processed
        bool has_queued_tasks = false;
        for (const auto& queue : queues_) {
            if (!queue->empty()) {
                has_queued_tasks = true;
                break;
            }
        }
        
        bool has_active_tasks = (active_tasks_ > 0);
        has_work = has_queued_tasks || has_active_tasks;
        
        if (has_work) {
            // Wait a bit for workers to process tasks and potentially emit new ones
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

void ThreadScheduler::process_all_remaining_tasks() {
    // Continuously process ALL remaining tasks until completely done
    // This handles nested dynamic task emission by running indefinitely until no more work exists
    
    // Use a longer stabilization period to handle async task emission
    int stable_iterations = 0;
    const int required_stable_iterations = 10; // Wait for 10 consecutive checks with no work
    
    while (true) {
        // Check if any queue has tasks OR if there are active tasks being processed
        bool has_queued_tasks = false;
        for (const auto& queue : queues_) {
            if (!queue->empty()) {
                has_queued_tasks = true;
                break;
            }
        }
        
        bool has_active_tasks = (active_tasks_ > 0);
        
        if (!has_queued_tasks && !has_active_tasks) {
            stable_iterations++;
            if (stable_iterations >= required_stable_iterations) {
                // Been stable with no work for sufficient time - we're done
                break;
            }
        } else {
            // Reset stability counter if we found work
            stable_iterations = 0;
        }
        
        // Wait a bit for workers to process tasks and potentially emit new ones
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void ThreadScheduler::process_dynamic_tasks_synchronously() {
    // Process all dynamic tasks similar to SequentialExecutor's process_queued_tasks
    // but using work-stealing thread pool
    
    while (true) {
        // Check if there are any tasks in any queue
        bool has_tasks = false;
        for (const auto& queue : queues_) {
            if (!queue->empty()) {
                has_tasks = true;
                break;
            }
        }
        
        if (!has_tasks) {
            // No more tasks to process
            break;
        }
        
        // Wait for all queued tasks to be processed
        wait_for_completion();
    }
}

void ThreadScheduler::submit_with_dependency_handling(ExecutorContext& execution_context, TaskIndex task_id, std::any input) {
    // Recursive helper to submit tasks with proper dependency handling
    auto completion_callback = [this, &execution_context, task_id](std::any result) {
        // Store the result
        task_outputs_[task_id] = std::move(result);
        task_completed_[task_id] = true;
        
        // Process dependent tasks recursively
        for (TaskIndex dependent : execution_context.get_task_dependents(task_id)) {
            if (--dependency_count_[dependent] == 0) {
                // All dependencies satisfied - prepare input
                std::any dependent_input;
                
                if (execution_context.get_task_dependencies(dependent).size() == 1) {
                    // Single dependency
                    dependent_input = task_outputs_[task_id];
                } else {
                    // Multiple dependencies
                    std::vector<std::any> combined_inputs;
                    for (TaskIndex dep : execution_context.get_task_dependencies(dependent)) {
                        combined_inputs.push_back(task_outputs_[dep]);
                    }
                    dependent_input = combined_inputs;
                }
                
                // Submit dependent task recursively
                submit_with_dependency_handling(execution_context, dependent, std::move(dependent_input));
            }
        }
        
        active_tasks_--;
        if (active_tasks_ == 0) {

            cv_.notify_all();
        }
    };
    
    Task* task_ptr = execution_context.get_task(task_id);
    submit(task_id, task_ptr, std::move(input), completion_callback);
}

}  // namespace dftracer::utils
