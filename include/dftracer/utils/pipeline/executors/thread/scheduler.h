#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_SCHEDULER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_SCHEDULER_H

#include <dftracer/utils/pipeline/tasks/task.h>
#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/executors/scheduler/scheduler_interface.h>
#include <dftracer/utils/pipeline/executors/thread/task_queue.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace dftracer::utils {

// Forward declaration
class Pipeline;

// GlobalScheduler manages task queues and enables work stealing
class GlobalScheduler : public SchedulerInterface {
private:
    // Singleton instance
    static std::unique_ptr<GlobalScheduler> instance_;
    static std::mutex instance_mutex_;

    // Task queues - one per worker thread
    std::vector<std::unique_ptr<TaskQueue>> queues_;
    
    // Worker thread management
    std::vector<std::thread> workers_;
    std::atomic<bool> should_terminate_{false};
    std::atomic<bool> workers_ready_{false};  // NEW: Signal when workers can start
    std::atomic<std::size_t> active_tasks_{0};
    std::condition_variable cv_;
    std::mutex cv_mutex_;
    
    // Pipeline execution state
    std::unordered_map<TaskIndex, std::any> task_outputs_;
    std::unordered_map<TaskIndex, std::atomic<bool>> task_completed_;
    std::unordered_map<TaskIndex, std::atomic<int>> dependency_count_;
    
    // Pipeline reference for task execution
    const Pipeline* current_pipeline_;
    std::mutex results_mutex_;  // Protects task_outputs_
    
public:
    GlobalScheduler();
    ~GlobalScheduler();

    // Delete copy/move constructors and assignment operators
    GlobalScheduler(const GlobalScheduler&) = delete;
    GlobalScheduler& operator=(const GlobalScheduler&) = delete;
    GlobalScheduler(GlobalScheduler&&) = delete;
    GlobalScheduler& operator=(GlobalScheduler&&) = delete;

    // Get singleton instance
    static GlobalScheduler* get_instance();
    
    // Set pipeline for current execution
    void set_pipeline(const Pipeline* pipeline) override;
    
    // Initialize with number of worker threads
    void initialize(std::size_t num_threads);

    // Shut down all worker threads
    void shutdown();
    
    // Submit a task to the scheduler
    void submit(TaskIndex task_id, std::any input,
               std::function<void(std::any)> completion_callback) override;
                    
    // Submit a task with direct Task pointer (for dynamic tasks)
    void submit(TaskIndex task_id, Task* task_ptr, std::any input,
               std::function<void(std::any)> completion_callback) override;
    
    // Execute a pipeline using the worker threads
    std::any execute_pipeline(const Pipeline& pipeline, std::any input) override;
    
    // Worker thread function
    void worker_thread(std::size_t thread_id);

    // Get task queue for a specific thread
    TaskQueue* get_queue(std::size_t thread_id);

    // Check if execution is complete
    bool is_execution_complete();
    
    // Wait for all tasks to complete
    void wait_for_completion();
    
    // Signal that an independent task has completed
    void signal_task_completion();
    
    // Process all queued dynamically emitted tasks (with work stealing)
    void process_all_queued_tasks();
    
    // Process all remaining tasks until completely done (handles nested dynamic emission)
    void process_all_remaining_tasks();
    
    // Process dynamic tasks synchronously like SequentialExecutor
    void process_dynamic_tasks_synchronously();
    
    // Helper method to submit tasks with recursive dependency handling
    void submit_with_dependency_handling(const Pipeline& pipeline, TaskIndex task_id, std::any input);
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_SCHEDULER_H
