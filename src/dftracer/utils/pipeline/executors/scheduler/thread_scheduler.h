#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_THREAD_SCHEDULER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_THREAD_SCHEDULER_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/executors/scheduler/scheduler.h>
#include <dftracer/utils/pipeline/executors/scheduler/thread_task_queue.h>
#include <dftracer/utils/pipeline/tasks/task.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dftracer::utils {

class Pipeline;
class ExecutorContext;

class ThreadScheduler : public Scheduler {
   public:
    ThreadScheduler();
    ~ThreadScheduler();
    ThreadScheduler(const ThreadScheduler&) = delete;
    ThreadScheduler& operator=(const ThreadScheduler&) = delete;
    ThreadScheduler(ThreadScheduler&&) = default;
    ThreadScheduler& operator=(ThreadScheduler&&) = default;

    void initialize(std::size_t num_threads);
    void shutdown();
    void submit(TaskIndex task_id, std::any input,
                std::function<void(std::any)> completion_callback) override;
    void submit(TaskIndex task_id, Task* task_ptr, std::any input,
                std::function<void(std::any)> completion_callback) override;
    PipelineOutput execute(const Pipeline& pipeline, std::any input) override;
    void signal_task_completion() override;

   private:
    std::vector<std::unique_ptr<TaskQueue>> queues_;
    std::vector<std::thread> workers_;
    std::atomic<bool> should_terminate_{false};
    std::atomic<bool> workers_ready_{false};
    std::atomic<std::size_t> active_tasks_{0};
    std::condition_variable cv_;
    std::mutex cv_mutex_;
    std::unordered_map<TaskIndex, std::any> task_outputs_;
    std::unordered_map<TaskIndex, std::atomic<bool>> task_completed_;
    std::unordered_map<TaskIndex, std::atomic<int>> dependency_count_;
    ExecutorContext* current_execution_context_;
    std::mutex results_mutex_;

    void worker_thread(std::size_t thread_id);
    void wait_for_completion();
    void submit_with_dependency_handling(ExecutorContext& execution_context,
                                         TaskIndex task_id, std::any input);
    bool queues_empty() const;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_THREAD_SCHEDULER_H
