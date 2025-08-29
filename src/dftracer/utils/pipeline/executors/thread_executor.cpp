#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/executors/thread_safe_queue.h>

#include <any>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dftracer::utils {

class ThreadPool {
   public:
    ThreadPool(size_t threads);
    ~ThreadPool();

    void enqueue(std::function<void()> task);
    bool pop_task_from_other_thread(std::function<void()>& task);
    void wait_for_tasks();

   private:
    std::vector<std::thread> workers_;
    std::vector<std::unique_ptr<ThreadSafeQueue<std::function<void()>>>>
        thread_queues_;
    std::atomic<bool> stop_;
    std::atomic<size_t> active_tasks_;
    std::condition_variable condition_;
    std::mutex mutex_;
};

ThreadPool::ThreadPool(size_t threads) : stop_(false), active_tasks_(0) {
    for (size_t i = 0; i < threads; ++i) {
        thread_queues_.emplace_back(
            std::make_unique<ThreadSafeQueue<std::function<void()>>>());
        workers_.emplace_back([this, i] {
            while (true) {
                std::function<void()> task;
                if (thread_queues_[i]->try_pop(task) ||
                    pop_task_from_other_thread(task)) {
                    task();
                    active_tasks_--;
                    condition_.notify_all();
                } else {
                    std::unique_lock<std::mutex> lock(this->mutex_);
                    if (this->stop_ && this->active_tasks_ == 0) {
                        return;
                    }
                    if (!this->stop_) {
                        this->condition_.wait(
                            lock, [this] { return this->stop_ || this->active_tasks_ > 0; });
                    }
                }
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_ = true;
    }
    condition_.notify_all();
    for (std::thread& worker : workers_) {
        worker.join();
    }
}

void ThreadPool::enqueue(std::function<void()> task) {
    active_tasks_++;
    static std::atomic<size_t> index = 0;
    thread_queues_[index++ % thread_queues_.size()]->push(std::move(task));
    condition_.notify_one();
}

bool ThreadPool::pop_task_from_other_thread(std::function<void()>& task) {
    for (size_t i = 0; i < thread_queues_.size(); ++i) {
        if (thread_queues_[i]->try_pop(task)) {
            return true;
        }
    }
    return false;
}

void ThreadPool::wait_for_tasks() {
    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [this] { return this->active_tasks_ == 0; });
}

ThreadExecutor::ThreadExecutor()
    : Executor(ExecutorType::THREAD),
      max_threads_(std::thread::hardware_concurrency()),
      pool_(std::make_unique<ThreadPool>(
          max_threads_ > 0 ? max_threads_ : 2)) {
    if (max_threads_ == 0) max_threads_ = 2;
    DFTRACER_UTILS_LOG_INFO("ThreadExecutor initialized with max_threads = %zu",
                            max_threads_);
}

ThreadExecutor::ThreadExecutor(size_t max_threads)
    : Executor(ExecutorType::THREAD),
      max_threads_(max_threads > 0 ? max_threads
                                   : std::thread::hardware_concurrency()),
      pool_(std::make_unique<ThreadPool>(
          max_threads_ > 0 ? max_threads_ : 2)) {
    if (max_threads_ == 0) {
        max_threads_ = 2;
    }
    DFTRACER_UTILS_LOG_INFO("ThreadExecutor initialized with max_threads = %zu",
                            max_threads_);
}

ThreadExecutor::~ThreadExecutor() = default;

std::any ThreadExecutor::execute(const Pipeline& pipeline, std::any input) {
    // gather parameter is ignored in thread executor (noop)
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

    auto execution_order = pipeline.topological_sort();
    std::unordered_map<TaskIndex, std::any> task_outputs;
    std::unordered_map<TaskIndex, std::atomic<int>> dependency_counters;
    std::unordered_map<TaskIndex, std::vector<TaskIndex>> dependents;
    std::mutex task_outputs_mutex;

    for (TaskIndex task_id : execution_order) {
        auto& dependencies = pipeline.get_task_dependencies(task_id);
        dependency_counters[task_id] = dependencies.size();
        for (TaskIndex dep_id : dependencies) {
            dependents[dep_id].push_back(task_id);
        }
    }

    auto schedule_task =
        [&](TaskIndex task_id, const auto& self) -> void {
        pool_->enqueue([&, task_id, self]() {
            std::any task_input;
            auto& dependencies = pipeline.get_task_dependencies(task_id);

            {
                std::lock_guard<std::mutex> lock(task_outputs_mutex);
                if (dependencies.empty()) {
                    task_input = input;
                } else if (dependencies.size() == 1) {
                    task_input = task_outputs.at(dependencies[0]);
                } else {
                    std::vector<std::any> combined_inputs;
                    for (TaskIndex dep : dependencies) {
                        combined_inputs.push_back(task_outputs.at(dep));
                    }
                    task_input = combined_inputs;
                }
            }

            auto result = pipeline.get_task(task_id)->execute(task_input);

            {
                std::lock_guard<std::mutex> lock(task_outputs_mutex);
                task_outputs[task_id] = result;
            }

            for (TaskIndex dependent_id : dependents[task_id]) {
                if (--dependency_counters[dependent_id] == 0) {
                    self(dependent_id, self);
                }
            }
        });
    };

    for (TaskIndex task_id : execution_order) {
        if (dependency_counters[task_id] == 0) {
            schedule_task(task_id, schedule_task);
        }
    }

    pool_->wait_for_tasks();

    if (!execution_order.empty()) {
        std::lock_guard<std::mutex> lock(task_outputs_mutex);
        return task_outputs[execution_order.back()];
    }

    return input;
}

}  // namespace dftracer::utils
