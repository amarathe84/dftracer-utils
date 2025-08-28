#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_SAFE_QUEUE_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_SAFE_QUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

namespace dftracer::utils {

template <typename T>
class ThreadSafeQueue {
   public:
    ThreadSafeQueue() = default;
    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

    void push(T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(value));
        cond_.notify_one();
    }

    bool try_pop(T& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void wait_and_pop(T& value) {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return !queue_.empty(); });
        value = std::move(queue_.front());
        queue_.pop();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

   private:
    mutable std::mutex mutex_;
    std::queue<T> queue_;
    std::condition_variable cond_;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_SAFE_QUEUE_H
