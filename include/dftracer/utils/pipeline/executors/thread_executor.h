#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_EXECUTOR_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_EXECUTOR_H

#include <dftracer/utils/pipeline/executors/executor.h>

#include <future>
#include <memory>
#include <vector>

namespace dftracer::utils {

class ThreadPool;
class ThreadWorker;

// struct ThreadWorker {
//   std::queue
// };

class ThreadExecutor : public Executor {
   private:
    size_t max_threads_;
    std::unique_ptr<ThreadPool> pool_;

   public:
    ThreadExecutor();
    explicit ThreadExecutor(size_t max_threads);
    ~ThreadExecutor() override;
    ThreadExecutor(const ThreadExecutor&) = delete;
    ThreadExecutor& operator=(const ThreadExecutor&) = delete;
    ThreadExecutor(ThreadExecutor&&) = default;
    ThreadExecutor& operator=(ThreadExecutor&&) = default;

    std::any execute(const Pipeline& pipeline, std::any input) override;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_EXECUTOR_H
