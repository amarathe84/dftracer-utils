#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_EXECUTOR_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_EXECUTOR_H

#include <dftracer/utils/pipeline/executors/executor.h>

#include <future>
#include <vector>

namespace dftracer::utils {

class ThreadExecutor : public Executor {
   private:
    size_t max_threads_;
    
   public:
    ThreadExecutor();
    explicit ThreadExecutor(size_t max_threads);
    ~ThreadExecutor() override = default;
    ThreadExecutor(const ThreadExecutor&) = delete;
    ThreadExecutor& operator=(const ThreadExecutor&) = delete;
    ThreadExecutor(ThreadExecutor&&) = default;
    ThreadExecutor& operator=(ThreadExecutor&&) = default;

    std::any execute(const Pipeline& pipeline, std::any input,
                     bool gather = true) override;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_THREAD_EXECUTOR_H
