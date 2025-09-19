#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/pipeline/executors/scheduler/thread_scheduler.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>

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

ThreadExecutor::ThreadExecutor()
    : Executor(ExecutorType::THREAD),
      max_threads_(std::thread::hardware_concurrency()) {
    if (max_threads_ == 0) max_threads_ = 2;
    DFTRACER_UTILS_LOG_DEBUG(
        "ThreadExecutor initialized with max_threads = %zu", max_threads_);
}

ThreadExecutor::ThreadExecutor(size_t max_threads)
    : Executor(ExecutorType::THREAD), max_threads_(max_threads) {
    if (max_threads_ == 0) {
        max_threads_ = 2;
    }
    DFTRACER_UTILS_LOG_DEBUG(
        "ThreadExecutor initialized with max_threads = %zu", max_threads_);
}

ThreadExecutor::~ThreadExecutor() = default;

PipelineOutput ThreadExecutor::execute(const Pipeline& pipeline, std::any input) {
    ThreadScheduler scheduler;
    scheduler.initialize(max_threads_);
    PipelineOutput result = scheduler.execute(pipeline, input);
    scheduler.shutdown();
    return result;
}

}  // namespace dftracer::utils
