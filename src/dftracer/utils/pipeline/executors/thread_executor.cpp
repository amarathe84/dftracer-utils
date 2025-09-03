#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/thread/scheduler.h>
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
    DFTRACER_UTILS_LOG_INFO("ThreadExecutor initialized with max_threads = %zu",
                            max_threads_);
}

ThreadExecutor::ThreadExecutor(size_t max_threads)
    : Executor(ExecutorType::THREAD), max_threads_(max_threads) {
    if (max_threads_ == 0) {
        max_threads_ = 2;
    }
    DFTRACER_UTILS_LOG_INFO("ThreadExecutor initialized with max_threads = %zu",
                            max_threads_);
}

ThreadExecutor::~ThreadExecutor() = default;

std::any ThreadExecutor::execute(const Pipeline& pipeline, std::any input) {
    // Validate pipeline
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
    
    // Get the global scheduler instance
    auto scheduler = GlobalScheduler::get_instance();
    
    // CRITICAL: Set pipeline reference so scheduler can access tasks
    scheduler->set_pipeline(&pipeline);
    
    // Initialize the scheduler with the number of threads
    scheduler->initialize(max_threads_);
    
    // Execute the pipeline using the scheduler
    std::any result = scheduler->execute_pipeline(pipeline, input);
    
    // Shutdown the scheduler when done
    scheduler->shutdown();
    
    return result;
}

}  // namespace dftracer::utils
