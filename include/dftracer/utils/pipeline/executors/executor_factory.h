#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_FACTORY_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_FACTORY_H

#include <dftracer/utils/pipeline/executors/executor_type.h>
#include <dftracer/utils/pipeline/executors/executor.h>

#include <memory>

namespace dftracer::utils {

/**
 * Factory class for creating executors without exposing implementation details.
 * 
 * This factory provides a clean interface to create different types of executors
 * while hiding the concrete implementation classes from users.
 * 
 * Example usage:
 *   auto sequential_executor = ExecutorFactory::create_sequential();
 *   auto thread_executor = ExecutorFactory::create_thread();  // uses default thread count
 *   auto thread_executor_4 = ExecutorFactory::create_thread(4);  // uses 4 threads
 */
class ExecutorFactory {
public:
    /**
     * Create a thread executor with the specified number of threads.
     * 
     * @param num_threads Number of worker threads (defaults to hardware_concurrency)
     * @return std::unique_ptr to the created thread executor
     */
    static std::unique_ptr<Executor> create_thread(std::size_t num_threads = 0);
    
    /**
     * Create a sequential executor (single-threaded).
     * 
     * @return std::unique_ptr to the created sequential executor
     */
    static std::unique_ptr<Executor> create_sequential();

private:
    /**
     * Get the default number of threads for thread executors.
     * Typically returns std::thread::hardware_concurrency() or a sensible default.
     * 
     * @return Default number of threads
     */
    static std::size_t get_default_thread_count();

private:
    // Private constructor - this is a factory class, not meant to be instantiated
    ExecutorFactory() = default;
};

} // namespace dftracer::utils

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_EXECUTOR_FACTORY_H