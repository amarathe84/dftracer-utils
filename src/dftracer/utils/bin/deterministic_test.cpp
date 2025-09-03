#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <dftracer/utils/common/logging.h>

#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>

using namespace dftracer::utils;

// Global counters to track work
std::atomic<int> tasks_executed{0};

int main() {
    DFTRACER_UTILS_LOGGER_INIT();
    DFTRACER_UTILS_LOG_INFO("=== Deterministic Result Test ===");
    
    std::cout << "\n=== Testing Deterministic Results ===\n";
    
    Pipeline pipeline;
    
    // Create a generator that emits deterministic tasks
    auto generator = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Generator processing input: %d", input);
        
        // Generate 5 deterministic subtasks
        for (int i = 0; i < 5; ++i) {
            auto work_task = [i, input](int work_amount, TaskContext& ctx2) -> int {
                std::thread::id thread_id = std::this_thread::get_id();
                
                // Deterministic work - just math operations with no timing dependencies
                int result = input;
                for (int j = 0; j < work_amount * 100; ++j) {
                    result = (result * 3 + 7) % 1000;  // Simple deterministic computation
                }
                
                tasks_executed++;
                
                std::hash<std::thread::id> hasher;
                size_t thread_hash = hasher(thread_id) % 1000;
                
                DFTRACER_UTILS_LOG_INFO("Work task %d (amount=%d) executed by thread %zu, result=%d", 
                                       i, work_amount, thread_hash, result);
                
                return result + i;  // Deterministic result
            };
            
            int work_amount = i + 1;  // Work amounts: 1, 2, 3, 4, 5
            TaskIndex work_id = ctx.emit<int, int>(work_task, Input{work_amount});
            DFTRACER_UTILS_LOG_INFO("Emitted work task %d with amount %d (ID: %d)", i, work_amount, work_id);
        }
        
        return input * 2;  // Deterministic result
    };
    
    // Add generator task to pipeline
    TaskIndex gen_id = pipeline.add_task<int, int>(generator);
    
    std::cout << "\n--- Testing Sequential Execution ---\n";
    tasks_executed = 0;
    int sequential_result = 0;
    
    try {
        SequentialExecutor seq_executor;
        std::any result = seq_executor.execute(pipeline, 42);
        sequential_result = std::any_cast<int>(result);
        
        std::cout << "Sequential result: " << sequential_result << "\n";
        std::cout << "Sequential tasks executed: " << tasks_executed << "\n";
        
    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Sequential execution failed: %s", e.what());
        return 1;
    }
    
    std::cout << "\n--- Testing Threaded Execution (2 threads) ---\n";
    tasks_executed = 0;
    int threaded_result = 0;
    
    try {
        ThreadExecutor thread_executor(2);
        std::any result = thread_executor.execute(pipeline, 42);
        threaded_result = std::any_cast<int>(result);
        
        std::cout << "Threaded result: " << threaded_result << "\n";
        std::cout << "Threaded tasks executed: " << tasks_executed << "\n";
        
    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Threaded execution failed: %s", e.what());
        return 1;
    }
    
    std::cout << "\n--- Testing Threaded Execution (4 threads) ---\n";
    tasks_executed = 0;
    int threaded4_result = 0;
    
    try {
        ThreadExecutor thread_executor(4);
        std::any result = thread_executor.execute(pipeline, 42);
        threaded4_result = std::any_cast<int>(result);
        
        std::cout << "Threaded (4) result: " << threaded4_result << "\n";
        std::cout << "Threaded (4) tasks executed: " << tasks_executed << "\n";
        
    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Threaded (4) execution failed: %s", e.what());
        return 1;
    }
    
    // Compare results
    std::cout << "\n=== RESULT COMPARISON ===\n";
    std::cout << "Sequential result:    " << sequential_result << "\n";
    std::cout << "Threaded (2) result:  " << threaded_result << "\n";
    std::cout << "Threaded (4) result:  " << threaded4_result << "\n";
    
    if (sequential_result == threaded_result && threaded_result == threaded4_result) {
        std::cout << "✅ All results match! Pipeline execution is deterministic.\n";
        return 0;
    } else {
        std::cout << "❌ Results don't match! There may be non-deterministic behavior.\n";
        return 1;
    }
}
