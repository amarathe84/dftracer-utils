#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <dftracer/utils/common/logging.h>

#include <iostream>

using namespace dftracer::utils;

int main() {
    std::cout << "=== Testing Basic Execution Fixes ===\n";
    
    Pipeline pipeline;
    
    // Simple test: function that doubles input
    auto double_task = [](int input, TaskContext& ctx) -> int {
        std::cout << "Executing double_task with input: " << input << std::endl;
        return input * 2;
    };
    
    // Add task to pipeline
    TaskIndex task_id = pipeline.add_task<int, int>(double_task);
    std::cout << "Added task with ID: " << task_id << std::endl;
    
    // Test with Sequential Executor
    std::cout << "\n=== Sequential Executor Test ===\n";
    try {
        SequentialExecutor seq_executor;
        std::any result = seq_executor.execute(pipeline, 21);
        int final_result = std::any_cast<int>(result);
        std::cout << "Sequential result: " << final_result << " (expected: 42)\n";
        
        if (final_result == 42) {
            std::cout << "✅ Sequential executor: PASSED\n";
        } else {
            std::cout << "❌ Sequential executor: FAILED\n";
        }
    } catch (const std::exception& e) {
        std::cout << "❌ Sequential executor failed: " << e.what() << std::endl;
    }
    
    // Test with Thread Executor
    std::cout << "\n=== Thread Executor Test ===\n";
    try {
        ThreadExecutor thread_executor(2);
        std::any result = thread_executor.execute(pipeline, 21);
        int final_result = std::any_cast<int>(result);
        std::cout << "Thread result: " << final_result << " (expected: 42)\n";
        
        if (final_result == 42) {
            std::cout << "✅ Thread executor: PASSED\n";
        } else {
            std::cout << "❌ Thread executor: FAILED\n";
        }
    } catch (const std::exception& e) {
        std::cout << "❌ Thread executor failed: " << e.what() << std::endl;
    }
    
    // Test task chaining (dependency)
    std::cout << "\n=== Task Chain Test ===\n";
    Pipeline chain_pipeline;
    
    auto task1 = [](int input, TaskContext& ctx) -> int {
        std::cout << "Task1 processing: " << input << std::endl;
        return input + 10;  // 5 -> 15
    };
    
    auto task2 = [](int input, TaskContext& ctx) -> int {
        std::cout << "Task2 processing: " << input << std::endl;
        return input * 2;   // 15 -> 30
    };
    
    TaskIndex t1 = chain_pipeline.add_task<int, int>(task1);
    TaskIndex t2 = chain_pipeline.add_task<int, int>(task2);
    chain_pipeline.add_dependency(t1, t2);  // t2 depends on t1
    
    try {
        SequentialExecutor seq_executor;
        // ThreadExecutor seq_executor;
        std::any result = seq_executor.execute(chain_pipeline, 5);
        int final_result = std::any_cast<int>(result);
        std::cout << "Chain result: " << final_result << " (expected: 30)\n";
        
        if (final_result == 30) {
            std::cout << "✅ Task chaining: PASSED\n";
        } else {
            std::cout << "❌ Task chaining: FAILED\n";
        }
    } catch (const std::exception& e) {
        std::cout << "❌ Task chaining failed: " << e.what() << std::endl;
    }
    
    std::cout << "\n=== Test Complete ===\n";
    return 0;
}
