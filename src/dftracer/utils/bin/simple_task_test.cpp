#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <dftracer/utils/common/logging.h>

#include <iostream>

using namespace dftracer::utils;

int main() {
    DFTRACER_UTILS_LOG_INFO("=== Simple Task Test (No Dynamic Emission) ===");
    
    Pipeline pipeline;
    
    // Simple task that doesn't emit other tasks
    auto simple_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Processing input: %d", input);
        return input * 2;
    };
    
    // Chain of simple tasks
    auto double_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Doubling: %d", input);
        return input * 2;
    };
    
    auto add_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Adding 10 to: %d", input);
        return input + 10;
    };
    
    TaskIndex task1 = pipeline.add_task<int, int>(simple_task);
    TaskIndex task2 = pipeline.add_task<int, int>(double_task);
    TaskIndex task3 = pipeline.add_task<int, int>(add_task);
    
    pipeline.add_dependency(task1, task2);
    pipeline.add_dependency(task2, task3);
    
    try {
        SequentialExecutor executor;
        
        DFTRACER_UTILS_LOG_INFO("=== Executing simple pipeline with input 5 ===");
        std::any result = executor.execute(pipeline, 5);
        int final_result = std::any_cast<int>(result);
        DFTRACER_UTILS_LOG_INFO("Final result: %d (expected: 30)", final_result);
        
        if (final_result == 30) {  // (5 * 2) * 2 + 10 = 30
            DFTRACER_UTILS_LOG_INFO("✅ Test PASSED");
            return 0;
        } else {
            DFTRACER_UTILS_LOG_ERROR("❌ Test FAILED - expected 30, got %d", final_result);
            return 1;
        }
        
    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline execution failed: %s", e.what());
        return 1;
    }
}