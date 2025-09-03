#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>

#include <iostream>
#include <string>
#include <vector>

using namespace dftracer::utils;

int main() {
    DFTRACER_UTILS_LOGGER_INIT();
    // Initialize logging
    DFTRACER_UTILS_LOG_INFO("Starting function task example");

    // Create separate pipelines for different data types
    Pipeline int_pipeline;
    Pipeline string_pipeline;
    Pipeline vector_pipeline;

    // Example 1: Simple function task without task emission
    auto simple_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Processing simple task with input: %d", input);
        return input * 2;
    };

    TaskIndex task1 = int_pipeline.add_task<int, int>(simple_task);
    DFTRACER_UTILS_LOG_INFO("Added simple task with ID: %d", task1);

    // Example 2: Function task that emits another task (SAFE - using emit()
    // with current())
    auto emitting_task = [](int input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Processing emitting task with input: %d",
                                input);

        // SAFE: Emit a task that depends on current task atomically
        auto dependent_task = [](int x, TaskContext& ctx2) -> int {
            DFTRACER_UTILS_LOG_INFO("Processing dependent task with input: %d",
                                    x);
            return x + 100;
        };

        TaskIndex dependent_id = ctx.emit<int, int>(
            dependent_task, Input{input * 2}, DependsOn{ctx.current()});
        DFTRACER_UTILS_LOG_INFO("Emitted dependent task with ID: %d",
                                dependent_id);

        return input + 10;
    };

    TaskIndex task2 = int_pipeline.add_task<int, int>(emitting_task);
    DFTRACER_UTILS_LOG_INFO("Added emitting task with ID: %d", task2);

    // Add dependency between tasks
    int_pipeline.add_dependency(task1, task2);

    // Example 3: String processing task
    auto string_task = [](std::string input, TaskContext& ctx) -> std::string {
        DFTRACER_UTILS_LOG_INFO("Processing string: %s", input.c_str());
        return "Processed: " + input;
    };

    TaskIndex task3 =
        string_pipeline.add_task<std::string, std::string>(string_task);
    DFTRACER_UTILS_LOG_INFO("Added string task with ID: %d", task3);

    // Example 4: Vector processing with dynamic task emission
    auto vector_processor = [](std::vector<int> input,
                               TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Processing vector of size: %zu", input.size());

        int sum = 0;
        for (size_t i = 0; i < input.size(); ++i) {
            // SAFE: Emit child tasks that depend on current task
            auto element_processor = [i](int element,
                                         TaskContext& ctx2) -> int {
                DFTRACER_UTILS_LOG_INFO("Processing element %zu: %d", i,
                                        element);
                return element * element;
            };

            TaskIndex element_task = ctx.emit<int, int>(
                element_processor, Input{input[i]}, DependsOn{ctx.current()});
            DFTRACER_UTILS_LOG_INFO("Emitted element task %zu with ID: %d", i,
                                    element_task);
            sum += input[i];
        }

        return sum;
    };

    TaskIndex task4 =
        vector_pipeline.add_task<std::vector<int>, int>(vector_processor);
    DFTRACER_UTILS_LOG_INFO("Added vector processor with ID: %d", task4);

    // Execute the pipelines with different inputs
    try {
        SequentialExecutor executor;

        DFTRACER_UTILS_LOG_INFO(
            "=== Executing int pipeline with integer input ===");
        std::any result1 = executor.execute(int_pipeline, 5);
        int final_result1 = std::any_cast<int>(result1);
        DFTRACER_UTILS_LOG_INFO("Final result: %d", final_result1);

        DFTRACER_UTILS_LOG_INFO(
            "=== Executing string pipeline with string input ===");
        std::string str_input = "Hello World";
        std::any result2 = executor.execute(string_pipeline, str_input);
        std::string final_result2 = std::any_cast<std::string>(result2);
        DFTRACER_UTILS_LOG_INFO("Final result: %s", final_result2.c_str());

        DFTRACER_UTILS_LOG_INFO(
            "=== Executing vector pipeline with vector input ===");
        std::vector<int> vec_input = {1, 2, 3, 4, 5};
        std::any result3 = executor.execute(vector_pipeline, vec_input);
        int final_result3 = std::any_cast<int>(result3);
        DFTRACER_UTILS_LOG_INFO("Final result: %d", final_result3);

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline execution failed: %s", e.what());
        return 1;
    }

    DFTRACER_UTILS_LOG_INFO("Function task example completed successfully");
    return 0;
}
