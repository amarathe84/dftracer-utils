#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace dftracer::utils;

int main() {
    DFTRACER_UTILS_LOGGER_INIT();
    DFTRACER_UTILS_LOG_INFO("=== Type Transformation Examples ===");

    // Example 1: int → string conversion pipeline
    std::cout << "\n=== Pipeline 1: int → string → vector<char> ===\n";
    Pipeline int_to_string_pipeline;

    // Task 1: int → string
    auto int_to_string = [](int input, TaskContext& ctx) -> std::string {
        DFTRACER_UTILS_LOG_INFO("Converting int %d to string", input);
        return "Number: " + std::to_string(input);
    };

    // Task 2: string → vector<char>
    auto string_to_vector = [](std::string input,
                               TaskContext& ctx) -> std::vector<char> {
        DFTRACER_UTILS_LOG_INFO("Converting string '%s' to vector<char>",
                                input.c_str());
        std::vector<char> result(input.begin(), input.end());
        return result;
    };

    TaskIndex t1 =
        int_to_string_pipeline.add_task<int, std::string>(int_to_string);
    TaskIndex t2 =
        int_to_string_pipeline.add_task<std::string, std::vector<char>>(
            string_to_vector);
    int_to_string_pipeline.add_dependency(t1, t2);

    // Execute pipeline 1
    try {
        SequentialExecutor executor;
        std::any result = executor.execute(int_to_string_pipeline, 42);
        std::vector<char> final_result =
            std::any_cast<std::vector<char>>(result);

        std::cout << "Result vector<char>: [";
        for (size_t i = 0; i < final_result.size(); ++i) {
            std::cout << "'" << final_result[i] << "'";
            if (i < final_result.size() - 1) std::cout << ", ";
        }
        std::cout << "] (size: " << final_result.size() << ")\n";

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline 1 failed: %s", e.what());
    }

    // Example 2: vector<int> → double → string pipeline
    std::cout << "\n=== Pipeline 2: vector<int> → double → string ===\n";
    Pipeline vector_to_string_pipeline;

    // Task 1: vector<int> → double (calculate average)
    auto vector_to_average = [](std::vector<int> input,
                                TaskContext& ctx) -> double {
        DFTRACER_UTILS_LOG_INFO(
            "Calculating average of vector with %zu elements", input.size());
        if (input.empty()) return 0.0;

        int sum = 0;
        for (int val : input) {
            sum += val;
        }
        double avg = static_cast<double>(sum) / input.size();
        DFTRACER_UTILS_LOG_INFO("Average calculated: %f", avg);
        return avg;
    };

    // Task 2: double → string (format with precision)
    auto double_to_formatted_string = [](double input,
                                         TaskContext& ctx) -> std::string {
        DFTRACER_UTILS_LOG_INFO("Formatting double %f to string", input);
        std::ostringstream oss;
        oss.precision(2);
        oss << std::fixed << "Average: " << input;
        return oss.str();
    };

    TaskIndex t3 = vector_to_string_pipeline.add_task<std::vector<int>, double>(
        vector_to_average);
    TaskIndex t4 = vector_to_string_pipeline.add_task<double, std::string>(
        double_to_formatted_string);
    vector_to_string_pipeline.add_dependency(t3, t4);

    // Execute pipeline 2
    try {
        SequentialExecutor executor;
        std::vector<int> input_data = {10, 20, 30, 40, 50};

        std::cout << "Input vector: [";
        for (size_t i = 0; i < input_data.size(); ++i) {
            std::cout << input_data[i];
            if (i < input_data.size() - 1) std::cout << ", ";
        }
        std::cout << "]\n";

        std::any result =
            executor.execute(vector_to_string_pipeline, input_data);
        std::string final_result = std::any_cast<std::string>(result);
        std::cout << "Result: " << final_result << "\n";

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline 2 failed: %s", e.what());
    }

    // Example 3: Dynamic task emission with type changes
    std::cout << "\n=== Pipeline 3: Dynamic Emission with Type Changes ===\n";
    Pipeline dynamic_transform_pipeline;

    // Complex task that emits multiple tasks with different types
    auto multi_transform_task = [](std::string input, TaskContext& ctx) -> int {
        DFTRACER_UTILS_LOG_INFO("Processing string '%s' for multi-transform",
                                input.c_str());

        // Emit task 1: string → int (length)
        auto get_length = [](std::string s, TaskContext& ctx2) -> int {
            DFTRACER_UTILS_LOG_INFO("Getting length of string '%s'", s.c_str());
            return static_cast<int>(s.length());
        };
        TaskIndex length_task =
            ctx.emit<std::string, int>(get_length, Input{input});

        // Emit task 2: string → double (hash as double)
        auto string_to_hash = [](std::string s, TaskContext& ctx2) -> double {
            DFTRACER_UTILS_LOG_INFO("Computing hash for string '%s'",
                                    s.c_str());
            std::hash<std::string> hasher;
            size_t hash_val = hasher(s);
            return static_cast<double>(hash_val % 1000) /
                   100.0;  // Normalize to 0-10 range
        };
        TaskIndex hash_task =
            ctx.emit<std::string, double>(string_to_hash, Input{input});

        DFTRACER_UTILS_LOG_INFO("Emitted length_task=%d, hash_task=%d",
                                length_task, hash_task);

        // Return the number of characters processed
        return static_cast<int>(input.size());
    };

    TaskIndex t5 = dynamic_transform_pipeline.add_task<std::string, int>(
        multi_transform_task);

    // Execute pipeline 3
    try {
        SequentialExecutor executor;
        std::string test_string = "Hello, Pipeline!";
        std::cout << "Input string: \"" << test_string << "\"\n";

        std::any result =
            executor.execute(dynamic_transform_pipeline, test_string);
        int final_result = std::any_cast<int>(result);
        std::cout << "Main task result (chars processed): " << final_result
                  << "\n";

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Pipeline 3 failed: %s", e.what());
    }

    DFTRACER_UTILS_LOG_INFO("=== Type transformation examples completed ===");

    // Summary
    std::cout << "\n=== TYPE TRANSFORMATION SUMMARY ===\n";
    std::cout << "1. int → string → vector<char>: Number to character array\n";
    std::cout
        << "2. vector<int> → double → string: Statistics with formatting\n";
    std::cout << "3. Dynamic emission: string → (int + double): Multi-output "
                 "processing\n";
    std::cout
        << "\n✅ All type transformations demonstrate pipeline flexibility!\n";

    return 0;
}
