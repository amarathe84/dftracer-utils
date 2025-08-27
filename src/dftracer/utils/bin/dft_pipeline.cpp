#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/tasks/factory.h>

#include <any>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <memory>
#include <vector>

using namespace dftracer::utils;

static void demonstrate_sequential_vs_thread_comparison() {
    std::cout << "=== Sequential vs Thread Comparison (Parallel DAG) ==="
              << std::endl;

    // Create multiple independent VERY CPU-intensive tasks that can run in
    // parallel
    auto create_parallel_tasks = []() {
        // Task 1: Heavy prime number computation (seconds-level work) - Using
        // factory API
        auto task1 = Tasks::map<int, double>([](const int& x) {
            int count = 0;
            int limit = x * 100000;  // Much larger computation
            for (int i = 2; i <= limit; ++i) {
                bool is_prime = true;
                for (int j = 2; j * j <= i && is_prime; ++j) {
                    if (i % j == 0) is_prime = false;
                }
                if (is_prime) count++;
            }
            return static_cast<double>(count);
        });

        // Task 2: Heavy mathematical series - Using factory API
        auto task2 = Tasks::map<int, double>([](const int& x) {
            double result = 0.0;
            int iterations = x * 500000;  // Much more computation
            for (int i = 1; i <= iterations; ++i) {
                double angle = i * 0.001;
                result += std::sin(angle) * std::cos(angle * 2) *
                          std::tan(angle * 0.5);
                result += std::log(1.0 + std::sqrt(i)) / std::pow(i, 0.3);
                result += std::exp(-angle * 0.001) * std::sinh(angle * 0.0001);
                if (i % 10000 == 0 && std::abs(result) > 1e8) {
                    result /= 1e6;  // Prevent overflow
                }
            }
            return result;
        });

        // Task 3: Heavy combinatorial computation - Using factory API
        auto task3 = Tasks::map<int, double>([](const int& x) {
            double result = 0.0;
            int base_iterations = x * 300000;

            // Nested loops for O(n²) complexity
            for (int i = 1; i <= base_iterations / 100; ++i) {
                for (int j = 1; j <= 100; ++j) {
                    result += std::sqrt(i * j) * std::log10(1.0 + i + j);
                    result += std::pow(1.0 + 1.0 / i, j * 0.001);

                    // Some branching to make CPU work harder
                    if ((i + j) % 7 == 0) {
                        result *= 1.000001;
                    } else {
                        result += std::sin(i) * std::cos(j);
                    }
                }
            }
            return result;
        });

        // Task 4: Matrix-like computation - Using factory API
        auto task4 = Tasks::map<int, double>([](const int& x) {
            double result = 0.0;
            int matrix_size = x * 500;  // Simulate matrix operations

            for (int i = 0; i < matrix_size; ++i) {
                for (int j = 0; j < matrix_size; ++j) {
                    double val = std::sin(i * 0.01) + std::cos(j * 0.01);
                    result += val * val;
                    result += std::sqrt(std::abs(val * i * j + 1));
                }
            }
            return result;
        });

        // Task 5: Heavy string/hash-like processing - Using factory API
        auto task5 = Tasks::map<int, double>([](const int& x) {
            double result = 0.0;
            int iterations = x * 200000;

            for (int i = 1; i <= iterations; ++i) {
                // Simulate complex hash/string operations
                long long hash = i;
                for (int round = 0; round < 50; ++round) {
                    hash = (hash * 1103515245LL + 12345LL) % (1LL << 31);
                    result += std::log(1.0 + hash % 1000) / (1.0 + round);
                }
            }
            return result;
        });

        return std::make_tuple(std::move(task1), std::move(task2),
                               std::move(task3), std::move(task4),
                               std::move(task5));
    };

    // Smaller dataset but extremely heavy computation per task
    std::vector<int> input;
    for (int i = 10; i <= 15;
         ++i) {  // Only 6 elements, but massive computation
        input.push_back(i);
    }
    std::cout << "Dataset: 6 integers with 5 independent VERY heavy parallel "
                 "tasks (seconds-level work)"
              << std::endl;

    // Sequential Pipeline - tasks run one after another
    {
        Pipeline pipeline;
        SequentialExecutor executor;
        auto [task1, task2, task3, task4, task5] = create_parallel_tasks();

        // Add tasks with NO dependencies - they should still run sequentially
        pipeline.add_task(std::move(task1));
        pipeline.add_task(std::move(task2));
        pipeline.add_task(std::move(task3));
        pipeline.add_task(std::move(task4));
        pipeline.add_task(std::move(task5));

        std::cout << "Running sequential pipeline (this will take a while)..."
                  << std::endl;
        auto start = std::chrono::high_resolution_clock::now();
        std::any result = executor.execute(pipeline, input);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::seconds>(end - start);
        std::cout << "Sequential Time: " << duration.count() << " seconds"
                  << std::endl;
    }

    // Thread Pipeline - tasks can run in parallel
    {
        Pipeline pipeline;
        ThreadExecutor executor;
        auto [task1, task2, task3, task4, task5] = create_parallel_tasks();

        // Add tasks with NO dependencies - they can run in parallel
        pipeline.add_task(std::move(task1));
        pipeline.add_task(std::move(task2));
        pipeline.add_task(std::move(task3));
        pipeline.add_task(std::move(task4));
        pipeline.add_task(std::move(task5));

        std::cout << "Running thread pipeline (should be much faster)..."
                  << std::endl;
        auto start = std::chrono::high_resolution_clock::now();
        std::any result = executor.execute(pipeline, input);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::seconds>(end - start);
        std::cout << "Thread Time: " << duration.count() << " seconds"
                  << std::endl;
    }

    std::cout << std::endl;
}

static void demonstrate_complex_dag() {
    std::cout << "=== Complex DAG Example ===" << std::endl;

    Pipeline pipeline;
    ThreadExecutor executor;

    // Create a more complex DAG:
    //     filter1 -> map1 -> reduce1
    //          \              /
    //           -> map2 ------

    // Using factory API for cleaner task creation
    auto filter_task = Tasks::filter<int>([](const int& x) { return x > 0; });
    auto map1_task =
        Tasks::map<int, double>([](const int& x) { return x * 2.0; });
    auto map2_task =
        Tasks::map<int, double>([](const int& x) { return x / 2.0; });

    // Note: This is a simplified example - real DAG merging would need more
    // complex logic
    auto sum_task = Tasks::sum<double>();

    // Add tasks
    auto filter_id = pipeline.add_task(std::move(filter_task));
    auto map1_id = pipeline.add_task(std::move(map1_task));
    auto map2_id = pipeline.add_task(std::move(map2_task));
    auto sum_id = pipeline.add_task(std::move(sum_task));

    // Create DAG structure
    pipeline.add_dependency(filter_id, map1_id);  // map1 depends on filter
    pipeline.add_dependency(filter_id, map2_id);  // map2 depends on filter
    pipeline.add_dependency(map1_id, sum_id);     // sum depends on map1
    // Note: In a real implementation, you'd need a merge task for multiple
    // inputs

    std::vector<int> input = {1, 2, 3, 4, 5};
    std::cout << "Input: ";
    for (int x : input) std::cout << x << " ";
    std::cout << std::endl;

    try {
        auto start = std::chrono::high_resolution_clock::now();
        std::any result = executor.execute(pipeline, input);
        auto end = std::chrono::high_resolution_clock::now();

        double final_result = std::any_cast<double>(result);
        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        std::cout << "Result: " << final_result << std::endl;
        std::cout << "Time: " << duration.count() << " microseconds"
                  << std::endl;
    } catch (const PipelineError& e) {
        std::cout << "Pipeline Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    std::cout << "DFTracer Pipeline Examples" << std::endl;
    std::cout << "=========================" << std::endl << std::endl;

    try {
        // Demonstrate complex DAG
        demonstrate_complex_dag();

        // Demonstrate sequential vs thread comparison
        demonstrate_sequential_vs_thread_comparison();

    } catch (const PipelineError& e) {
        std::cerr << "Pipeline Error: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
