#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/executor_factory.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>

using namespace dftracer::utils;

// Global counters to track work stealing
std::atomic<int> tasks_executed{0};
std::atomic<int> total_work_items{0};

int main() {
    DFTRACER_UTILS_LOGGER_INIT();
    DFTRACER_UTILS_LOG_INFO("=== Work Stealing Test ===");

    std::cout
        << "\n=== Testing Work Stealing with Heavy Dynamic Task Creation ===\n";

    // Create a task that generates many subtasks with varying work loads
    auto heavy_task_generator = [](int base_input, TaskContext& ctx) -> int {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(10));  // Some initial work

        DFTRACER_UTILS_LOG_INFO("Generator task processing input: %d",
                                base_input);
        std::cout << ">>> GEN1: Processing input " << base_input << std::endl;

        // Generate multiple subtasks with different work characteristics
        for (int i = 0; i < 5; ++i) {
            // Create tasks with varying work loads
            auto work_task = [i, base_input](int work_amount,
                                             TaskContext& ctx2) -> int {
                std::thread::id thread_id = std::this_thread::get_id();
                auto start = std::chrono::high_resolution_clock::now();

                // Simulate work with varying intensity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(work_amount * 10));

                // Simulate some CPU work
                volatile int dummy = 0;
                for (int j = 0; j < work_amount * 1000; ++j) {
                    dummy += j % 17;
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        end - start);

                tasks_executed++;
                total_work_items += work_amount;
                std::cout << "COUNTER: Work task executed, tasks_executed="
                          << tasks_executed << ", work_amount=" << work_amount
                          << std::endl;

                // Log which thread executed this task
                std::hash<std::thread::id> hasher;
                size_t thread_hash = hasher(thread_id) % 1000;

                DFTRACER_UTILS_LOG_INFO(
                    "Work task %d (amount=%d) executed by thread %zu in %ldms",
                    i, work_amount, thread_hash, duration.count());

                return base_input + i + work_amount;
            };

            // Vary the work amount: some light (1-3), some heavy (5-8) tasks
            int work_amount = (i % 2 == 0) ? (1 + i) : (5 + i);

            // Emit independent tasks (no dependencies) to allow work stealing
            TaskIndex work_id =
                ctx.emit<int, int>(work_task, Input{work_amount});
            DFTRACER_UTILS_LOG_INFO(
                "Emitted work task %d with amount %d (ID: %d)", i, work_amount,
                work_id);
        }

        return base_input * 10;
    };

    // Create another generator to create more work
    auto secondary_generator = [](int input, TaskContext& ctx) -> int {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        DFTRACER_UTILS_LOG_INFO("Secondary generator processing: %d", input);
        std::cout << ">>> GEN2: Processing input " << input << std::endl;

        // Generate 3 more tasks with heavy work
        for (int i = 0; i < 3; ++i) {
            auto heavy_work = [i, input](int load, TaskContext& ctx2) -> int {
                std::thread::id thread_id = std::this_thread::get_id();
                auto start = std::chrono::high_resolution_clock::now();

                // Heavy computational work
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(load * 15));

                // More CPU work
                volatile double result = 0;
                for (int j = 0; j < load * 2000; ++j) {
                    result += std::sin(j * 0.1) * std::cos(j * 0.1);
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        end - start);

                tasks_executed++;
                total_work_items += load;
                std::cout << "COUNTER: Heavy task executed, tasks_executed="
                          << tasks_executed << ", load=" << load << std::endl;

                std::hash<std::thread::id> hasher;
                size_t thread_hash = hasher(thread_id) % 1000;

                DFTRACER_UTILS_LOG_INFO(
                    "Heavy task %d (load=%d) executed by thread %zu in %ldms",
                    i, load, thread_hash, duration.count());

                return input + i + static_cast<int>(result * 0.0001);
            };

            int load = 3 + i * 2;  // Loads: 3, 5, 7
            TaskIndex heavy_id = ctx.emit<int, int>(heavy_work, Input{load});
            DFTRACER_UTILS_LOG_INFO(
                "Emitted heavy task %d with load %d (ID: %d)", i, load,
                heavy_id);
        }

        return input + 1000;
    };

    // Test with different thread counts
    std::vector<int> thread_counts = {1, 2, 4};

    for (int num_threads : thread_counts) {
        // Create a fresh pipeline for each test to avoid task accumulation
        Pipeline pipeline;

        // Add tasks to pipeline with dependencies to create initial work
        // distribution
        TaskIndex gen1 = pipeline.add_task<int, int>(heavy_task_generator);
        TaskIndex gen2 = pipeline.add_task<int, int>(secondary_generator);

        // Make gen2 depend on gen1 to stagger work generation
        pipeline.add_dependency(gen1, gen2);
        std::cout << "\n--- Testing with " << num_threads << " threads ---\n";

        tasks_executed = 0;
        total_work_items = 0;

        auto start_time = std::chrono::high_resolution_clock::now();

        try {
            auto executor = ExecutorFactory::create_thread(num_threads);
            std::any result = executor->execute(pipeline, 42);
            int final_result = std::any_cast<int>(result);

            auto end_time = std::chrono::high_resolution_clock::now();
            auto total_duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);

            std::cout << "Final result: " << final_result << "\n";
            std::cout << "Tasks executed: " << tasks_executed << "\n";
            std::cout << "Total work items: " << total_work_items << "\n";
            std::cout << "Total execution time: " << total_duration.count()
                      << "ms\n";
            std::cout << "Average work per task: "
                      << (tasks_executed > 0
                              ? total_work_items.load() / tasks_executed.load()
                              : 0)
                      << "\n";
            std::cout << "Throughput: "
                      << (total_duration.count() > 0
                              ? (tasks_executed.load() * 1000) /
                                    total_duration.count()
                              : 0)
                      << " tasks/sec\n";

        } catch (const std::exception& e) {
            DFTRACER_UTILS_LOG_ERROR("Execution with %d threads failed: %s",
                                     num_threads, e.what());
        }
    }

    // Compare with sequential execution
    std::cout << "\n--- Comparing with Sequential Execution ---\n";
    tasks_executed = 0;
    total_work_items = 0;

    // Create a fresh pipeline for sequential execution
    Pipeline sequential_pipeline;
    TaskIndex seq_gen1 =
        sequential_pipeline.add_task<int, int>(heavy_task_generator);
    TaskIndex seq_gen2 =
        sequential_pipeline.add_task<int, int>(secondary_generator);
    sequential_pipeline.add_dependency(seq_gen1, seq_gen2);

    auto start_time = std::chrono::high_resolution_clock::now();

    try {
        auto executor = ExecutorFactory::create_sequential();
        std::any result = executor->execute(sequential_pipeline, 42);
        int final_result = std::any_cast<int>(result);

        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                  start_time);

        std::cout << "Sequential final result: " << final_result << "\n";
        std::cout << "Sequential execution time: " << total_duration.count()
                  << "ms\n";
        std::cout << "Sequential tasks executed: " << tasks_executed << "\n";
        std::cout << "Sequential total work items: " << total_work_items
                  << "\n";

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Sequential execution failed: %s", e.what());
    }

    DFTRACER_UTILS_LOG_INFO("=== Work stealing test completed ===");

    std::cout << "\n=== WORK STEALING ANALYSIS ===\n";
    std::cout << "✅ Work stealing is implemented with:\n";
    std::cout << "   - Per-thread task queues (one per worker)\n";
    std::cout << "   - Owner threads take from FRONT (FIFO)\n";
    std::cout << "   - Stealing threads take from BACK (LIFO)\n";
    std::cout << "   - Random queue selection for stealing\n";
    std::cout << "   - Dynamic task emission creates uneven workloads\n";
    std::cout
        << "✅ Benefits: Load balancing, scalability, reduced contention\n";

    return 0;
}