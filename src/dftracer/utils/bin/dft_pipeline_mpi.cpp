#include <dftracer/utils/pipeline/mpi_pipeline.h>
#include <dftracer/utils/pipeline/sequential_pipeline.h>
#include <dftracer/utils/pipeline/tasks/factory.h>
#include <dftracer/utils/utils/mpi.h>

#include <any>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <memory>
#include <vector>

using namespace dftracer::utils;

static void demonstrate_mpi_pipeline() {
    MPIPipeline pipeline;

    if (pipeline.is_master()) {
        std::cout << "=== MPI Pipeline Example ===" << std::endl;
        std::cout << "Running distributed pipeline with " << pipeline.size()
                  << " processes" << std::endl;
    }

    // Create multiple independent CPU-intensive tasks using factory API
    auto task1 = Tasks::map<int, double>([](const int& x) {
        // Prime number computation
        int count = 0;
        int limit = x * 200000;  // Much heavier computation
        for (int i = 2; i <= limit; ++i) {
            bool is_prime = true;
            for (int j = 2; j * j <= i && is_prime; ++j) {
                if (i % j == 0) is_prime = false;
            }
            if (is_prime) count++;
        }
        return static_cast<double>(count);
    });

    auto task2 = Tasks::map<int, double>([](const int& x) {
        double result = 0.0;
        int iterations = x * 500000;
        for (int i = 1; i <= iterations; ++i) {
            double angle = i * 0.001;
            result += std::sin(angle) * std::cos(angle * 2);
            result += std::log(1.0 + std::sqrt(i)) / std::pow(i, 0.3);
            if (i % 10000 == 0 && std::abs(result) > 1e8) {
                result /= 1e6;
            }
        }
        return result;
    });

    auto task3 = Tasks::map<int, double>([](const int& x) {
        // Matrix-like computation
        double result = 0.0;
        int size = x * 1000;
        for (int i = 0; i < size; ++i) {
            for (int j = 0; j < size; ++j) {
                double val = std::sin(i * 0.01) + std::cos(j * 0.01);
                result += val * val;
                result += std::sqrt(std::abs(val * i * j + 1));
            }
        }
        return result;
    });

    auto task4 = Tasks::map<int, double>([](const int& x) {
        // Hash-like processing
        double result = 0.0;
        int iterations = x * 400000;
        for (int i = 1; i <= iterations; ++i) {
            long long hash = i;
            for (int round = 0; round < 30; ++round) {
                hash = (hash * 1103515245LL + 12345LL) % (1LL << 31);
                result += std::log(1.0 + static_cast<double>(hash % 1000)) /
                          (1.0 + round);
            }
        }
        return result;
    });

    auto task5 = Tasks::map<int, double>([](const int& x) {
        // Combinatorial computation
        double result = 0.0;
        int base = x * 600000;
        for (int i = 1; i <= base / 50; ++i) {
            for (int j = 1; j <= 50; ++j) {
                result += std::sqrt(i * j) * std::log10(1.0 + i + j);
                if ((i + j) % 7 == 0) {
                    result *= 1.000001;
                } else {
                    result += std::sin(i) * std::cos(j);
                }
            }
        }
        return result;
    });

    // Add all tasks to the pipeline (no dependencies - can run in parallel)
    pipeline.add_task(std::move(task1));
    pipeline.add_task(std::move(task2));
    pipeline.add_task(std::move(task3));
    pipeline.add_task(std::move(task4));
    pipeline.add_task(std::move(task5));

    // Prepare input data
    std::vector<int> input;
    for (int i = 10; i <= 15; ++i) {  // Small dataset but heavy computation
        input.push_back(i);
    }

    if (pipeline.is_master()) {
        std::cout << "Input dataset: 6 integers with 5 independent heavy tasks"
                  << std::endl;
        std::cout << "Starting MPI distributed execution..." << std::endl;
    }

    try {
        auto start = std::chrono::high_resolution_clock::now();
        std::any result = pipeline.execute(input);
        auto end = std::chrono::high_resolution_clock::now();

        if (pipeline.is_master()) {
            auto duration =
                std::chrono::duration_cast<std::chrono::seconds>(end - start);
            std::cout << "MPI Pipeline completed in: " << duration.count()
                      << " seconds" << std::endl;

            // Try to extract and display the final result
            try {
                auto final_result = std::any_cast<std::vector<double>>(result);
                std::cout << "Final result size: " << final_result.size()
                          << std::endl;
                std::cout << "First few results: ";
                for (size_t i = 0; i < std::min(size_t(3), final_result.size());
                     ++i) {
                    std::cout << std::fixed << std::setprecision(2)
                              << final_result[i] << " ";
                }
                std::cout << std::endl;
            } catch (const std::bad_any_cast& e) {
                std::cout << "Result type: " << result.type().name()
                          << std::endl;
            }
        }

    } catch (const PipelineError& e) {
        if (pipeline.is_master()) {
            std::cerr << "Pipeline Error: " << e.what() << std::endl;
        }
    } catch (const std::exception& e) {
        if (pipeline.is_master()) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }

    if (pipeline.is_master()) {
        std::cout << std::endl;
    }
}

static void demonstrate_mpi_vs_sequential_comparison() {
    MPIPipeline mpi_pipeline;

    if (mpi_pipeline.is_master()) {
        std::cout << "=== MPI vs Sequential Direct Comparison ===" << std::endl;
        std::cout << "Running identical workloads on sequential vs distributed "
                     "pipeline"
                  << std::endl;
    }

    // Create a CPU-intensive task factory function that can be reused
    auto create_cpu_intensive_task = [](int task_id) {
        return Tasks::map<int, double>([task_id](const int& x) {
            double result = 0.0;
            int base_work =
                x * 2000000 + task_id * 500000;  // Extremely heavy computation

            if (task_id % 3 == 0) {
                // Prime counting - limited to prevent overflow
                int limit =
                    std::min(base_work, 5000000);  // Higher computation cap
                for (int i = 2; i <= limit; ++i) {
                    bool is_prime = true;
                    for (int j = 2; j * j <= i && is_prime; ++j) {
                        if (i % j == 0) is_prime = false;
                    }
                    if (is_prime) result += 1.0;
                }
            } else if (task_id % 3 == 1) {
                // Mathematical computation - limited and safe
                int limit = std::min(base_work * 5,
                                     10000000);  // Much higher iteration cap
                for (int i = 1; i <= limit; ++i) {
                    double angle = i * 0.0001;
                    double sin_val = std::sin(angle);
                    double cos_val = std::cos(angle * 2);
                    if (std::isfinite(sin_val) && std::isfinite(cos_val)) {
                        result += sin_val * cos_val;
                    }
                    if (i > 1) {
                        double log_val = std::log(1.0 + i) / (1.0 + i);
                        if (std::isfinite(log_val)) {
                            result += log_val;
                        }
                    }
                    // Aggressive overflow prevention
                    if (i % 1000 == 0) {
                        if (!std::isfinite(result) || std::abs(result) > 1e6) {
                            result = std::copysign(
                                1e6, result);  // Clamp to safe range
                        }
                    }
                }
            } else {
                // Matrix-like computation with safe operations
                int size = static_cast<int>(std::sqrt(base_work * 2));
                size = std::min(size, 5000);  // Higher matrix size cap
                for (int i = 0; i < size; ++i) {
                    for (int j = 0; j < size; ++j) {
                        double sin_val = std::sin(i * 0.001);
                        double cos_val = std::cos(j * 0.001);
                        double val = sin_val + cos_val;

                        if (std::isfinite(val)) {
                            result += val * val;
                            double sqrt_val =
                                std::sqrt(std::abs(val * i * j + 1));
                            if (std::isfinite(sqrt_val)) {
                                result += sqrt_val;
                            }

                            // Safer power computation
                            for (int k = 0; k < 3;
                                 ++k) {  // Reduced from 5 to 3
                                double power = 1.0 + k * 0.1;
                                if (std::abs(val) <
                                    10.0) {  // Much tighter bound
                                    double pow_val =
                                        std::pow(std::abs(val), power);
                                    if (std::isfinite(pow_val)) {
                                        result += pow_val;
                                    }
                                }
                            }
                        }

                        // Continuous overflow control
                        if ((i * size + j) % 1000 == 0) {
                            if (!std::isfinite(result) ||
                                std::abs(result) > 1e6) {
                                result = std::copysign(1e6, result);
                            }
                        }
                    }
                }
            }

            // Final safety check
            if (!std::isfinite(result)) {
                result = 1e6;  // Return a reasonable fallback value
            }

            return result;
        });
    };

    // Input data - even larger values for extremely heavy computation
    std::vector<int> input = {25, 30,
                              35};  // Even larger input for extreme work
    int num_tasks =
        mpi_pipeline.size() * 4;  // 4 tasks per process for maximum load

    if (mpi_pipeline.is_master()) {
        std::cout << "Input: " << input.size() << " integers" << std::endl;
        std::cout << "Tasks: " << num_tasks
                  << " CPU-intensive independent tasks" << std::endl;
        std::cout << "Processes: " << mpi_pipeline.size() << std::endl;
        std::cout << std::endl;
    }

    long long sequential_time = 0;
    long long mpi_time = 0;
    std::vector<double> sequential_result;
    std::vector<double> mpi_result;

    // Run Sequential Pipeline (only on master to avoid duplicate work)
    if (mpi_pipeline.is_master()) {
        std::cout << "Running Sequential Pipeline..." << std::endl;
        SequentialPipeline seq_pipeline;

        // Add identical tasks to sequential pipeline
        for (int i = 0; i < num_tasks; ++i) {
            seq_pipeline.add_task(create_cpu_intensive_task(i));
        }

        auto start = std::chrono::high_resolution_clock::now();
        std::any seq_result = seq_pipeline.execute(input);
        auto end = std::chrono::high_resolution_clock::now();

        sequential_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count();
        std::cout << "Sequential Time: " << sequential_time << " ms"
                  << std::endl;

        // Extract and store sequential result for comparison
        try {
            sequential_result = std::any_cast<std::vector<double>>(seq_result);
            std::cout << "Sequential result size: " << sequential_result.size()
                      << std::endl;
            std::cout << "Sequential first few values: ";
            for (size_t i = 0;
                 i < std::min(size_t(3), sequential_result.size()); ++i) {
                std::cout << std::fixed << std::setprecision(2)
                          << sequential_result[i] << " ";
            }
            std::cout << std::endl;
        } catch (const std::bad_any_cast& e) {
            std::cout << "Sequential result type: " << seq_result.type().name()
                      << std::endl;
        }
        std::cout << std::endl;
    }

    // Broadcast sequential time to all processes for comparison
    MPIContext& mpi = MPIContext::instance();
    mpi.broadcast(&sequential_time, 1, MPI_LONG_LONG, 0);

    // Run MPI Pipeline
    if (mpi_pipeline.is_master()) {
        std::cout << "Running MPI Distributed Pipeline..." << std::endl;
    }

    // Add identical tasks to MPI pipeline
    for (int i = 0; i < num_tasks; ++i) {
        mpi_pipeline.add_task(create_cpu_intensive_task(i));
    }

    try {
        auto start = std::chrono::high_resolution_clock::now();
        std::any mpi_result_any = mpi_pipeline.execute(input);
        auto end = std::chrono::high_resolution_clock::now();

        mpi_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                .count();

        if (mpi_pipeline.is_master()) {
            std::cout << "MPI Time: " << mpi_time << " ms" << std::endl;

            // Extract and store MPI result for comparison
            try {
                mpi_result = std::any_cast<std::vector<double>>(mpi_result_any);
                std::cout << "MPI result size: " << mpi_result.size()
                          << std::endl;
                std::cout << "MPI first few values: ";
                for (size_t i = 0; i < std::min(size_t(3), mpi_result.size());
                     ++i) {
                    std::cout << std::fixed << std::setprecision(2)
                              << mpi_result[i] << " ";
                }
                std::cout << std::endl;
            } catch (const std::bad_any_cast& e) {
                std::cout << "MPI result type: " << mpi_result_any.type().name()
                          << std::endl;
                // Try other possible types
                try {
                    auto int_result =
                        std::any_cast<std::vector<int>>(mpi_result_any);
                    std::cout << "MPI result (as int vector) size: "
                              << int_result.size() << std::endl;
                    std::cout << "MPI int values: ";
                    for (size_t i = 0;
                         i < std::min(size_t(3), int_result.size()); ++i) {
                        std::cout << int_result[i] << " ";
                    }
                    std::cout << std::endl;
                } catch (const std::bad_any_cast&) {
                    try {
                        double single_result =
                            std::any_cast<double>(mpi_result_any);
                        std::cout
                            << "MPI result (single double): " << single_result
                            << std::endl;
                        mpi_result = {
                            single_result};  // Convert to vector for comparison
                    } catch (const std::bad_any_cast&) {
                        std::cout
                            << "Could not cast MPI result to any expected type"
                            << std::endl;
                    }
                }
            }
            std::cout << std::endl;

            // Calculate and display performance comparison
            std::cout << "=== Performance Comparison ===" << std::endl;
            std::cout << "Sequential: " << sequential_time << " ms"
                      << std::endl;
            std::cout << "MPI (" << mpi_pipeline.size()
                      << " processes): " << mpi_time << " ms" << std::endl;

            if (mpi_time > 0) {
                double speedup = static_cast<double>(sequential_time) /
                                 static_cast<double>(mpi_time);
                double efficiency = speedup / mpi_pipeline.size() * 100.0;

                std::cout << "Speedup: " << std::fixed << std::setprecision(2)
                          << speedup << "x" << std::endl;
                std::cout << "Efficiency: " << std::fixed
                          << std::setprecision(1) << efficiency << "%"
                          << std::endl;
                std::cout << "Time saved: " << (sequential_time - mpi_time)
                          << " ms" << std::endl;

                // Compare results for correctness
                std::cout << std::endl;
                std::cout << "=== Result Comparison ===" << std::endl;
                if (!sequential_result.empty() && !mpi_result.empty()) {
                    if (sequential_result.size() == mpi_result.size()) {
                        std::cout << "Result sizes match: "
                                  << sequential_result.size() << std::endl;

                        // Check if results are approximately equal (allowing
                        // for floating-point differences)
                        bool results_match = true;
                        double max_diff = 0.0;
                        double tolerance =
                            1e-6;  // Allow small floating-point differences

                        for (size_t i = 0;
                             i < sequential_result.size() && i < 10;
                             ++i) {  // Check first 10 elements
                            double diff =
                                std::abs(sequential_result[i] - mpi_result[i]);
                            max_diff = std::max(max_diff, diff);
                            if (diff > tolerance) {
                                results_match = false;
                            }
                        }

                        if (results_match) {
                            std::cout << "✓ Results match (within tolerance "
                                      << tolerance << ")" << std::endl;
                            std::cout << "Max difference: " << std::scientific
                                      << max_diff << std::endl;
                        } else {
                            std::cout << "✗ Results differ beyond tolerance"
                                      << std::endl;
                            std::cout << "Max difference: " << std::scientific
                                      << max_diff << std::endl;
                            std::cout << "Sample differences:" << std::endl;
                            for (size_t i = 0;
                                 i <
                                 std::min(size_t(3), sequential_result.size());
                                 ++i) {
                                std::cout
                                    << "  [" << i
                                    << "] Seq: " << sequential_result[i]
                                    << " MPI: " << mpi_result[i] << " Diff: "
                                    << (sequential_result[i] - mpi_result[i])
                                    << std::endl;
                            }
                        }
                    } else {
                        std::cout << "✗ Result sizes differ: Sequential="
                                  << sequential_result.size()
                                  << " MPI=" << mpi_result.size() << std::endl;
                    }
                } else {
                    std::cout
                        << "Unable to compare results (empty or wrong type)"
                        << std::endl;
                }
            } else {
                std::cout << "MPI execution too fast to measure" << std::endl;
            }
        }

    } catch (const std::exception& e) {
        if (mpi_pipeline.is_master()) {
            std::cerr << "MPI Pipeline Error: " << e.what() << std::endl;
        }
    }

    if (mpi_pipeline.is_master()) {
        std::cout << std::endl;
    }
}

int main(int argc, char** argv) {
    MPISession mpi_session(&argc, &argv);

    MPIContext& mpi = MPIContext::instance();

    try {
        // Basic MPI pipeline demonstration
        demonstrate_mpi_pipeline();

        // Performance comparison demonstration
        demonstrate_mpi_vs_sequential_comparison();

    } catch (const std::exception& e) {
        if (mpi.is_master()) {
            std::cerr << "Fatal error: " << e.what() << std::endl;
        }
        mpi.abort(1);
        return 1;
    }

    return 0;
}
