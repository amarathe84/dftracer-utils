#include <dftracer/utils/pipeline/stream.h>

#include <any>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <vector>

using namespace dftracer::utils;
using namespace dftracer::utils::ops;

static void demonstrate_streaming_basic() {
    std::cout << "=== Streaming Interface Basic Example ===" << std::endl;

    std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    std::cout << "Input: ";
    for (int x : data) std::cout << x << " ";
    std::cout << std::endl;

    try {
        // Streaming pipeline: filter > 5, double them, then sum
        auto result = stream(data) | filter([](int x) { return x > 5; }) |
                      map([](int x) { return x * 2.0; }) | sum() |
                      execute_sequential();

        double final_result = std::any_cast<double>(result);
        std::cout << "Result (filter >5, double, sum): " << final_result
                  << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

static void demonstrate_streaming_complex() {
    std::cout << "=== Streaming Interface Complex Pipeline ===" << std::endl;

    std::vector<int> large_data;
    for (int i = 1; i <= 1000; ++i) {
        large_data.push_back(i);
    }

    std::cout << "Processing 1000 integers with streaming operations..."
              << std::endl;

    auto benchmark_pipeline = [&](const std::string& name, auto executor) {
        auto start = std::chrono::high_resolution_clock::now();

        auto result =
            stream(large_data) |
            filter([](int x) { return x % 2 == 0; })  // Keep even numbers
            | map([](int x) {
                  return std::sqrt(x * x + 1.0);
              })  // Square root transformation
            | filter([](double x) { return x > 10.0; })  // Keep larger values
            |
            map([](double x) { return std::log(x) * 2.0; })  // Apply logarithm
            | sum() | std::move(executor);

        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        double final_result = std::any_cast<double>(result);

        std::cout << name << " Result: " << std::fixed << std::setprecision(2)
                  << final_result << " (took " << duration.count() << " μs)"
                  << std::endl;

        return final_result;
    };

    try {
        // Compare different execution engines with streaming syntax
        double seq_result =
            benchmark_pipeline("Sequential", execute_sequential());
        double thread_result =
            benchmark_pipeline("Threaded  ", execute_threaded());

        // Verify results match
        if (std::abs(seq_result - thread_result) < 1e-6) {
            std::cout << "✓ Results match between execution engines"
                      << std::endl;
        } else {
            std::cout << "✗ Results differ between engines!" << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

static void demonstrate_streaming_reductions() {
    std::cout << "=== Streaming Interface Reduction Operations ==="
              << std::endl;

    std::vector<double> data = {1.5, 2.3, 3.7, 4.1, 5.9,
                                6.2, 7.8, 8.4, 9.1, 10.6};

    std::cout << "Input: ";
    for (double x : data)
        std::cout << std::fixed << std::setprecision(1) << x << " ";
    std::cout << std::endl;

    try {
        // Sum using streaming interface
        auto sum_result = stream(data) | sum() | execute_sequential();
        std::cout << "Sum: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(sum_result) << std::endl;

        // Product using streaming interface
        auto product_result = stream(data) | product() | execute_sequential();
        std::cout << "Product: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(product_result) << std::endl;

        // Max using streaming interface
        auto max_result = stream(data) | max<double>() | execute_sequential();
        std::cout << "Max: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(max_result) << std::endl;

        // Min using streaming interface
        auto min_result = stream(data) | min<double>() | execute_sequential();
        std::cout << "Min: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(min_result) << std::endl;

        // Complex chained operations with streaming
        auto chained_result = stream(data) |
                              filter([](double x) { return x > 5.0; }) |
                              map([](double x) { return x * x; })  // Square
                              | max<double>() | execute_threaded();

        std::cout << "Max of squares (>5.0): " << std::fixed
                  << std::setprecision(2)
                  << std::any_cast<double>(chained_result) << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

static void demonstrate_streaming_vs_fluent() {
    std::cout << "=== Streaming vs Fluent API Comparison ===" << std::endl;

    std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    std::cout << "Comparing streaming vs fluent syntax..." << std::endl;

    try {
        // Fluent API style
        auto fluent_result =
            from(data)
                .filter([](int x) { return x % 2 == 0; })
                .template map<double>([](int x) { return x * 1.5; })
                .sum()
                .execute_sequential();

        // Streaming API style
        auto stream_result =
            stream(data) | filter([](int x) { return x % 2 == 0; }) |
            map([](int x) { return x * 1.5; }) | sum() | execute_sequential();

        double fluent_val = std::any_cast<double>(fluent_result);
        double stream_val = std::any_cast<double>(stream_result);

        std::cout << "Fluent API result:   " << std::fixed
                  << std::setprecision(2) << fluent_val << std::endl;
        std::cout << "Streaming API result: " << std::fixed
                  << std::setprecision(2) << stream_val << std::endl;

        if (std::abs(fluent_val - stream_val) < 1e-6) {
            std::cout << "✓ Both APIs produce identical results!" << std::endl;
        } else {
            std::cout << "✗ Results differ!" << std::endl;
        }

        std::cout << std::endl;
        std::cout << "Syntax comparison:" << std::endl;
        std::cout
            << "Fluent:    "
               "from(data).filter(...).map<T>(...).sum().execute_sequential()"
            << std::endl;
        std::cout << "Streaming: stream(data) | filter(...) | map(...) | sum() "
                     "| execute_sequential()"
                  << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

static void demonstrate_new_operations() {
    std::cout << "=== New Pipeline Operations Demo ===" << std::endl;

    std::vector<int> data = {5, 2, 8, 2, 1, 9, 5, 3, 7, 4, 1, 6, 8, 3, 9};

    std::cout << "Input data: ";
    for (int x : data) std::cout << x << " ";
    std::cout << std::endl;

    try {
        // Take/Limit demonstration
        auto take_result = stream(data) | take(5) | execute_sequential();
        auto taken_vec = std::any_cast<std::vector<int>>(take_result);
        std::cout << "Take 5: ";
        for (int x : taken_vec) std::cout << x << " ";
        std::cout << std::endl;

        // Skip/Drop demonstration
        auto skip_result =
            stream(data) | skip(3) | take(7) | execute_sequential();
        auto skipped_vec = std::any_cast<std::vector<int>>(skip_result);
        std::cout << "Skip 3, take 7: ";
        for (int x : skipped_vec) std::cout << x << " ";
        std::cout << std::endl;

        // Distinct demonstration
        auto distinct_result = stream(data) | distinct() | execute_sequential();
        auto distinct_vec = std::any_cast<std::vector<int>>(distinct_result);
        std::cout << "Distinct: ";
        for (int x : distinct_vec) std::cout << x << " ";
        std::cout << std::endl;

        // Complex pipeline with new operations
        auto complex_result =
            stream(data) | distinct()              // Remove duplicates
            | filter([](int x) { return x > 3; })  // Keep values > 3
            | skip(2)                              // Skip first 2 results
            | take(4)                              // Take only 4 elements
            | map([](int x) { return x * x; })     // Square them
            | sum()                                // Sum the results
            | execute_threaded();

        int complex_val = std::any_cast<int>(complex_result);
        std::cout << "Complex pipeline result: " << complex_val << std::endl;

        std::cout << "Pipeline: distinct() | filter(>3) | skip(2) | take(4) | "
                     "square | sum"
                  << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

int main() {
    std::cout << "DFTracer Streaming Pipeline API Examples" << std::endl;
    std::cout << "=======================================" << std::endl
              << std::endl;

    try {
        demonstrate_streaming_basic();
        demonstrate_streaming_complex();
        demonstrate_streaming_reductions();
        demonstrate_streaming_vs_fluent();
        demonstrate_new_operations();

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}