#include <dftracer/utils/pipeline/builder.h>

#include <any>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <vector>

using namespace dftracer::utils;

static void demonstrate_fluent_api_basic() {
    std::cout << "=== Fluent API Basic Example ===" << std::endl;

    std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    std::cout << "Input: ";
    for (int x : data) std::cout << x << " ";
    std::cout << std::endl;

    try {
        // Fluent pipeline: filter positive numbers, double them, then sum
        auto result = from(data)
                          .filter([](int x) { return x > 5; })
                          .template map<double>([](int x) { return x * 2.0; })
                          .sum()
                          .execute_sequential();

        double final_result = std::any_cast<double>(result);
        std::cout << "Result (filter >5, double, sum): " << final_result
                  << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

static void demonstrate_fluent_api_complex() {
    std::cout << "=== Fluent API Complex Pipeline ===" << std::endl;

    std::vector<int> large_data;
    for (int i = 1; i <= 1000; ++i) {
        large_data.push_back(i);
    }

    std::cout << "Processing 1000 integers with complex transformations..."
              << std::endl;

    auto run_pipeline = [&](const std::string& name, auto executor) {
        auto start = std::chrono::high_resolution_clock::now();

        auto pipeline =
            from(large_data)
                .filter([](int x) { return x % 2 == 0; })  // Keep even numbers
                .template map<double>([](int x) {  // Square root transformation
                    return std::sqrt(x * x + 1.0);
                })
                .filter(
                    [](double x) { return x > 10.0; })  // Keep larger values
                .template map<double>([](double x) {    // Apply logarithm
                    return std::log(x) * 2.0;
                })
                .sum();

        auto result = executor(std::move(pipeline));

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
        // Compare different execution engines
        double seq_result =
            run_pipeline("Sequential", [](auto&& builder) -> std::any {
                return std::move(builder).execute_sequential();
            });

        double thread_result =
            run_pipeline("Threaded  ", [](auto&& builder) -> std::any {
                return std::move(builder).execute_threaded();
            });

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

static void demonstrate_fluent_api_reductions() {
    std::cout << "=== Fluent API Reduction Operations ===" << std::endl;

    std::vector<double> data = {1.5, 2.3, 3.7, 4.1, 5.9,
                                6.2, 7.8, 8.4, 9.1, 10.6};

    std::cout << "Input: ";
    for (double x : data)
        std::cout << std::fixed << std::setprecision(1) << x << " ";
    std::cout << std::endl;

    try {
        // Sum
        auto sum_result = from(data).sum().execute_sequential();
        std::cout << "Sum: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(sum_result) << std::endl;

        // Product
        auto product_result = from(data).product().execute_sequential();
        std::cout << "Product: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(product_result) << std::endl;

        // Max
        auto max_result = from(data).max().execute_sequential();
        std::cout << "Max: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(max_result) << std::endl;

        // Min
        auto min_result = from(data).min().execute_sequential();
        std::cout << "Min: " << std::fixed << std::setprecision(2)
                  << std::any_cast<double>(min_result) << std::endl;

        // Chained operations
        auto chained_result =
            from(data)
                .filter([](double x) { return x > 5.0; })
                .template map<double>([](double x) { return x * x; })  // Square
                .max()
                .execute_threaded();

        std::cout << "Max of squares (>5.0): " << std::fixed
                  << std::setprecision(2)
                  << std::any_cast<double>(chained_result) << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << std::endl;
}

int main() {
    std::cout << "DFTracer Fluent Pipeline API Examples" << std::endl;
    std::cout << "====================================" << std::endl
              << std::endl;

    try {
        demonstrate_fluent_api_basic();
        demonstrate_fluent_api_complex();
        demonstrate_fluent_api_reductions();

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}