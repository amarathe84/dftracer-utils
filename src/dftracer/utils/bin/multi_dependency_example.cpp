#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/executor_factory.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>

#include <any>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace dftracer::utils;

class DataFetchTask : public Task {
   private:
    std::string source_name_;
    int delay_ms_;

   public:
    DataFetchTask(const std::string& source_name, int delay_ms = 100)
        : Task(typeid(int), typeid(std::string)),
          source_name_(source_name),
          delay_ms_(delay_ms) {}

    std::any execute(std::any& input) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
        std::cout << "✓ Fetched data from " << source_name_ << std::endl;
        return std::string("data_from_") + source_name_;
    }
};

class DataCombineTask : public Task {
   public:
    DataCombineTask()
        : Task(typeid(std::vector<std::any>), typeid(std::string)) {}

    std::any execute(std::any& input) override {
        auto inputs = std::any_cast<std::vector<std::any>>(input);
        std::cout << "🔗 Combining " << inputs.size()
                  << " data sources:" << std::endl;

        std::string combined_result = "COMBINED[";
        for (size_t i = 0; i < inputs.size(); ++i) {
            std::string data = std::any_cast<std::string>(inputs[i]);
            std::cout << "   - Input " << i << ": " << data << std::endl;
            combined_result += data;
            if (i < inputs.size() - 1) combined_result += " + ";
        }
        combined_result += "]";

        std::cout << "✓ Combined result: " << combined_result << std::endl;
        return combined_result;
    }
};

class ProcessTask : public Task {
   private:
    std::string process_name_;

   public:
    ProcessTask(const std::string& name)
        : Task(typeid(std::string), typeid(std::string)), process_name_(name) {}

    std::any execute(std::any& input) override {
        std::string data = std::any_cast<std::string>(input);
        std::cout << "⚙️  Processing '" << data << "' with " << process_name_
                  << std::endl;
        return process_name_ + "_PROCESSED(" + data + ")";
    }
};

int main() {
    std::cout << "=== Multi-Dependency Pipeline Example ===" << std::endl;
    std::cout << "Scenario: Combine data from multiple sources, then process "
                 "the result"
              << std::endl;
    std::cout << std::endl;

    Pipeline pipeline;

    // Task 0: Fetch from database (takes 150ms)
    pipeline.add_task(std::make_unique<DataFetchTask>("DATABASE", 150));

    // Task 1: Fetch from API (takes 200ms)
    pipeline.add_task(std::make_unique<DataFetchTask>("API", 200));

    // Task 2: Fetch from file (takes 100ms)
    pipeline.add_task(std::make_unique<DataFetchTask>("FILE", 100));

    // Task 3: Combine all three data sources (depends on tasks 0, 1, 2)
    pipeline.add_task(std::make_unique<DataCombineTask>());

    // Task 4: Process the combined data (depends on task 3)
    pipeline.add_task(std::make_unique<ProcessTask>("ML_ALGORITHM"));

    // Add dependencies: Task 3 depends on tasks 0, 1, 2 (multiple
    // dependencies!)
    pipeline.add_dependency(0, 3);  // DATABASE -> COMBINE
    pipeline.add_dependency(1, 3);  // API -> COMBINE
    pipeline.add_dependency(2, 3);  // FILE -> COMBINE

    // Add dependency: Task 4 depends on task 3 (single dependency)
    pipeline.add_dependency(3, 4);  // COMBINE -> PROCESS

    std::cout << "Pipeline structure:" << std::endl;
    std::cout << "  [DATABASE] ──┐" << std::endl;
    std::cout << "  [API]      ──┼─► [COMBINE] ──► [PROCESS]" << std::endl;
    std::cout << "  [FILE]     ──┘" << std::endl;
    std::cout << std::endl;

    // Test with different executors
    std::vector<std::string> executor_types = {"sequential", "thread"};

    for (const auto& exec_type : executor_types) {
        std::cout << "--- Running with " << exec_type << " executor ---"
                  << std::endl;

        auto start_time = std::chrono::high_resolution_clock::now();

        try {
            std::unique_ptr<Executor> executor;
            if (exec_type == "sequential") {
                executor = ExecutorFactory::create_sequential();
            } else {
                executor = ExecutorFactory::create_thread(4);
            }

            std::any result = executor->execute(pipeline, 42);

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);

            std::string final_result = std::any_cast<std::string>(result);
            std::cout << "🎯 Final result: " << final_result << std::endl;
            std::cout << "⏱️  Execution time: " << duration.count() << "ms"
                      << std::endl;

            if (exec_type == "sequential") {
                std::cout << "   (Sequential: DATABASE(150ms) + API(200ms) + "
                             "FILE(100ms) + COMBINE + PROCESS = ~450ms+)"
                          << std::endl;
            } else {
                std::cout << "   (Threaded: max(DATABASE(150ms), API(200ms), "
                             "FILE(100ms)) + COMBINE + PROCESS = ~200ms+)"
                          << std::endl;
            }

        } catch (const std::exception& e) {
            std::cout << "❌ Error: " << e.what() << std::endl;
        }

        std::cout << std::endl;
    }

    std::cout << "=== Key Multi-Dependency Insights ===" << std::endl;
    std::cout
        << "1. 🔄 Task 3 waits for ALL dependencies (0,1,2) before starting"
        << std::endl;
    std::cout << "2. 📦 Multiple dependency inputs are combined into "
                 "std::vector<std::any>"
              << std::endl;
    std::cout << "3. ⚡ ThreadScheduler runs DATABASE/API/FILE in parallel "
                 "(~200ms vs ~450ms)"
              << std::endl;
    std::cout << "4. 🔒 Dependency counting ensures correctness with atomic "
                 "operations"
              << std::endl;
    std::cout
        << "5. 🚀 Work-stealing optimizes CPU utilization across all threads"
        << std::endl;

    return 0;
}