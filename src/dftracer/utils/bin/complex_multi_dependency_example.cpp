#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/executors/executor_factory.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/function_task.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>

#include <algorithm>
#include <any>
#include <chrono>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace dftracer::utils;

// Simulate different data source types with realistic delays
class DatabaseQueryTask : public Task {
   private:
    std::string query_;
    int complexity_;

   public:
    DatabaseQueryTask(const std::string& query, int complexity = 1)
        : Task(typeid(int), typeid(std::map<std::string, int>)),
          query_(query),
          complexity_(complexity) {}

    std::any execute(std::any& input) override {
        int delay_ms = 50 + (complexity_ * 30);
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

        std::map<std::string, int> result;
        result["records"] = 1000 * complexity_;
        result["execution_time_ms"] = delay_ms;

        std::cout << "📊 DB Query '" << query_ << "' returned "
                  << result["records"] << " records (" << delay_ms << "ms)"
                  << std::endl;
        return result;
    }
};

class APICallTask : public Task {
   private:
    std::string endpoint_;
    bool should_emit_dynamic_;

   public:
    APICallTask(const std::string& endpoint, bool emit_dynamic = false)
        : Task(typeid(int), typeid(std::vector<std::string>)),
          endpoint_(endpoint),
          should_emit_dynamic_(emit_dynamic) {}

    std::any execute(std::any& input) override {
        // Use deterministic delays based on endpoint for consistent results
        int delay_ms;
        int data_count;

        if (endpoint_ == "user_profiles") {
            delay_ms = 150;
            data_count = 5;
        } else if (endpoint_ == "payment_methods") {
            delay_ms = 120;
            data_count = 4;
        } else {
            delay_ms = 100;
            data_count = 3;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

        std::vector<std::string> api_data;
        for (int i = 0; i < data_count; ++i) {
            api_data.push_back(endpoint_ + "_item_" + std::to_string(i));
        }

        std::cout << "🌐 API '" << endpoint_ << "' returned " << api_data.size()
                  << " items (" << delay_ms << "ms)";

        // Emit dynamic validation task if requested
        if (should_emit_dynamic_ && needs_context()) {
            std::cout << " [EMITTING VALIDATION TASK]";
            // This would emit a dynamic task to validate the API response
        }
        std::cout << std::endl;

        return api_data;
    }

    bool needs_context() const override { return should_emit_dynamic_; }

    void setup_context(TaskContext* context) override {
        if (should_emit_dynamic_) {
            // Create a validation task that depends on this API call
            auto validation_func = [endpoint = endpoint_](
                                       std::any input,
                                       TaskContext& ctx) -> bool {
                try {
                    // The input should be std::vector<std::string> from the API
                    // call
                    auto data = std::any_cast<std::vector<std::string>>(input);
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    bool is_valid = data.size() >= 3 && data.size() <= 10;
                    std::cout << "✅ Validation for " << endpoint << ": "
                              << (is_valid ? "PASS" : "FAIL") << std::endl;
                    return is_valid;
                } catch (const std::bad_any_cast& e) {
                    std::cout << "❌ Validation failed: Invalid input type for "
                              << endpoint << std::endl;
                    return false;
                }
            };

            // Use std::any for input type to match the dependency handling
            context->emit<std::any, bool>(validation_func,
                                          DependsOn(context->current()));
        }
    }
};

class FileProcessorTask : public Task {
   private:
    std::string file_type_;

   public:
    FileProcessorTask(const std::string& file_type)
        : Task(typeid(int), typeid(std::string)), file_type_(file_type) {}

    std::any execute(std::any& input) override {
        // Use deterministic values based on file type for consistent results
        int delay_ms;
        int file_size;

        if (file_type_ == "CSV_logs") {
            delay_ms = 90;
            file_size = 2048;
        } else if (file_type_ == "JSON_config") {
            delay_ms = 110;
            file_size = 4096;
        } else {
            delay_ms = 80;
            file_size = 1024;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

        std::string result =
            file_type_ + "_processed_" + std::to_string(file_size) + "KB";
        std::cout << "📁 File " << file_type_ << " processed: " << file_size
                  << "KB (" << delay_ms << "ms)" << std::endl;

        return result;
    }
};

// Complex aggregation task with multiple dependency types
class DataAggregatorTask : public Task {
   public:
    DataAggregatorTask()
        : Task(typeid(std::vector<std::any>),
               typeid(std::map<std::string, std::any>)) {}

    std::any execute(std::any& input) override {
        auto inputs = std::any_cast<std::vector<std::any>>(input);
        std::cout << "🔗 Aggregating " << inputs.size()
                  << " data sources:" << std::endl;

        std::map<std::string, std::any> aggregated_result;
        int total_records = 0;
        std::vector<std::string> all_items;
        std::vector<std::string> processed_files;

        for (size_t i = 0; i < inputs.size(); ++i) {
            try {
                // Try database result
                if (auto db_result =
                        std::any_cast<std::map<std::string, int>>(&inputs[i])) {
                    total_records += (*db_result)["records"];
                    std::cout << "   📊 DB Source " << i << ": "
                              << (*db_result)["records"] << " records"
                              << std::endl;
                }
                // Try API result
                else if (auto api_result =
                             std::any_cast<std::vector<std::string>>(
                                 &inputs[i])) {
                    all_items.insert(all_items.end(), api_result->begin(),
                                     api_result->end());
                    std::cout << "   🌐 API Source " << i << ": "
                              << api_result->size() << " items" << std::endl;
                }
                // Try file result
                else if (auto file_result =
                             std::any_cast<std::string>(&inputs[i])) {
                    processed_files.push_back(*file_result);
                    std::cout << "   📁 File Source " << i << ": "
                              << *file_result << std::endl;
                }
            } catch (const std::bad_any_cast& e) {
                std::cout << "   ⚠️  Unknown type for input " << i << std::endl;
            }
        }

        aggregated_result["total_records"] = total_records;
        aggregated_result["api_items"] = all_items;
        aggregated_result["processed_files"] = processed_files;
        aggregated_result["aggregation_timestamp"] =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();

        std::cout << "✓ Aggregation complete: " << total_records
                  << " DB records, " << all_items.size() << " API items, "
                  << processed_files.size() << " files" << std::endl;

        return aggregated_result;
    }
};

// ML Pipeline task that processes aggregated data
class MLPipelineTask : public Task {
   private:
    std::string algorithm_;

   public:
    MLPipelineTask(const std::string& algorithm)
        : Task(typeid(std::map<std::string, std::any>),
               typeid(std::map<std::string, double>)),
          algorithm_(algorithm) {}

    std::any execute(std::any& input) override {
        auto data = std::any_cast<std::map<std::string, std::any>>(input);

        // Simulate complex ML processing
        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        std::map<std::string, double> ml_results;

        try {
            int records = std::any_cast<int>(data["total_records"]);
            auto items =
                std::any_cast<std::vector<std::string>>(data["api_items"]);
            auto files = std::any_cast<std::vector<std::string>>(
                data["processed_files"]);

            ml_results["data_quality_score"] =
                std::min(100.0, (records * 0.01) + (items.size() * 2.5));
            ml_results["complexity_factor"] =
                files.size() * 1.8 + items.size() * 0.3;
            ml_results["confidence"] = 85.5 + (records > 5000 ? 10.0 : 0.0);
            ml_results["processing_efficiency"] = 92.3;

            std::cout << "🤖 ML " << algorithm_ << " Results: Quality="
                      << ml_results["data_quality_score"]
                      << "%, Confidence=" << ml_results["confidence"] << "%"
                      << std::endl;

        } catch (const std::bad_any_cast& e) {
            std::cout << "❌ ML Pipeline failed: Invalid input data types"
                      << std::endl;
            ml_results["error"] = 1.0;
        }

        return ml_results;
    }
};

// Final report generator with multiple ML inputs
class ReportGeneratorTask : public Task {
   public:
    ReportGeneratorTask()
        : Task(typeid(std::vector<std::any>), typeid(std::string)) {}

    std::any execute(std::any& input) override {
        auto inputs = std::any_cast<std::vector<std::any>>(input);
        std::cout << "📋 Generating final report from " << inputs.size()
                  << " ML models:" << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(75));

        std::ostringstream report;
        report << "=== COMPLEX MULTI-DEPENDENCY ANALYSIS REPORT ===\n";

        double avg_quality = 0.0, avg_confidence = 0.0;
        int model_count = 0;

        for (size_t i = 0; i < inputs.size(); ++i) {
            try {
                auto ml_result =
                    std::any_cast<std::map<std::string, double>>(inputs[i]);

                if (ml_result.find("error") == ml_result.end()) {
                    report << "Model " << (i + 1)
                           << ": Quality=" << ml_result["data_quality_score"]
                           << "%, Confidence=" << ml_result["confidence"]
                           << "%\n";
                    avg_quality += ml_result["data_quality_score"];
                    avg_confidence += ml_result["confidence"];
                    model_count++;
                    std::cout << "   📊 Model " << (i + 1)
                              << " included in report" << std::endl;
                }
            } catch (const std::bad_any_cast& e) {
                std::cout << "   ⚠️  Invalid ML result " << i << std::endl;
            }
        }

        if (model_count > 0) {
            avg_quality /= model_count;
            avg_confidence /= model_count;
            report << "ENSEMBLE AVERAGE: Quality=" << avg_quality
                   << "%, Confidence=" << avg_confidence << "%\n";
        }

        report << "Report generated with " << model_count << " models\n";
        report << "=== END REPORT ===";

        std::cout << "✓ Report generated with " << model_count << " models"
                  << std::endl;
        return report.str();
    }
};

int main() {
    std::cout << "=== COMPLEX MULTI-DEPENDENCY PIPELINE EXAMPLE ==="
              << std::endl;
    std::cout << "Scenario: Multi-source data processing with dynamic tasks "
                 "and ML ensemble"
              << std::endl;
    std::cout << std::endl;

    Pipeline pipeline;

    // LAYER 1: Data Sources (Independent - can run in parallel)
    pipeline.add_task(std::make_unique<DatabaseQueryTask>(
        "SELECT * FROM users", 3));   // Task 0 - Complex query
    pipeline.add_task(std::make_unique<DatabaseQueryTask>(
        "SELECT * FROM orders", 2));  // Task 1 - Medium query
    pipeline.add_task(std::make_unique<APICallTask>(
        "user_profiles", true));      // Task 2 - With dynamic validation
    pipeline.add_task(std::make_unique<APICallTask>(
        "payment_methods", false));   // Task 3 - Simple API
    pipeline.add_task(std::make_unique<FileProcessorTask>(
        "CSV_logs"));                 // Task 4 - File processing
    pipeline.add_task(std::make_unique<FileProcessorTask>(
        "JSON_config"));              // Task 5 - File processing

    // LAYER 2: Data Aggregation (Depends on data sources)
    pipeline.add_task(
        std::make_unique<DataAggregatorTask>());  // Task 6 - Aggregates all
                                                  // sources

    // LAYER 3: ML Processing (Multiple models on aggregated data)
    pipeline.add_task(std::make_unique<MLPipelineTask>(
        "RandomForest"));      // Task 7 - ML Model 1
    pipeline.add_task(std::make_unique<MLPipelineTask>(
        "NeuralNetwork"));     // Task 8 - ML Model 2
    pipeline.add_task(std::make_unique<MLPipelineTask>(
        "GradientBoosting"));  // Task 9 - ML Model 3

    // LAYER 4: Final Report (Depends on all ML models)
    pipeline.add_task(
        std::make_unique<ReportGeneratorTask>());  // Task 10 - Final report

    // COMPLEX DEPENDENCY STRUCTURE:

    // Layer 1 → Layer 2: All data sources feed into aggregator
    pipeline.add_dependency(0, 6);  // DB users → Aggregator
    pipeline.add_dependency(1, 6);  // DB orders → Aggregator
    pipeline.add_dependency(2, 6);  // API profiles → Aggregator
    pipeline.add_dependency(3, 6);  // API payments → Aggregator
    pipeline.add_dependency(4, 6);  // CSV logs → Aggregator
    pipeline.add_dependency(5, 6);  // JSON config → Aggregator

    // Layer 2 → Layer 3: Aggregated data feeds all ML models (fan-out)
    pipeline.add_dependency(6, 7);  // Aggregator → RandomForest
    pipeline.add_dependency(6, 8);  // Aggregator → NeuralNetwork
    pipeline.add_dependency(6, 9);  // Aggregator → GradientBoosting

    // Layer 3 → Layer 4: All ML models feed into final report (fan-in)
    pipeline.add_dependency(7, 10);  // RandomForest → Report
    pipeline.add_dependency(8, 10);  // NeuralNetwork → Report
    pipeline.add_dependency(9, 10);  // GradientBoosting → Report

    std::cout << "Complex Pipeline Structure (4 layers, 11 tasks):"
              << std::endl;
    std::cout << "Layer 1: [DB1] [DB2] [API1*] [API2] [FILE1] [FILE2] (6 "
                 "parallel sources)"
              << std::endl;
    std::cout << "   │      │      │       │       │        │" << std::endl;
    std::cout << "   └──────┴──────┴───────┴───────┴────────┴─► [AGGREGATOR]"
              << std::endl;
    std::cout << "                                                   │"
              << std::endl;
    std::cout << "                           "
                 "┌───────────────────────┼───────────────────────┐"
              << std::endl;
    std::cout << "                           ▼                       ▼         "
                 "              ▼"
              << std::endl;
    std::cout << "Layer 3:              [ML_RF]               [ML_NN]          "
                 "     [ML_GB]"
              << std::endl;
    std::cout << "                           │                       │         "
                 "              │"
              << std::endl;
    std::cout << "                           "
                 "└───────────────────────┼───────────────────────┘"
              << std::endl;
    std::cout << "                                                   ▼"
              << std::endl;
    std::cout << "Layer 4:                                      [REPORT]"
              << std::endl;
    std::cout << "(*API1 emits dynamic validation tasks)" << std::endl;
    std::cout << std::endl;

    // Test with both executors
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
                executor = ExecutorFactory::create_thread(
                    8);  // More threads for complex pipeline
            }

            std::any result = executor->execute(pipeline, 42);

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);

            std::string final_report = std::any_cast<std::string>(result);
            std::cout << "\n" << final_report << std::endl;
            std::cout << "⏱️  Total execution time: " << duration.count() << "ms"
                      << std::endl;

            if (exec_type == "sequential") {
                std::cout << "   (Sequential: All tasks executed one by one)"
                          << std::endl;
            } else {
                std::cout << "   (Parallel: Layer 1 tasks run concurrently, "
                             "massive speedup expected)"
                          << std::endl;
            }

        } catch (const std::exception& e) {
            std::cout << "❌ Error: " << e.what() << std::endl;
        }

        std::cout << std::endl;
    }

    std::cout << "=== COMPLEX MULTI-DEPENDENCY INSIGHTS ===" << std::endl;
    std::cout
        << "1. 🏗️  4-Layer Architecture: Sources → Aggregation → ML → Reporting"
        << std::endl;
    std::cout << "2. 🔀 Fan-Out/Fan-In: 6→1→3→1 dependency pattern"
              << std::endl;
    std::cout << "3. ⚡ Layer-Level Parallelism: All tasks in same layer run "
                 "concurrently"
              << std::endl;
    std::cout
        << "4. 🎯 Dynamic Task Emission: API tasks can emit validation subtasks"
        << std::endl;
    std::cout << "5. 🧠 Type-Safe Multi-Dependencies: Different input types "
                 "properly aggregated"
              << std::endl;
    std::cout << "6. 🔒 Dependency Coordination: Complex waiting/signaling "
                 "with atomic counters"
              << std::endl;
    std::cout << "7. 🚀 Massive Parallelization: ThreadScheduler maximizes "
                 "concurrent execution"
              << std::endl;

    return 0;
}