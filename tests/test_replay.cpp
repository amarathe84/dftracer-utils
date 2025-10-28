#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <dftracer/utils/replay/replay.h>
#include <dftracer/utils/common/logging.h>
#include <doctest/doctest.h>

#include <fstream>
#include <chrono>
#include <string>
#include <vector>
#include <filesystem>

#include "testing_utilities.h"

using namespace dftracer::utils::replay;
using namespace dftracer::utils;

TEST_CASE("DFTracer Replay - Basic functionality") {
    DFTRACER_UTILS_LOGGER_INIT();
    
    // Create a temporary trace file with sample data
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dftracer_replay_test";
    std::filesystem::create_directories(temp_dir);
    
    std::string trace_file = (temp_dir / "test_trace.pfw").string();
    
    SUBCASE("Create sample trace file") {
        std::ofstream file(trace_file);
        REQUIRE(file.is_open());
        
        // Write sample trace entries based on the bert trace format
        file << "[\n";
        file << R"({"id":1,"name":"opendir","cat":"POSIX","pid":12345,"tid":12345,"ts":1000000,"dur":1500,"ph":"X","args":{"fhash":"abc123","level":1}})" << "\n";
        file << R"({"id":2,"name":"read","cat":"POSIX","pid":12345,"tid":12345,"ts":1002000,"dur":2500,"ph":"X","args":{"fhash":"def456","size":1024,"level":1}})" << "\n";
        file << R"({"id":3,"name":"write","cat":"POSIX","pid":12345,"tid":12345,"ts":1005000,"dur":3000,"ph":"X","args":{"fhash":"ghi789","size":2048,"level":1}})" << "\n";
        file << R"({"id":4,"name":"fopen","cat":"STDIO","pid":12345,"tid":12345,"ts":1009000,"dur":500,"ph":"X","args":{"fhash":"jkl012","level":1}})" << "\n";
        file << "]";
        file.close();
        
        REQUIRE(std::filesystem::exists(trace_file));
    }
    
    SUBCASE("Test DFTracer sleep-based replay mode") {
        ReplayConfig config;
        config.dftracer_mode = true;
        config.maintain_timing = false;  // Don't maintain timing for faster test
        config.verbose = true;
        
        ReplayEngine engine(config);
        
        auto start_time = std::chrono::steady_clock::now();
        ReplayResult result = engine.replay(trace_file);
        auto end_time = std::chrono::steady_clock::now();
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        CHECK(result.total_events > 0);
        CHECK(result.executed_events > 0);
        CHECK(result.failed_events == 0);
        
        // In dftracer mode, all events should be handled by DFTracerExecutor
        CHECK(result.executed_events == result.total_events);
        
        std::cout << "Processed " << result.total_events << " events in " 
                  << duration.count() << " microseconds" << std::endl;
    }
    
    SUBCASE("Test normal replay mode vs DFTracer mode") {
        // Test normal mode
        ReplayConfig normal_config;
        normal_config.dftracer_mode = false;
        normal_config.maintain_timing = false;
        normal_config.dry_run = true;  // Use dry run to avoid actual I/O
        
        ReplayEngine normal_engine(normal_config);
        ReplayResult normal_result = normal_engine.replay(trace_file);
        
        // Test DFTracer mode
        ReplayConfig dftracer_config;
        dftracer_config.dftracer_mode = true;
        dftracer_config.maintain_timing = false;
        
        ReplayEngine dftracer_engine(dftracer_config);
        ReplayResult dftracer_result = dftracer_engine.replay(trace_file);
        
        // Both should process the same number of events
        CHECK(normal_result.total_events == dftracer_result.total_events);
        
        // DFTracer mode should execute all events, normal mode might skip some
        CHECK(dftracer_result.executed_events >= normal_result.executed_events);
        
        std::cout << "Normal mode: " << normal_result.executed_events << "/" << normal_result.total_events << " executed" << std::endl;
        std::cout << "DFTracer mode: " << dftracer_result.executed_events << "/" << dftracer_result.total_events << " executed" << std::endl;
    }
    
    SUBCASE("Test timing simulation") {
        ReplayConfig config;
        config.dftracer_mode = true;
        config.maintain_timing = false;  // Test without timing for consistent results
        
        ReplayEngine engine(config);
        
        // Measure execution time
        auto start = std::chrono::steady_clock::now();
        ReplayResult result = engine.replay(trace_file);
        auto end = std::chrono::steady_clock::now();
        
        auto execution_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // The execution should have taken some time due to sleep calls
        // But without maintain_timing, it should be relatively fast
        CHECK(execution_time.count() > 0);
        CHECK(result.total_events > 0);
        CHECK(result.executed_events == result.total_events);
        
        std::cout << "Execution time: " << execution_time.count() << " microseconds" << std::endl;
    }
    
    // Cleanup
    std::filesystem::remove_all(temp_dir);
}

TEST_CASE("DFTracer Replay - Real trace file") {
    DFTRACER_UTILS_LOGGER_INIT();
    
    std::string trace_file = "/g/g92/marathe1/myworkspace/dldl/dftracer-utils/trace/bert_v100-1.pfw";
    
    SUBCASE("Test with actual bert trace file") {
        if (!std::filesystem::exists(trace_file)) {
            MESSAGE("Skipping real trace test - file not found: ", trace_file);
            return;
        }
        
        ReplayConfig config;
        config.dftracer_mode = true;
        config.maintain_timing = false;  // Don't maintain timing for faster test
        config.verbose = false;  // Reduce verbosity for large trace
        
        // Add filters to process only a subset for testing
        config.filter_categories.insert("POSIX");
        
        ReplayEngine engine(config);
        
        auto start_time = std::chrono::steady_clock::now();
        ReplayResult result = engine.replay(trace_file);
        auto end_time = std::chrono::steady_clock::now();
        
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        CHECK(result.total_events > 0);
        std::cout << "Processed " << result.total_events << " events from bert trace in " 
                  << duration.count() << " milliseconds" << std::endl;
        std::cout << "Executed: " << result.executed_events << ", Failed: " << result.failed_events << std::endl;
        
        // Print function statistics
        if (!result.function_counts.empty()) {
            std::cout << "Function counts:" << std::endl;
            for (const auto& [func, count] : result.function_counts) {
                std::cout << "  " << func << ": " << count << std::endl;
            }
        }
    }
}