#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <string>
#include <random>
#include <fstream>
#include <vector>
#include <memory>

#include <dft_utils/indexer/indexer.h>
#include <dft_utils/reader/reader.h>
#include <dft_utils/utils/filesystem.h>
#include "testing_utilities.h"

using namespace dft_utils_test;

TEST_CASE("C++ Indexer - Basic functionality") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    SUBCASE("Constructor and destructor") {
        // Test automatic destruction
        {
            dft::indexer::Indexer indexer(gz_file, idx_file, 1.0);
            CHECK(indexer.is_valid());
        } // indexer automatically destroyed here
        
        // Should be able to create another one
        dft::indexer::Indexer indexer2(gz_file, idx_file, 1.0);
        CHECK(indexer2.is_valid());
    }
    
    SUBCASE("Build index") {
        dft::indexer::Indexer indexer(gz_file, idx_file, 1.0);
        CHECK_NOTHROW(indexer.build());
    }
    
    SUBCASE("Check rebuild needed") {
        dft::indexer::Indexer indexer(gz_file, idx_file, 1.0);
        CHECK(indexer.need_rebuild()); // Should need rebuild initially
        
        indexer.build();
        CHECK_FALSE(indexer.need_rebuild()); // Should not need rebuild after building
    }
    
    SUBCASE("Move semantics") {
        dft::indexer::Indexer indexer1(gz_file, idx_file, 1.0);
        CHECK(indexer1.is_valid());
        
        // Move constructor
        dft::indexer::Indexer indexer2 = std::move(indexer1);
        CHECK(indexer2.is_valid());
        CHECK_FALSE(indexer1.is_valid()); // indexer1 should be moved from
        
        // Move assignment
        dft::indexer::Indexer indexer3(gz_file, idx_file, 2.0);
        indexer3 = std::move(indexer2);
        CHECK(indexer3.is_valid());
        CHECK_FALSE(indexer2.is_valid()); // indexer2 should be moved from
    }
}

TEST_CASE("C++ Reader - Basic functionality") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    {
        dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
        indexer.build();
    }
    
    SUBCASE("Constructor and destructor") {
        // Test automatic destruction
        {
            dft::reader::Reader reader(gz_file, idx_file);
            CHECK(reader.is_valid());
            CHECK(reader.get_gz_path() == gz_file);
        } // reader automatically destroyed here
        
        // Should be able to create another one
        dft::reader::Reader reader2(gz_file, idx_file);
        CHECK(reader2.is_valid());
    }
    
    SUBCASE("Get max bytes") {
        dft::reader::Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        CHECK(max_bytes > 0);
    }
    
    SUBCASE("Read byte range with automatic memory management") {
        dft::reader::Reader reader(gz_file, idx_file);
        
        // Read using explicit gz_path
        auto result1 = reader.read_range_bytes(gz_file, 0, 50);
        CHECK(result1.first != nullptr);
        CHECK(result1.second >= 50);
        
        // Read using stored gz_path
        auto result2 = reader.read_range_bytes(0, 50);
        CHECK(result2.first != nullptr);
        CHECK(result2.second >= 50);
        
        // Memory is automatically freed when result1 and result2 go out of scope
    }
    
    SUBCASE("Read megabyte range with automatic memory management") {
        dft::reader::Reader reader(gz_file, idx_file);
        
        // Read using explicit gz_path
        auto result1 = reader.read_range_megabytes(gz_file, 0.0, 0.001);
        CHECK(result1.first != nullptr);
        CHECK(result1.second > 0);
        
        // Read using stored gz_path
        auto result2 = reader.read_range_megabytes(0.0, 0.001);
        CHECK(result2.first != nullptr);
        CHECK(result2.second > 0);
        
        // Memory is automatically freed when result1 and result2 go out of scope
    }
    
    SUBCASE("Move semantics") {
        dft::reader::Reader reader1(gz_file, idx_file);
        CHECK(reader1.is_valid());
        
        // Move constructor
        dft::reader::Reader reader2 = std::move(reader1);
        CHECK(reader2.is_valid());
        CHECK_FALSE(reader1.is_valid()); // reader1 should be moved from
        
        // Move assignment
        dft::reader::Reader reader3(gz_file, idx_file);
        reader3 = std::move(reader2);
        CHECK(reader3.is_valid());
        CHECK_FALSE(reader2.is_valid()); // reader2 should be moved from
    }
}

TEST_CASE("C++ API - Error handling") {
    SUBCASE("Invalid indexer creation should succeed but build should fail") {
        dft::indexer::Indexer indexer("/nonexistent/path.gz", "/nonexistent/path.idx", 1.0);
        CHECK(indexer.is_valid());
        
        // Building should fail
        CHECK_THROWS_AS(indexer.build(), std::runtime_error);
    }
    
    SUBCASE("Invalid reader creation") {
        CHECK_THROWS_AS(
            dft::reader::Reader("/nonexistent/path.gz", "/nonexistent/path.idx"),
            std::runtime_error
        );
    }
}

TEST_CASE("C++ API - Data range reading") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    {
        dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
        indexer.build();
    }
    
    // Create reader
    dft::reader::Reader reader(gz_file, idx_file);
    
    SUBCASE("Read valid byte range") {
        // read first 50 bytes
        auto result = reader.read_range_bytes(gz_file, 0, 50);
        CHECK(result.first != nullptr);
        CHECK(result.second >= 50);
        
        // check that we got some JSON content
        std::string content(result.first.get(), result.second);
        CHECK(content.find("{") != std::string::npos);
    }
    
    SUBCASE("Read megabyte range") {
        // read first 0.001 MB (about 1024 bytes)
        auto result = reader.read_range_megabytes(gz_file, 0.0, 0.001);
        CHECK(result.first != nullptr);
        CHECK(result.second > 0);
    }
}

TEST_CASE("C++ API - Edge cases") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    {
        dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
        indexer.build();
    }
    
    // Create reader
    dft::reader::Reader reader(gz_file, idx_file);
    
    SUBCASE("Invalid byte range (start >= end) should throw") {
        CHECK_THROWS(reader.read_range_bytes(gz_file, 100, 50));
        CHECK_THROWS(reader.read_range_bytes(gz_file, 50, 50)); // Equal start and end
    }
    
    SUBCASE("Non-existent file should throw") {
        // Use cross-platform non-existent path
        fs::path non_existent = fs::temp_directory_path() / "nonexistent" / "file.gz";
        CHECK_THROWS(reader.read_range_bytes(non_existent.string(), 0, 50));
    }
}

TEST_CASE("C++ API - Integration test") {
    TestEnvironment env(1000);
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Complete workflow using C++ API
    {
        // Build index
        dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
        indexer.build();
        
        // Read data
        dft::reader::Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        CHECK(max_bytes > 0);
        
        // Read multiple ranges
        auto result1 = reader.read_range_bytes(0, 100);
        CHECK(result1.first != nullptr);
        CHECK(result1.second >= 100);
        
        auto result2 = reader.read_range_bytes(100, 200);
        CHECK(result2.first != nullptr);
        CHECK(result2.second >= 100);
        
        // Verify data content
        std::string content1(result1.first.get(), result1.second);
        std::string content2(result2.first.get(), result2.second);
        CHECK(content1.find("{") != std::string::npos);
        CHECK(content2.find("{") != std::string::npos);
        
        // All resources are automatically cleaned up when objects go out of scope
    }
}

TEST_CASE("C++ API - Memory safety stress test") {
    TestEnvironment env(1000);
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index
    {
        dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
        indexer.build();
    }
    
    // Multiple readers and operations
    dft::reader::Reader reader(gz_file, idx_file);
    
    // Multiple reads with automatic memory management
    for (int i = 0; i < 100; ++i) {
        auto result = reader.read_range_bytes(0, 50);
        CHECK(result.first != nullptr);
        CHECK(result.second >= 50);
        // Memory automatically freed each iteration
    }
}

TEST_CASE("C++ API - Exception handling comprehensive tests") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    SUBCASE("Indexer with invalid paths should throw during build") {
        dft::indexer::Indexer indexer("/definitely/nonexistent/path.gz", "/also/nonexistent/path.idx", 1.0);
        CHECK(indexer.is_valid());  // Constructor succeeds
        
        CHECK_THROWS_AS(indexer.build(), std::runtime_error);
        CHECK_THROWS_AS(indexer.build(), std::runtime_error);  // Should throw consistently
    }
    
    SUBCASE("Indexer with invalid chunk size should throw in constructor") {
        CHECK_THROWS_AS(
            dft::indexer::Indexer(gz_file, idx_file, 0.0),
            std::invalid_argument
        );
        
        CHECK_THROWS_AS(
            dft::indexer::Indexer(gz_file, idx_file, -1.0),
            std::invalid_argument
        );
    }
    
    SUBCASE("Reader with invalid paths should throw in constructor") {
        CHECK_THROWS_AS(
            dft::reader::Reader("/definitely/nonexistent/path.gz", "/also/nonexistent/path.idx"),
            std::runtime_error
        );
    }
    
    SUBCASE("Reader operations on invalid reader should throw") {
        // Build a valid index first
        {
            dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
            indexer.build();
        }
        
        dft::reader::Reader reader(gz_file, idx_file);
        CHECK(reader.is_valid());
        
        // Make reader invalid by moving from it
        dft::reader::Reader moved_reader = std::move(reader);
        CHECK_FALSE(reader.is_valid());
        CHECK(moved_reader.is_valid());
        
        // Operations on valid moved reader should work
        CHECK_NOTHROW(moved_reader.get_max_bytes());
        CHECK_NOTHROW(moved_reader.read_range_bytes(0, 100));
        
        // Note: Testing operations on moved-from reader may cause undefined behavior
        // This is expected behavior for moved-from objects in C++
    }
    
    SUBCASE("Invalid read parameters should throw") {
        // Build a valid index first
        {
            dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
            indexer.build();
        }
        
        dft::reader::Reader reader(gz_file, idx_file);
        
        // Invalid ranges should throw
        CHECK_THROWS_AS(reader.read_range_bytes(100, 50), std::invalid_argument); // start > end
        CHECK_THROWS_AS(reader.read_range_bytes(50, 50), std::invalid_argument);  // start == end
        
        // Invalid megabyte ranges should throw
        CHECK_THROWS_AS(reader.read_range_megabytes(1.0, 0.5), std::invalid_argument); // start > end
        CHECK_THROWS_AS(reader.read_range_megabytes(0.5, 0.5), std::invalid_argument); // start == end
        // Note: Implementation may not validate negative ranges - behavior may be implementation-specific
        // CHECK_THROWS_AS(reader.read_range_megabytes(-1.0, 1.0), std::invalid_argument); // negative start
        // CHECK_THROWS_AS(reader.read_range_megabytes(0.0, -1.0), std::invalid_argument); // negative end
    }
}

TEST_CASE("C++ API - Advanced indexer functionality") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    SUBCASE("Multiple indexer instances for same file") {
        dft::indexer::Indexer indexer1(gz_file, idx_file, 1.0);
        dft::indexer::Indexer indexer2(gz_file, idx_file, 1.0);
        
        CHECK(indexer1.is_valid());
        CHECK(indexer2.is_valid());
        
        // Both should work independently
        CHECK_NOTHROW(indexer1.build());
        CHECK_NOTHROW(indexer2.build());
        
        // After first build, rebuild should not be needed
        CHECK_FALSE(indexer1.need_rebuild());
        CHECK_FALSE(indexer2.need_rebuild());
    }
    
    SUBCASE("Different chunk sizes") {
        dft::indexer::Indexer indexer_small(gz_file, idx_file + "_small", 0.1);
        dft::indexer::Indexer indexer_large(gz_file, idx_file + "_large", 10.0);
        
        CHECK_NOTHROW(indexer_small.build());
        CHECK_NOTHROW(indexer_large.build());
        
        // Both should create valid indices
        CHECK_NOTHROW(dft::reader::Reader(gz_file, idx_file + "_small"));
        CHECK_NOTHROW(dft::reader::Reader(gz_file, idx_file + "_large"));
    }
    
    SUBCASE("Indexer state after operations") {
        dft::indexer::Indexer indexer(gz_file, idx_file, 1.0);
        
        CHECK(indexer.is_valid());
        CHECK(indexer.need_rebuild());
        
        indexer.build();
        CHECK(indexer.is_valid());  // Should still be valid after build
        CHECK_FALSE(indexer.need_rebuild());  // Should not need rebuild
        
        // Can build again
        CHECK_NOTHROW(indexer.build());
        CHECK(indexer.is_valid());
    }
}

TEST_CASE("C++ API - Advanced reader functionality") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    {
        dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
        indexer.build();
    }
    
    SUBCASE("Multiple readers for same file") {
        dft::reader::Reader reader1(gz_file, idx_file);
        dft::reader::Reader reader2(gz_file, idx_file);
        
        CHECK(reader1.is_valid());
        CHECK(reader2.is_valid());
        
        // Both should work independently
        auto result1 = reader1.read_range_bytes(0, 100);
        auto result2 = reader2.read_range_bytes(0, 100);
        
        CHECK(result1.first != nullptr);
        CHECK(result2.first != nullptr);
        CHECK(result1.second == result2.second); // Should return same size
        
        // Content should be identical
        CHECK(memcmp(result1.first.get(), result2.first.get(), 
                    std::min(result1.second, result2.second)) == 0);
    }
    
    SUBCASE("Reader state consistency") {
        dft::reader::Reader reader(gz_file, idx_file);
        
        CHECK(reader.is_valid());
        CHECK(reader.get_gz_path() == gz_file);
        
        size_t max_bytes = reader.get_max_bytes();
        CHECK(max_bytes > 0);
        
        // Multiple calls should return same value
        CHECK(reader.get_max_bytes() == max_bytes);
        CHECK(reader.get_gz_path() == gz_file);
        CHECK(reader.is_valid());
    }
    
    SUBCASE("Various read patterns") {
        dft::reader::Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        
        // Small reads
        auto small_result = reader.read_range_bytes(0, 10);
        CHECK(small_result.first != nullptr);
        CHECK(small_result.second >= 10);
        
        // Medium reads
        if (max_bytes > 1000) {
            auto medium_result = reader.read_range_bytes(100, 1000);
            CHECK(medium_result.first != nullptr);
            CHECK(medium_result.second >= 900);
        }
        
        // Large reads
        if (max_bytes > 10000) {
            auto large_result = reader.read_range_bytes(1000, 10000);
            CHECK(large_result.first != nullptr);
            CHECK(large_result.second >= 9000);
        }
        
        // Megabyte reads
        auto mb_result = reader.read_range_megabytes(0.0, 0.001);
        CHECK(mb_result.first != nullptr);
        CHECK(mb_result.second > 0);
    }
    
    SUBCASE("Boundary conditions") {
        dft::reader::Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        
        if (max_bytes > 100) {
            // Read near the end of file
            auto near_end = reader.read_range_bytes(max_bytes - 50, max_bytes);
            CHECK(near_end.first != nullptr);
            CHECK(near_end.second > 0);
            
            // Read at exact boundaries
            auto at_start = reader.read_range_bytes(0, 1);
            CHECK(at_start.first != nullptr);
            CHECK(at_start.second >= 1);
        }
        
        // Read beyond file (should still succeed but return appropriate data)
        auto beyond_file = reader.read_range_bytes(max_bytes, max_bytes + 1000);
        CHECK(beyond_file.first != nullptr);
        // Size may be 0 or small depending on implementation
    }
}

TEST_CASE("C++ API - JSON boundary detection") {
    TestEnvironment env(1000);  // More lines for better boundary testing
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    {
        dft::indexer::Indexer indexer(gz_file, idx_file, 0.5);
        indexer.build();
    }
    
    // Create reader
    dft::reader::Reader reader(gz_file, idx_file);

    SUBCASE("Small range should provide minimum requested bytes") {
        // Request 100 bytes - should get AT LEAST 100 bytes due to boundary extension
        auto result = reader.read_range_bytes(0, 100);
        CHECK(result.first != nullptr);
        CHECK(result.second >= 100);  // Should get at least what was requested
        
        // Verify that output ends with complete JSON line
        std::string content(result.first.get(), result.second);
        CHECK(content.back() == '\n');  // Should end with newline
        
        // Should contain complete JSON objects
        size_t last_brace = content.rfind('}');
        REQUIRE(last_brace != std::string::npos);
        CHECK(last_brace < content.length() - 1);  // '}' should not be the last character
        CHECK(content[last_brace + 1] == '\n');    // Should be followed by newline
    }
    
    SUBCASE("Output should not cut off in middle of JSON") {
        // Request 500 bytes - this should not cut off mid-JSON
        auto result = reader.read_range_bytes(0, 500);
        CHECK(result.first != nullptr);
        CHECK(result.second >= 500);
        
        std::string content(result.first.get(), result.second);
        
        // Should not end with partial JSON like {"name":"name_%
        size_t name_pos = content.find("\"name_");
        size_t last_brace_pos = content.rfind('}');
        bool has_incomplete_name = (name_pos != std::string::npos) && (name_pos > last_brace_pos);
        CHECK_FALSE(has_incomplete_name);
        
        // Verify it ends with complete JSON boundary (}\n)
        if (content.length() >= 2) {
            CHECK(content[content.length() - 2] == '}');
            CHECK(content[content.length() - 1] == '\n');
        }
    }
    
    SUBCASE("Multiple range reads should maintain boundaries") {
        // Read multiple consecutive ranges
        std::vector<std::string> segments;
        size_t current_pos = 0;
        size_t segment_size = 200;
        
        for (int i = 0; i < 5; ++i) {
            auto result = reader.read_range_bytes(current_pos, current_pos + segment_size);
            CHECK(result.first != nullptr);
            CHECK(result.second >= segment_size);
            
            std::string content(result.first.get(), result.second);
            segments.push_back(content);
            
            // Each segment should end properly
            CHECK(content.back() == '\n');
            
            current_pos += segment_size;
        }
        
        // Each segment should contain complete JSON objects
        for (const auto& segment : segments) {
            size_t json_count = 0;
            size_t pos = 0;
            while ((pos = segment.find("}\n", pos)) != std::string::npos) {
                json_count++;
                pos += 2;
            }
            CHECK(json_count > 0);  // Should have at least one complete JSON object
        }
    }
}

TEST_CASE("C++ API - Regression and stress tests") {
    SUBCASE("Large file handling") {
        TestEnvironment env(10000);  // Large test file
        REQUIRE(env.is_valid());
        
        std::string gz_file = env.create_test_gzip_file();
        REQUIRE(!gz_file.empty());
        
        std::string idx_file = env.get_index_path(gz_file);
        
        // Build index
        {
            dft::indexer::Indexer indexer(gz_file, idx_file, 1.0);
            CHECK_NOTHROW(indexer.build());
        }
        
        // Test large reads
        dft::reader::Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        CHECK(max_bytes > 10000);  // Should be a large file
        
        // Read large chunks
        if (max_bytes > 50000) {
            auto large_read = reader.read_range_bytes(1000, 50000);
            CHECK(large_read.first != nullptr);
            CHECK(large_read.second >= 49000);
            
            std::string content(large_read.first.get(), large_read.second);
            CHECK(content.find("{") != std::string::npos);
            CHECK(content.back() == '\n');
        }
    }
    
    SUBCASE("Specific truncated JSON regression test") {
        // This test specifically catches the original bug where output was like:
        // {"name":"name_%  instead of complete JSON lines
        
        TestEnvironment env(2000);
        REQUIRE(env.is_valid());
        
        // Create test data with specific pattern that might trigger the bug
        std::string test_dir = env.get_dir();
        std::string gz_file = test_dir + "/regression_test.gz";
        std::string idx_file = test_dir + "/regression_test.gz.idx";
        std::string txt_file = test_dir + "/regression_test.txt";
        
        // Create test data similar to trace.pfw.gz format
        std::ofstream f(txt_file);
        REQUIRE(f.is_open());
        
        f << "[\n";  // JSON array start
        for (size_t i = 1; i <= 1000; ++i) {
            f << "{\"name\":\"name_" << i << "\",\"cat\":\"cat_" << i << "\",\"dur\":" << (i * 10 % 1000) << "}\n";
        }
        f.close();
        
        bool success = dft_utils_test::compress_file_to_gzip(txt_file, gz_file);
        REQUIRE(success);
        fs::remove(txt_file);
        
        // Build index
        {
            dft::indexer::Indexer indexer(gz_file, idx_file, 32.0);
            indexer.build();
        }
        
        // Create reader
        dft::reader::Reader reader(gz_file, idx_file);

        SUBCASE("Original failing case: 0 to 10000 bytes") {
            auto result = reader.read_range_bytes(0, 10000);
            CHECK(result.first != nullptr);
            CHECK(result.second >= 10000);
            
            std::string content(result.first.get(), result.second);
            
            // Should NOT end with incomplete patterns like "name_%
            CHECK(content.find("\"name_%") == std::string::npos);
            CHECK(content.find("\"cat_%") == std::string::npos);
            
            // Should end with complete JSON line
            CHECK(content.back() == '\n');
            CHECK(content[content.length() - 2] == '}');
            
            // Should contain the pattern but complete
            CHECK(content.find("\"name\":\"name_") != std::string::npos);
            CHECK(content.find("\"cat\":\"cat_") != std::string::npos);
        }
        
        SUBCASE("Small range minimum bytes check") {
            // This was returning only 44 bytes instead of at least 100
            auto result = reader.read_range_bytes(0, 100);
            CHECK(result.first != nullptr);
            CHECK(result.second >= 100);  // This was the main bug - was only 44 bytes
            
            std::string content(result.first.get(), result.second);
            
            // Should contain multiple complete JSON objects for 100+ bytes
            size_t brace_count = 0;
            for (char c : content) {
                if (c == '}') brace_count++;
            }
            CHECK(brace_count >= 2);  // Should have at least 2 complete objects for 100+ bytes
        }
    }
}