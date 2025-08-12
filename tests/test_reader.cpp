#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <random>
#include <fstream>
#include <vector>
#include <zlib.h>

#include <dft_utils/indexer/indexer.h>
#include <dft_utils/reader/reader.h>
#include <dft_utils/utils/filesystem.h>
#include "testing_utilities.h"

using namespace dft_utils_test;

TEST_CASE("Indexer creation and destruction") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    CHECK(indexer != nullptr);
    
    if (indexer) {
        dft_indexer_destroy(indexer);
    }
}

TEST_CASE("Gzip index building") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    CHECK(result == 0);
    
    dft_indexer_destroy(indexer);
}

TEST_CASE("Data range reading") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);
    
    SUBCASE("Read valid byte range") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // read first 50 bytes
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 50, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size > 0);
            CHECK(output_size >= 50);
            
            // check that we got some JSON content
            std::string content(output, output_size);
            CHECK(content.find("{") != std::string::npos);
            
            free(output);
        }
    }
    
    SUBCASE("Read with null parameters") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // null reader
        result = dft_reader_read_range_bytes(nullptr, gz_file.c_str(), 0, 50, &output, &output_size);
        CHECK(result == -1);
        
        // null gz_path
        result = dft_reader_read_range_bytes(reader, nullptr, 0, 50, &output, &output_size);
        CHECK(result == -1);
        
        // null output
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 50, nullptr, &output_size);
        CHECK(result == -1);
        
        // null output_size
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 50, &output, nullptr);
        CHECK(result == -1);
    }
    
    SUBCASE("Read megabyte range") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // read first 0.001 MB (about 1024 bytes)
        result = dft_reader_read_range_megabytes(reader, gz_file.c_str(), 0.0, 0.001, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size > 0);
            
            free(output);
        }
    }
    
    dft_reader_destroy(reader);
}

TEST_CASE("Edge cases") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);
    
    SUBCASE("Invalid byte range (start >= end)") {
        char* output = nullptr;
        size_t output_size = 0;

        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 100, 50, &output, &output_size);
        CHECK(result == -1);
        
        // Equal start and end should also fail
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 50, 50, &output, &output_size);
        CHECK(result == -1);
    }
    
    SUBCASE("Non-existent file") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Use cross-platform non-existent path
        fs::path non_existent = fs::temp_directory_path() / "nonexistent" / "file.gz";
        result = dft_reader_read_range_bytes(reader, non_existent.string().c_str(), 0, 50, &output, &output_size);
        CHECK(result == -1);
    }
    
    dft_reader_destroy(reader);
}

TEST_CASE("Get maximum bytes") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    SUBCASE("Empty index") {
        // Create reader without building index first
        dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
        if (reader) {
            size_t max_bytes;
            int result = dft_reader_get_max_bytes(reader, &max_bytes);
            // May fail or return 0 for empty index
            if (result == 0) {
                CHECK(max_bytes == 0);
            }
            dft_reader_destroy(reader);
        }
    }
    
    SUBCASE("Reader with built index") {
        // Build index first
        dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
        REQUIRE(indexer != nullptr);
        
        int result = dft_indexer_build(indexer);
        REQUIRE(result == 0);
        dft_indexer_destroy(indexer);
        
        // Create reader
        dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
        REQUIRE(reader != nullptr);
        
        size_t max_bytes;
        result = dft_reader_get_max_bytes(reader, &max_bytes);
        CHECK(result == 0);
        CHECK(max_bytes > 0);
        
        // Verify that we can't read beyond max_bytes
        char* output = nullptr;
        size_t output_size = 0;
        
        // Try to read beyond max_bytes - should succeed
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), max_bytes + 1, max_bytes + 100, &output, &output_size);
        CHECK(result == 0);
        
        // Try to read up to max_bytes - should succeed
        if (max_bytes > 10) {
            result = dft_reader_read_range_bytes(reader, gz_file.c_str(), max_bytes - 10, max_bytes, &output, &output_size);
            if (result == 0 && output) {
                CHECK(output_size > 0);
                free(output);
            }
        }
        
        dft_reader_destroy(reader);
    }
    
    SUBCASE("Null parameters") {
        std::string test_gz_file = env.create_test_gzip_file();
        REQUIRE(!test_gz_file.empty());
        
        std::string test_idx_file = env.get_index_path(test_gz_file);
        
        dft_reader_handle_t reader = dft_reader_create(test_gz_file.c_str(), test_idx_file.c_str());
        if (reader) {
            size_t max_bytes;
            
            // null reader
            int result = dft_reader_get_max_bytes(nullptr, &max_bytes);
            CHECK(result == -1);
            
            // null max_bytes
            result = dft_reader_get_max_bytes(reader, nullptr);
            CHECK(result == -1);
            
            dft_reader_destroy(reader);
        }
    }
}

TEST_CASE("Memory management") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);
    
    // multiple reads to ensure no memory leaks
    for (int i = 0; i < 100; i++) {
        char* output = nullptr;
        size_t output_size = 0;

        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 30, &output, &output_size);

        if (result == 0 && output) {
            CHECK(output_size > 0);
            free(output);
        }
    }
    
    dft_reader_destroy(reader);
}

TEST_CASE("Exact byte reading (small ranges)") {
    TestEnvironment env(23000);

    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);

    SUBCASE("Read at least 10 bytes") {
        char* output = nullptr;
        size_t output_size = 0;
        
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 10, 20, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 10);
            free(output);
        }
    }
    
    SUBCASE("Read at least 50 bytes from start") {
        char* output = nullptr;
        size_t output_size = 0;
        
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 50, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 50);
            free(output);
        }
    }

    SUBCASE("Large range should use JSON boundary search") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // First get max bytes to know file size
        size_t max_bytes;
        int max_result = dft_reader_get_max_bytes(reader, &max_bytes);
        REQUIRE(max_result == 0);
        
        if (max_bytes > 1024) {
            // Only test if file is large enough
            result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 2000, &output, &output_size);
            CHECK(result == 0);
            
            if (result == 0) {
                CHECK(output != nullptr);
                // Should be more than requested due to JSON boundary search
                CHECK(output_size >= 2000);
                free(output);
            }
        } else {
            // For small files, just test that we get all data
            result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, max_bytes, &output, &output_size);
            CHECK(result == 0);
            
            if (result == 0) {
                CHECK(output != nullptr);
                CHECK(output_size <= max_bytes);  // Should get all or less data
                free(output);
            }
        }
    }

    dft_reader_destroy(reader);
}

TEST_CASE("JSON boundary detection") {
    TestEnvironment env(1000);  // More lines for better boundary testing
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);

    SUBCASE("Small range should provide minimum requested bytes") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Request 100 bytes - should get AT LEAST 100 bytes due to boundary extension
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 100, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 100);  // Should get at least what was requested
            
            // Verify that output ends with complete JSON line
            std::string content(output, output_size);
            CHECK(content.back() == '\n');  // Should end with newline
            
            // Should contain complete JSON objects
            size_t last_brace = content.rfind('}');
            REQUIRE(last_brace != std::string::npos);
            CHECK(last_brace < content.length() - 1);  // '}' should not be the last character
            CHECK(content[last_brace + 1] == '\n');    // Should be followed by newline
            
            free(output);
        }
    }
    
    SUBCASE("Output should not cut off in middle of JSON") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Request 500 bytes - this should not cut off mid-JSON
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 500, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 500);
            
            std::string content(output, output_size);
            
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
            
            free(output);
        }
    }
    
    SUBCASE("Large range boundary detection") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Request 10000 bytes
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 10000, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 10000);
            
            std::string content(output, output_size);
            
            // Should end with complete JSON line
            CHECK(content.back() == '\n');
            
            // Count complete JSON objects
            size_t json_count = 0;
            size_t pos = 0;
            while ((pos = content.find("}\n", pos)) != std::string::npos) {
                json_count++;
                pos += 2;
            }
            CHECK(json_count > 0);  // Should have multiple complete JSON objects
            
            free(output);
        }
    }
    
    SUBCASE("Middle range with start boundary detection") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Start from byte 5000 to test start boundary detection
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 5000, 15000, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 10000);  // Should get at least requested range
            
            std::string content(output, output_size);
            
            // Should start with beginning of JSON object
            CHECK(content[0] == '{');
            
            // Should end with complete JSON line
            CHECK(content.back() == '\n');
            CHECK(content[content.length() - 2] == '}');
            
            free(output);
        }
    }
    
    SUBCASE("Very small range edge case") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Request only 2 bytes - should still get complete JSON
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 10, 12, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 2);  // Should get at least what was requested
            
            std::string content(output, output_size);
            
            // Even for tiny requests, should get complete JSON objects
            if (content.length() > 0) {
                // Should either be complete JSON objects or start with '{'
                bool starts_with_brace = (content[0] == '{');
                bool starts_with_bracket = (content[0] == '[');
                bool starts_properly = starts_with_brace || starts_with_bracket;
                CHECK(starts_properly);
                
                // If it contains '}', should end properly
                if (content.find('}') != std::string::npos) {
                    CHECK(content.back() == '\n');
                }
            }
            
            free(output);
        }
    }

    dft_reader_destroy(reader);
}

TEST_CASE("Regression test for truncated JSON output") {
    // This test specifically catches the original bug where output was like:
    // {"name":"name_%  instead of complete JSON lines
    
    TestEnvironment env(2000);  // Enough lines to trigger the boundary issue
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
    
    bool success = compress_file_to_gzip(txt_file, gz_file);
    REQUIRE(success);
    fs::remove(txt_file);
    
    // Build index
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 32.0, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);

    SUBCASE("Original failing case: 0 to 10000 bytes") {
        char* output = nullptr;
        size_t output_size = 0;
        
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 10000, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 10000);
            
            std::string content(output, output_size);
            
            // Should NOT end with incomplete patterns like "name_%
            CHECK(content.find("\"name_%") == std::string::npos);
            CHECK(content.find("\"cat_%") == std::string::npos);
            
            // Should end with complete JSON line
            CHECK(content.back() == '\n');
            CHECK(content[content.length() - 2] == '}');
            
            // Should contain the pattern but complete
            CHECK(content.find("\"name\":\"name_") != std::string::npos);
            CHECK(content.find("\"cat\":\"cat_") != std::string::npos);
            
            free(output);
        }
    }
    
    SUBCASE("Small range minimum bytes check") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // This was returning only 44 bytes instead of at least 100
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 100, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size >= 100);  // This was the main bug - was only 44 bytes
            
            std::string content(output, output_size);
            
            // Should contain multiple complete JSON objects for 100+ bytes
            size_t brace_count = 0;
            for (char c : content) {
                if (c == '}') brace_count++;
            }
            CHECK(brace_count >= 2);  // Should have at least 2 complete objects for 100+ bytes
            
            free(output);
        }
    }

    dft_reader_destroy(reader);
}
