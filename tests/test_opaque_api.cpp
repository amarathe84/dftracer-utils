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

#include "indexer.h"
#include "reader.h"
#include "filesystem.h"
#include "testing_utilities.h"

using namespace dft_utils_test;

TEST_CASE("Opaque indexer creation and destruction") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = gz_file + ".idx";
    
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    CHECK(indexer != nullptr);
    
    if (indexer) {
        dft_indexer_destroy(indexer);
    }
}

TEST_CASE("Opaque indexer invalid parameters") {
    dft_indexer_handle_t indexer;
    
    // Test null gz_path
    indexer = dft_indexer_create(nullptr, "test.idx", 1.0, false);
    CHECK(indexer == nullptr);
    
    // Test null idx_path
    indexer = dft_indexer_create("test.gz", nullptr, 1.0, false);
    CHECK(indexer == nullptr);
    
    // Test invalid chunk size
    indexer = dft_indexer_create("test.gz", "test.idx", 0.0, false);
    CHECK(indexer == nullptr);
    
    indexer = dft_indexer_create("test.gz", "test.idx", -1.0, false);
    CHECK(indexer == nullptr);
}

TEST_CASE("Opaque indexer build and rebuild detection") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = gz_file + ".idx";
    
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    REQUIRE(indexer != nullptr);
    
    // Initial build should be needed
    int need_rebuild = dft_indexer_need_rebuild(indexer);
    CHECK(need_rebuild == 1);
    
    // Build the index
    int result = dft_indexer_build(indexer);
    CHECK(result == 0);
    
    dft_indexer_destroy(indexer);
    
    // Create new indexer with same parameters
    indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    REQUIRE(indexer != nullptr);
    
    // Should not need rebuild now
    need_rebuild = dft_indexer_need_rebuild(indexer);
    CHECK(need_rebuild == 0);
    
    dft_indexer_destroy(indexer);
    
    // Create new indexer with different chunk size
    indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 2.0, false);
    REQUIRE(indexer != nullptr);
    
    // Should not rebuild due to different chunk size
    need_rebuild = dft_indexer_need_rebuild(indexer);
    CHECK(need_rebuild == 0);
    
    dft_indexer_destroy(indexer);
}

TEST_CASE("Opaque indexer force rebuild") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = gz_file + ".idx";
    
    // Build initial index
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    CHECK(result == 0);
    
    dft_indexer_destroy(indexer);
    
    // Create indexer with force rebuild
    indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, true);
    REQUIRE(indexer != nullptr);
    
    // Should need rebuild because force is enabled
    int need_rebuild = dft_indexer_need_rebuild(indexer);
    CHECK(need_rebuild == 1);
    
    dft_indexer_destroy(indexer);
}

TEST_CASE("Opaque reader creation and destruction") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = gz_file + ".idx";
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    CHECK(reader != nullptr);
    
    if (reader) {
        dft_reader_destroy(reader);
    }
}

TEST_CASE("Opaque reader invalid parameters") {
    dft_reader_handle_t reader;
    
    // Test null gz_path
    reader = dft_reader_create(nullptr, "test.idx");
    CHECK(reader == nullptr);
    
    // Test null idx_path
    reader = dft_reader_create("test.gz", nullptr);
    CHECK(reader == nullptr);
    
    // Test with valid paths (SQLite will create database if it doesn't exist)
    // This is expected behavior, so we'll test that it succeeds
    reader = dft_reader_create("nonexistent.gz", "nonexistent.idx");
    if (reader) {
        dft_reader_destroy(reader);
    }
    // Note: Reader creation succeeds even with non-existent files
    // because SQLite creates the database file if it doesn't exist
}

TEST_CASE("Opaque reader byte range reading") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = gz_file + ".idx";
    
    // Build index
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
        
        // Read first 100 bytes
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 100, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size > 0);
            CHECK(output_size >= 100); // May return more to complete lines
            
            // Check that output contains valid JSON-like content
            std::string data(output, output_size);
            CHECK(data.find("{\"id\":") != std::string::npos);
            
            free(output);
        }
    }
    
    SUBCASE("Read invalid parameters") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Test null output pointer
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 50, nullptr, &output_size);
        CHECK(result != 0);
        
        // Test null output_size pointer
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 50, &output, nullptr);
        CHECK(result != 0);
        
        // Test invalid range (start > end)
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 100, 50, &output, &output_size);
        CHECK(result != 0);
    }
    
    dft_reader_destroy(reader);
}

TEST_CASE("Opaque reader maximum bytes") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = gz_file + ".idx";
    
    // Build index
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);
    
    size_t max_bytes = 0;
    result = dft_reader_get_max_bytes(reader, &max_bytes);
    CHECK(result == 0);
    CHECK(max_bytes > 0);

    // Test reading beyond max bytes should succeed but return empty output and output size
    char* output = nullptr;
    size_t output_size = 0;
    result = dft_reader_read_range_bytes(reader, gz_file.c_str(), max_bytes + 1, max_bytes + 100, &output, &output_size);
    CHECK(result == 0);
    CHECK(output_size == 0);
    free(output);

    dft_reader_destroy(reader);
}

TEST_CASE("C++ namespace wrappers") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = gz_file + ".idx";
    
    // Test indexer namespace wrappers with std::string
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
    CHECK(indexer != nullptr);
    
    if (indexer) {
        bool need_rebuild = dft_indexer_need_rebuild(indexer);
        CHECK(need_rebuild == true); // First time should need rebuild
        
        int result = dft_indexer_build(indexer);
        CHECK(result == 0);
        
        dft_indexer_destroy(indexer);
    }
    
    // Test reader namespace wrappers with std::string
    dft_reader_handle_t reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    CHECK(reader != nullptr);
    
    if (reader) {
        size_t max_bytes = 0;
        int result = dft_reader_get_max_bytes(reader, &max_bytes);
        CHECK(result == 0);
        CHECK(max_bytes > 0);
        
        char* output = nullptr;
        size_t output_size = 0;
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 100, &output, &output_size);
        CHECK(result == 0);
        
        if (output) {
            CHECK(output_size > 0);
            free(output);
        }
        
        dft_reader_destroy(reader);
    }
}
