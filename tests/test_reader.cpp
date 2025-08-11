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

// Cross-platform gzip compression function
static bool compress_file_to_gzip(const std::string& input_file, const std::string& output_file) {
    std::ifstream input(input_file, std::ios::binary);
    if (!input.is_open()) {
        return false;
    }
    
    gzFile gz_output = gzopen(output_file.c_str(), "wb");
    if (!gz_output) {
        return false;
    }
    
    const size_t buffer_size = 8192;
    std::vector<char> buffer(buffer_size);
    
    while (input.read(buffer.data(), buffer_size) || input.gcount() > 0) {
        unsigned int bytes_read = static_cast<unsigned int>(input.gcount());
        if (gzwrite(gz_output, buffer.data(), bytes_read) != static_cast<int>(bytes_read)) {
            gzclose(gz_output);
            return false;
        }
    }
    
    gzclose(gz_output);
    return true;
}

class TestEnvironment {
public:
    TestEnvironment(size_t lines): num_lines(lines) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(100000, 999999);
        
        fs::path temp_base = fs::temp_directory_path();
        fs::path test_path = temp_base / ("dftracer_test_" + std::to_string(dis(gen)));
        
        try {
            if (fs::create_directories(test_path)) {
                test_dir = test_path.string();
            }
        } catch (const std::exception& e) {
            // Leave test_dir empty to indicate failure
        }
    }

    TestEnvironment(): TestEnvironment(100) {}

    TestEnvironment(const TestEnvironment&) = delete;
    TestEnvironment& operator=(const TestEnvironment&) = delete;

    ~TestEnvironment() {
        if (!test_dir.empty()) {
            fs::remove_all(test_dir);
        }
    }
    
    const std::string& get_dir() const { return test_dir; }
    bool is_valid() const { return !test_dir.empty(); }


    std::string create_test_gzip_file() {
        if (test_dir.empty()) {
            return "";
        }
        
        // Create test file in the unique directory
        std::string gz_file = test_dir + "/test_data.gz";
        std::string idx_file = test_dir + "/test_data.gz.idx";
        std::string txt_file = test_dir + "/test_data.txt";
        
        // Write test data to text file
        std::ofstream f(txt_file);
        if (!f.is_open()) {
            return "";
        }

        for (size_t i = 1; i <= num_lines; ++i) {
            f << "{\"id\": " << i << ", \"message\": \"Test message " << i << "\"}\n";
        }
        f.close();
        
        bool success = compress_file_to_gzip(txt_file, gz_file);

        fs::remove(txt_file);
        
        if (success) {
            return gz_file;
        }
        
        return "";
    }

    std::string get_index_path(const std::string& gz_file) {
        return gz_file + ".idx";
    }
    
private:
    size_t num_lines;
    std::string test_dir;
};

TEST_CASE("Indexer creation and destruction") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    dft_indexer_t* indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
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
    
    dft_indexer_t* indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 1.0, false);
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
    dft_indexer_t* indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_t* reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
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
            // For small ranges (< 1KB), should get exact bytes
            CHECK(output_size == 50);
            
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
    dft_indexer_t* indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_t* reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
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
        dft_reader_t* reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
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
        dft_indexer_t* indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
        REQUIRE(indexer != nullptr);
        
        int result = dft_indexer_build(indexer);
        REQUIRE(result == 0);
        dft_indexer_destroy(indexer);
        
        // Create reader
        dft_reader_t* reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
        REQUIRE(reader != nullptr);
        
        size_t max_bytes;
        result = dft_reader_get_max_bytes(reader, &max_bytes);
        CHECK(result == 0);
        CHECK(max_bytes > 0);
        
        // Verify that we can't read beyond max_bytes
        char* output = nullptr;
        size_t output_size = 0;
        
        // Try to read beyond max_bytes - should fail
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), max_bytes + 1, max_bytes + 100, &output, &output_size);
        CHECK(result == -1);
        
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
        
        dft_reader_t* reader = dft_reader_create(test_gz_file.c_str(), test_idx_file.c_str());
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
    dft_indexer_t* indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_t* reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);
    
    // multiple reads to ensure no memory leaks
    for (int i = 0; i < 100; i++) {  // Reduced from 10000 to speed up tests
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
    dft_indexer_t* indexer = dft_indexer_create(gz_file.c_str(), idx_file.c_str(), 0.5, false);
    REQUIRE(indexer != nullptr);
    
    int result = dft_indexer_build(indexer);
    REQUIRE(result == 0);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_t* reader = dft_reader_create(gz_file.c_str(), idx_file.c_str());
    REQUIRE(reader != nullptr);

    SUBCASE("Read exactly 10 bytes") {
        char* output = nullptr;
        size_t output_size = 0;
        
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 10, 20, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size == 10);  // Should be exact for small ranges
            free(output);
        }
    }
    
    SUBCASE("Read exactly 50 bytes from start") {
        char* output = nullptr;
        size_t output_size = 0;
        
        result = dft_reader_read_range_bytes(reader, gz_file.c_str(), 0, 50, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size == 50);  // Should be exact for small ranges
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
