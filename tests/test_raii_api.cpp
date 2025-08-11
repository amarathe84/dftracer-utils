#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

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
        fs::path test_path = temp_base / ("dftracer_raii_test_" + std::to_string(dis(gen)));
        
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

TEST_CASE("RAII Indexer - Basic functionality") {
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

TEST_CASE("RAII Reader - Basic functionality") {
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
        CHECK(result1.second == 50);
        
        // Read using stored gz_path
        auto result2 = reader.read_range_bytes(0, 50);
        CHECK(result2.first != nullptr);
        CHECK(result2.second == 50);
        
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

TEST_CASE("RAII API - Error handling") {
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

TEST_CASE("RAII API - Integration test") {
    TestEnvironment env(1000);
    REQUIRE(env.is_valid());
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    std::string idx_file = env.get_index_path(gz_file);
    
    // Complete workflow using RAII
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
        CHECK(result1.second == 100);
        
        auto result2 = reader.read_range_bytes(100, 200);
        CHECK(result2.first != nullptr);
        CHECK(result2.second == 100);
        
        // Verify data content
        std::string content1(result1.first.get(), result1.second);
        std::string content2(result2.first.get(), result2.second);
        CHECK(content1.find("{") != std::string::npos);
        CHECK(content2.find("{") != std::string::npos);
        
        // All resources are automatically cleaned up when objects go out of scope
    }
}

TEST_CASE("RAII API - Memory safety stress test") {
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
        CHECK(result.second == 50);
        // Memory automatically freed each iteration
    }
}
