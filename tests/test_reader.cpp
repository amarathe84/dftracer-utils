#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sqlite3.h>
#include <string>
#include <random>
#include <fstream>
#include <vector>
#include <zlib.h>

#include "indexer.h"
#include "reader.h"
#include "filesystem.h"

sqlite3* create_temp_db(void);
bool compress_file_to_gzip(const std::string& input_file, const std::string& output_file);

// Cross-platform gzip compression function
bool compress_file_to_gzip(const std::string& input_file, const std::string& output_file) {
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
    TestEnvironment() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(100000, 999999);
        
        fs::path temp_base = fs::temp_directory_path();
        std::string base_dir = temp_base / ("dftracer_test_" + std::to_string(dis(gen)));
        fs::create_directories(base_dir);
        test_dir = base_dir;
    }

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
        std::string txt_file = test_dir + "/test_data.txt";
        
        // Write test data to text file
        std::ofstream f(txt_file);
        if (!f.is_open()) {
            return "";
        }

        f << "{\"id\": 1, \"message\": \"Hello World\"}\n";
        f << "{\"id\": 2, \"message\": \"Testing 123\"}\n";
        f << "{\"id\": 3, \"message\": \"More test data\"}\n";
        f << "{\"id\": 4, \"message\": \"Final line\"}\n";
        f.close();
        
        bool success = compress_file_to_gzip(txt_file, gz_file);

        fs::remove(txt_file);
        
        if (success) {
            return gz_file;
        }
        
        return "";
    }
    
private:
    std::string test_dir;
};

sqlite3* create_temp_db() {
    sqlite3* db = nullptr;
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        return nullptr;
    }

    if (dft::indexer::init(db) != 0) {
        sqlite3_close(db);
        return nullptr;
    }
    
    return db;
}

TEST_CASE("Database schema initialization") {
    sqlite3* db = create_temp_db();
    REQUIRE(db != nullptr);
    
    // check if tables exist
    const char* check_tables_sql = 
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('files', 'chunks');";
    sqlite3_stmt* stmt;
    
    int rc = sqlite3_prepare_v2(db, check_tables_sql, -1, &stmt, nullptr);
    REQUIRE(rc == SQLITE_OK);
    
    int table_count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        table_count++;
    }
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    CHECK(table_count == 2);
}

TEST_CASE("Gzip index building") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    sqlite3* db = create_temp_db();
    REQUIRE(db != nullptr);
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());
    
    int result = dft::indexer::build(db, 1, gz_file.c_str(), 1024);
    CHECK(result == 0);
    
    // verify chunks were created
    const char* count_chunks_sql = "SELECT COUNT(*) FROM chunks WHERE file_id = 1;";
    sqlite3_stmt* stmt;
    
    int rc = sqlite3_prepare_v2(db, count_chunks_sql, -1, &stmt, nullptr);
    REQUIRE(rc == SQLITE_OK);
    
    rc = sqlite3_step(stmt);
    REQUIRE(rc == SQLITE_ROW);
    
    int chunk_count = sqlite3_column_int(stmt, 0);
    CHECK(chunk_count > 0);
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

TEST_CASE("Data range reading") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    sqlite3* db = create_temp_db();
    REQUIRE(db != nullptr);
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    int result = dft::indexer::build(db, 1, gz_file.c_str(), 512);
    REQUIRE(result == 0);
    
    SUBCASE("Read valid byte range") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // read first 50 bytes
        // reader may return more to complete JSON lines
        result = dft::reader::read_range_bytes(db, gz_file.c_str(), 0, 50, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size > 0);
            // reader may return more bytes to complete JSON lines
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
        
        // null db
        result = dft::reader::read_range_bytes(nullptr, gz_file.c_str(), 0, 50, &output, &output_size);
        CHECK(result == -1);
        
        // null gz_path
        result = dft::reader::read_range_bytes(db, nullptr, 0, 50, &output, &output_size);
        CHECK(result == -1);
        
        // null output
        result = dft::reader::read_range_bytes(db, gz_file.c_str(), 0, 50, nullptr, &output_size);
        CHECK(result == -1);
        
        // null output_size
        result = dft::reader::read_range_bytes(db, gz_file.c_str(), 0, 50, &output, nullptr);
        CHECK(result == -1);
    }
    
    SUBCASE("Read megabyte range") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // read first 0.001 MB (about 1024 bytes)
        result = dft::reader::read_range_megabytes(db, gz_file.c_str(), 0.0, 0.001, &output, &output_size);
        CHECK(result == 0);
        
        if (result == 0) {
            CHECK(output != nullptr);
            CHECK(output_size > 0);
            
            free(output);
        }
    }
    
    sqlite3_close(db);
}

TEST_CASE("Edge cases") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    sqlite3* db = create_temp_db();
    REQUIRE(db != nullptr);

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    int result = dft::indexer::build(db, 1, gz_file.c_str(), 512);
    REQUIRE(result == 0);
    
    SUBCASE("Invalid byte range (start > end)") {
        char* output = nullptr;
        size_t output_size = 0;

        result = dft::reader::read_range_bytes(db, gz_file.c_str(), 100, 50, &output, &output_size);
        // this should either fail or handle gracefully
        if (result == 0 && output) {
            free(output);
        }
    }
    
    SUBCASE("Non-existent file") {
        char* output = nullptr;
        size_t output_size = 0;
        
        // Use cross-platform non-existent path
        fs::path non_existent = fs::temp_directory_path() / "nonexistent" / "file.gz";
        result = dft::reader::read_range_bytes(db, non_existent.string().c_str(), 0, 50, &output, &output_size);
        CHECK(result == -1);
    }
    
    sqlite3_close(db);
}

TEST_CASE("Memory management") {
    TestEnvironment env;
    REQUIRE(env.is_valid());
    
    sqlite3* db = create_temp_db();
    REQUIRE(db != nullptr);
    
    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    int result = dft::indexer::build(db, 1, gz_file.c_str(), 512);
    REQUIRE(result == 0);
    
    // multiple reads to ensure no memory leaks
    for (int i = 0; i < 10000; i++) {
        char* output = nullptr;
        size_t output_size = 0;

        result = dft::reader::read_range_bytes(db, gz_file.c_str(), 0, 30, &output, &output_size);

        if (result == 0 && output) {
            CHECK(output_size > 0);
            free(output);
        }
    }
    
    sqlite3_close(db);
}
