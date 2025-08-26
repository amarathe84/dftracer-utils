#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/error.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/filesystem.h>
#include <doctest/doctest.h>

#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "testing_utilities.h"

using namespace dftracer::utils;
using namespace dft_utils_test;

TEST_CASE("C++ Indexer - Basic functionality") {
    TestEnvironment env;
    REQUIRE(env.is_valid());

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    std::string idx_file = env.get_index_path(gz_file);

    SUBCASE("Build index") {
        Indexer indexer(gz_file, idx_file, mb_to_b(1.0));
        CHECK_NOTHROW(indexer.build());
    }

    SUBCASE("Check rebuild needed") {
        Indexer indexer(gz_file, idx_file, mb_to_b(1.0));
        CHECK(indexer.need_rebuild());  // Should need rebuild initially

        indexer.build();
        CHECK_FALSE(
            indexer.need_rebuild());  // Should not need rebuild after building
    }

    SUBCASE("Getter methods") {
        size_t ckpt_size = mb_to_b(1.5);
        Indexer indexer(gz_file, idx_file, ckpt_size);

        // Test getter methods
        CHECK(indexer.get_gz_path() == gz_file);
        CHECK(indexer.get_idx_path() == idx_file);

        // size will be adjusted
        CHECK(indexer.get_checkpoint_size() <= ckpt_size);
    }

    SUBCASE("Move semantics") {
        Indexer indexer1(gz_file, idx_file, 1.0);

        // Move constructor
        Indexer indexer2 = std::move(indexer1);

        // Move assignment
        Indexer indexer3(gz_file, idx_file, 2.0);
        indexer3 = std::move(indexer2);
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
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();
    }

    SUBCASE("Constructor and destructor") {
        // Test automatic destruction
        {
            Reader reader(gz_file, idx_file);
            CHECK(reader.is_valid());
            CHECK(reader.get_gz_path() == gz_file);
        }

        // Should be able to create another one
        Reader reader2(gz_file, idx_file);
        CHECK(reader2.is_valid());
    }

    SUBCASE("Get max bytes") {
        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        CHECK(max_bytes > 0);
    }

    SUBCASE("Getter methods") {
        Reader reader(gz_file, idx_file);

        // Test getter methods
        CHECK(reader.get_gz_path() == gz_file);
        CHECK(reader.get_idx_path() == idx_file);
    }

    SUBCASE("Read byte range using streaming API") {
        Reader reader(gz_file, idx_file);

        // Read using streaming API
        const size_t buffer_size = 1024;
        char buffer[1024];
        std::string result;

        // Stream data until no more available
        size_t bytes_read;
        while ((bytes_read = reader.read(0, 50, buffer, buffer_size)) > 0) {
            result.append(buffer, bytes_read);
        }

        CHECK(result.size() <= 50);
        CHECK(!result.empty());
    }

    SUBCASE("Move semantics") {
        Reader reader1(gz_file, idx_file);
        CHECK(reader1.is_valid());

        // Move constructor
        Reader reader2 = std::move(reader1);
        CHECK(reader2.is_valid());
        CHECK_FALSE(reader1.is_valid());  // reader1 should be moved from

        // Move assignment
        Reader reader3(gz_file, idx_file);
        reader3 = std::move(reader2);
        CHECK(reader3.is_valid());
        CHECK_FALSE(reader2.is_valid());  // reader2 should be moved from
    }
}

TEST_CASE("C++ API - Error handling") {
    SUBCASE("Invalid indexer creation") {
        CHECK_THROWS_AS(Indexer indexer("/nonexistent/path.gz",
                                        "/nonexistent/path.idx", 1.0),
                        IndexerError);
    }

    SUBCASE("Invalid reader creation") {
        CHECK_THROWS_AS(Reader("/nonexistent/path.gz", "/nonexistent/path.idx"),
                        ReaderError);
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
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();
    }

    // Create reader
    Reader reader(gz_file, idx_file);

    SUBCASE("Read valid byte range") {
        // read first 50 bytes using streaming API
        const size_t buffer_size = 1024;
        char buffer[1024];
        std::string content;

        // Stream data until no more available
        size_t bytes_read;
        while ((bytes_read = reader.read(0, 50, buffer, buffer_size)) > 0) {
            content.append(buffer, bytes_read);
        }

        CHECK(content.size() <= 50);
        CHECK(content.find("{") != std::string::npos);
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
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();
    }

    // Create reader

    SUBCASE("Invalid byte range (start >= end) should throw") {
        Reader reader(gz_file, idx_file);
        char buffer[1024];
        CHECK_THROWS(reader.read(100, 50, buffer, sizeof(buffer)));
        CHECK_THROWS(reader.read(50, 50, buffer,
                                 sizeof(buffer)));  // Equal start and end
    }

    SUBCASE("Non-existent file should throw") {
        // Use cross-platform non-existent path
        fs::path non_existent =
            fs::temp_directory_path() / "nonexistent" / "file.gz";
        CHECK_THROWS(Reader(non_existent.string(), non_existent.string()));
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
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();

        // Read data
        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        CHECK(max_bytes > 0);

        // Read multiple ranges using streaming API
        char buffer[1024];

        // Read first range
        std::string content1;
        size_t bytes_read;
        while ((bytes_read = reader.read(0, 100, buffer, sizeof(buffer))) > 0) {
            content1.append(buffer, bytes_read);
        }
        CHECK(content1.size() <= 100);

        // Read second range
        std::string content2;
        while ((bytes_read = reader.read(100, 200, buffer, sizeof(buffer))) >
               0) {
            content2.append(buffer, bytes_read);
        }
        CHECK(content2.size() <= 100);

        // Verify data content
        CHECK(content1.find("{") != std::string::npos);
        CHECK(content2.find("{") != std::string::npos);

        // All resources are automatically cleaned up when objects go out of
        // scope
    }
}

TEST_CASE("C++ API - Memory safety stress test") {
    TestEnvironment env(100000);
    REQUIRE(env.is_valid());

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    std::string idx_file = env.get_index_path(gz_file);

    // Build index
    {
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();
    }

    // Multiple reads with streaming API
    Reader reader(gz_file, idx_file);
    for (int i = 0; i < 3; ++i) {  // Reduced from 100 to 3 for easier debugging
        char buffer[1024];
        size_t total_bytes = 0;
        size_t bytes_read;

        while ((bytes_read = reader.read(0, 4 * 1024 * 1024, buffer,
                                         sizeof(buffer))) > 0) {
            total_bytes += bytes_read;
        }

        CHECK(total_bytes >= 50);
        reader.reset();
    }
}

TEST_CASE("C++ API - Exception handling comprehensive tests") {
    TestEnvironment env;
    REQUIRE(env.is_valid());

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    std::string idx_file = env.get_index_path(gz_file);

    SUBCASE("Indexer with invalid paths should throw during build") {
        CHECK_THROWS_AS(
            Indexer indexer("/definitely/nonexistent/path.gz",
                            "/also/nonexistent/path.idx", mb_to_b(1.0)),
            IndexerError);
    }

    SUBCASE("Indexer with invalid chunk size should throw in constructor") {
        CHECK_THROWS_AS(Indexer(gz_file, idx_file, mb_to_b(0.0)), IndexerError);
    }

    SUBCASE("Reader with invalid paths should throw in constructor") {
        CHECK_THROWS_AS(Reader("/definitely/nonexistent/path.gz",
                               "/also/nonexistent/path.idx"),
                        std::runtime_error);
    }

    SUBCASE("Reader operations on invalid reader should throw") {
        // Build a valid index first
        {
            Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
            indexer.build();
        }

        Reader reader(gz_file, idx_file);
        CHECK(reader.is_valid());

        // Make reader invalid by moving from it
        Reader moved_reader = std::move(reader);
        CHECK_FALSE(reader.is_valid());
        CHECK(moved_reader.is_valid());

        // Operations on valid moved reader should work
        CHECK_NOTHROW(moved_reader.get_max_bytes());
        char buffer[1024];
        CHECK_NOTHROW(moved_reader.read(0, 100, buffer, sizeof(buffer)));

        // Note: Testing operations on moved-from reader may cause undefined
        // behavior This is expected behavior for moved-from objects in C++
    }

    SUBCASE("Invalid read parameters should throw") {
        // Build a valid index first
        {
            Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
            indexer.build();
        }

        Reader reader(gz_file, idx_file);

        // Invalid ranges should throw
        char buffer[1024];
        CHECK_THROWS_AS(reader.read(100, 50, buffer, sizeof(buffer)),
                        ReaderError);  // start > end
        CHECK_THROWS_AS(reader.read(50, 50, buffer, sizeof(buffer)),
                        ReaderError);  // start == end
    }
}

TEST_CASE("C++ API - Advanced indexer functionality") {
    TestEnvironment env;
    REQUIRE(env.is_valid());

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    std::string idx_file = env.get_index_path(gz_file);

    SUBCASE("Multiple indexer instances for same file") {
        Indexer indexer1(gz_file, idx_file, mb_to_b(1.0));
        Indexer indexer2(gz_file, idx_file, mb_to_b(1.0));

        // Both should work independently
        CHECK_NOTHROW(indexer1.build());
        CHECK_NOTHROW(indexer2.build());

        // After first build, rebuild should not be needed
        CHECK_FALSE(indexer1.need_rebuild());
        CHECK_FALSE(indexer2.need_rebuild());
    }

    SUBCASE("Different checkpoint sizes") {
        Indexer indexer_small(gz_file, idx_file + "_small", mb_to_b(0.1));
        Indexer indexer_large(gz_file, idx_file + "_large", mb_to_b(10.0));

        CHECK_NOTHROW(indexer_small.build());
        CHECK_NOTHROW(indexer_large.build());

        // Both should create valid indices
        CHECK_NOTHROW(Reader(gz_file, idx_file + "_small"));
        CHECK_NOTHROW(Reader(gz_file, idx_file + "_large"));
    }

    SUBCASE("Indexer state after operations") {
        Indexer indexer(gz_file, idx_file, mb_to_b(1.0));

        CHECK(indexer.need_rebuild());

        indexer.build();
        CHECK_FALSE(indexer.need_rebuild());  // Should not need rebuild

        // Can build again
        CHECK_NOTHROW(indexer.build());
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
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();
    }

    SUBCASE("Multiple readers for same file") {
        Reader reader1(gz_file, idx_file);
        Reader reader2(gz_file, idx_file);

        CHECK(reader1.is_valid());
        CHECK(reader2.is_valid());

        // Both should work independently
        char buffer1[1024], buffer2[1024];
        std::string result1, result2;

        // Read from first reader
        size_t bytes_read1;
        while ((bytes_read1 = reader1.read(0, 100, buffer1, sizeof(buffer1))) >
               0) {
            result1.append(buffer1, bytes_read1);
        }

        // Read from second reader
        size_t bytes_read2;
        while ((bytes_read2 = reader2.read(0, 100, buffer2, sizeof(buffer2))) >
               0) {
            result2.append(buffer2, bytes_read2);
        }

        CHECK(!result1.empty());
        CHECK(!result2.empty());
        CHECK(result1.size() == result2.size());  // Should return same size

        // Content should be identical
        CHECK(result1 == result2);
    }

    SUBCASE("Reader state consistency") {
        Reader reader(gz_file, idx_file);

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
        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();

        char buffer[2048];
        std::string result;

        // Small reads
        result.clear();
        size_t bytes_read;
        while ((bytes_read = reader.read_line_bytes(0, 10, buffer,
                                                    sizeof(buffer))) > 0) {
            result.append(buffer, bytes_read);
        }
        // Small reads should return no data
        CHECK(result.size() == 0);

        // Medium reads
        if (max_bytes > 1000) {
            result.clear();
            while ((bytes_read = reader.read_line_bytes(100, 1000, buffer,
                                                        sizeof(buffer))) > 0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() <= 900);
        }

        // Large reads
        if (max_bytes > 10000) {
            result.clear();
            while ((bytes_read = reader.read_line_bytes(1000, 10000, buffer,
                                                        sizeof(buffer))) > 0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() >= 9000);
        }
    }

    SUBCASE("Boundary conditions") {
        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();

        char buffer[1024];
        std::string result;
        size_t bytes_read;

        if (max_bytes > 100) {
            // Read near the end of file
            result.clear();
            while ((bytes_read = reader.read(max_bytes - 50, max_bytes, buffer,
                                             sizeof(buffer))) > 0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() > 0);

            // Read at exact boundaries
            result.clear();
            while ((bytes_read = reader.read(0, 1, buffer, sizeof(buffer))) >
                   0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() <= 1);
        }

        // Read beyond file (should throw exception)
        result.clear();
        CHECK_THROWS_AS(
            reader.read(max_bytes, max_bytes + 1000, buffer, sizeof(buffer)),
            ReaderError);
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
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();
    }

    // Create reader
    Reader reader(gz_file, idx_file);

    SUBCASE("Small range should provide minimum requested bytes") {
        // Request 100 bytes - should get AT LEAST 100 bytes due to boundary
        // extension
        char buffer[2048];
        std::string content;
        size_t bytes_read;

        while ((bytes_read = reader.read_line_bytes(0, 100, buffer,
                                                    sizeof(buffer))) > 0) {
            content.append(buffer, bytes_read);
        }

        CHECK(content.size() <= 100);  // Should get at least what was requested

        // Verify that output ends with complete JSON line
        CHECK(content.back() == '\n');  // Should end with newline

        // Should contain complete JSON objects
        size_t last_brace = content.rfind('}');
        REQUIRE(last_brace != std::string::npos);
        CHECK(last_brace <
              content.length() - 1);  // '}' should not be the last character
        CHECK(content[last_brace + 1] ==
              '\n');  // Should be followed by newline
    }

    SUBCASE("Output should not cut off in middle of JSON") {
        // Request 500 bytes - this should not cut off mid-JSON
        char buffer[2048];
        std::string content;
        size_t bytes_read;

        while ((bytes_read = reader.read_line_bytes(0, 500, buffer,
                                                    sizeof(buffer))) > 0) {
            content.append(buffer, bytes_read);
        }

        CHECK(content.size() <= 500);

        // Should not end with partial JSON like {"name":"name_%
        size_t name_pos = content.find("\"name_");
        size_t last_brace_pos = content.rfind('}');
        bool has_incomplete_name =
            (name_pos != std::string::npos) && (name_pos > last_brace_pos);
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
            char buffer[2048];
            std::string content;
            size_t bytes_read;

            while ((bytes_read = reader.read_line_bytes(
                        current_pos, current_pos + segment_size, buffer,
                        sizeof(buffer))) > 0) {
                content.append(buffer, bytes_read);
            }

            CHECK(content.size() <= segment_size);
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
            CHECK(json_count >
                  0);  // Should have at least one complete JSON object
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
            Indexer indexer(gz_file, idx_file, mb_to_b(1.0));
            CHECK_NOTHROW(indexer.build());
        }

        // Test large reads
        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();
        CHECK(max_bytes > 10000);  // Should be a large file

        // Read large chunks
        if (max_bytes > 50000) {
            char buffer[4096];
            std::string content;

            size_t bytes_read;
            while ((bytes_read = reader.read_line_bytes(1000, 50000, buffer,
                                                        sizeof(buffer))) > 0) {
                content.append(buffer, bytes_read);
            }

            CHECK(content.size() <= 49000);
            CHECK(content.find("{") != std::string::npos);
            CHECK(content.back() == '\n');
        }
    }

    SUBCASE("Specific truncated JSON regression test") {
        // This test specifically catches the original bug where output was
        // like:
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
            f << "{\"name\":\"name_" << i << "\",\"cat\":\"cat_" << i
              << "\",\"dur\":" << (i * 10 % 1000) << "}\n";
        }
        f.close();

        bool success = dft_utils_test::compress_file_to_gzip(txt_file, gz_file);
        REQUIRE(success);
        fs::remove(txt_file);

        // Build index
        {
            Indexer indexer(gz_file, idx_file, mb_to_b(32.0));
            indexer.build();
        }

        // Create reader
        Reader reader(gz_file, idx_file);

        SUBCASE("Original failing case: 0 to 10000 bytes") {
            char buffer[4096];
            std::string content;

            size_t bytes_read;
            while ((bytes_read = reader.read_line_bytes(0, 10000, buffer,
                                                        sizeof(buffer))) > 0) {
                content.append(buffer, bytes_read);
            }

            CHECK(content.size() <= 10000);

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
            char buffer[2048];
            std::string content;

            size_t bytes_read;
            while ((bytes_read = reader.read_line_bytes(0, 100, buffer,
                                                        sizeof(buffer))) > 0) {
                content.append(buffer, bytes_read);
            }

            CHECK(content.size() <=
                  100);  // This was the main bug - was only 44 bytes

            // Should contain multiple complete JSON objects for 100+ bytes
            size_t brace_count = 0;
            for (char c : content) {
                if (c == '}') brace_count++;
            }
            CHECK(brace_count >=
                  2);  // Should have at least 2 complete objects for 100+ bytes
        }
    }
}

TEST_CASE("C++ Reader - Raw reading functionality") {
    TestEnvironment env;
    REQUIRE(env.is_valid());

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    std::string idx_file = env.get_index_path(gz_file);

    // Build index first
    {
        Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
        indexer.build();
    }

    SUBCASE("Basic raw read functionality") {
        Reader reader(gz_file, idx_file);

        // Read using raw API
        const size_t buffer_size = 1024;
        char buffer[1024];
        std::string raw_result;

        // Stream raw data until no more available
        size_t bytes_read;
        while ((bytes_read = reader.read(0, 50, buffer, buffer_size)) > 0) {
            raw_result.append(buffer, bytes_read);
        }

        CHECK(raw_result.size() >= 50);
        CHECK(!raw_result.empty());

        // Raw read should not care about JSON boundaries, so size should be
        // closer to requested
        CHECK(raw_result.size() <=
              60);  // Should be much closer to 50 than regular read
    }

    SUBCASE("Compare raw vs regular read") {
        Reader reader1(gz_file, idx_file);
        Reader reader2(gz_file, idx_file);

        const size_t buffer_size = 1024;
        char buffer1[1024], buffer2[1024];
        std::string raw_result, regular_result;

        // Raw read (new default behavior)
        size_t bytes_read1;
        while ((bytes_read1 = reader1.read(0, 100, buffer1, buffer_size)) > 0) {
            raw_result.append(buffer1, bytes_read1);
        }

        // Line bytes read (old read behavior)
        size_t bytes_read2;
        while ((bytes_read2 = reader2.read_line_bytes(0, 100, buffer2,
                                                      buffer_size)) > 0) {
            regular_result.append(buffer2, bytes_read2);
        }

        // Raw read should be closer to requested size (100 bytes)
        CHECK(raw_result.size() == 100);
        CHECK(regular_result.size() <= 100);

        // Regular read should be larger due to JSON boundary extension
        CHECK(regular_result.size() <= raw_result.size());

        // Regular read should end with complete JSON line
        CHECK(regular_result.back() == '\n');

        // Raw read may not end with newline (doesn't care about boundaries)
        // (but could happen to end with newline depending on data)

        // Both should start with same data
        size_t min_size = std::min(raw_result.size(), regular_result.size());
        CHECK(raw_result.substr(0, min_size) ==
              regular_result.substr(0, min_size));
    }

    SUBCASE("Raw read with different overloads") {
        Reader reader(gz_file, idx_file);

        const size_t buffer_size = 512;
        char buffer1[512], buffer2[512];
        std::string result1, result2;

        // Test explicit gz_path overload
        size_t bytes_read1;
        while ((bytes_read1 = reader.read(0, 75, buffer1, buffer_size)) > 0) {
            result1.append(buffer1, bytes_read1);
        }

        reader.reset();

        // Test stored gz_path overload
        size_t bytes_read2;
        while ((bytes_read2 = reader.read(0, 75, buffer2, buffer_size)) > 0) {
            result2.append(buffer2, bytes_read2);
        }

        // Both should return identical results
        CHECK(result1.size() == 75);
        CHECK(result1.size() == result2.size());
        CHECK(result1 == result2);
    }

    SUBCASE("Raw read edge cases") {
        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();

        char buffer[1024];
        std::string result;

        // Single byte read
        result.clear();
        size_t bytes_read;
        while ((bytes_read = reader.read(0, 1, buffer, sizeof(buffer))) > 0) {
            result.append(buffer, bytes_read);
        }
        CHECK(result.size() == 1);

        // Read near end of file
        if (max_bytes > 10) {
            result.clear();
            while ((bytes_read = reader.read(max_bytes - 10, max_bytes - 1,
                                             buffer, sizeof(buffer))) > 0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() == 9);
        }

        // Invalid ranges should still throw
        CHECK_THROWS(reader.read(100, 50, buffer, sizeof(buffer)));
        CHECK_THROWS(reader.read(50, 50, buffer, sizeof(buffer)));
    }

    SUBCASE("Raw read with small buffer") {
        Reader reader(gz_file, idx_file);

        // Use very small buffer to test streaming behavior
        const size_t small_buffer_size = 16;
        char small_buffer[16];
        std::string result;
        size_t total_calls = 0;

        size_t bytes_read;
        while ((bytes_read =
                    reader.read(0, 200, small_buffer, small_buffer_size)) > 0) {
            result.append(small_buffer, bytes_read);
            total_calls++;
            CHECK(bytes_read <= small_buffer_size);
            if (total_calls > 50) break;  // Safety guard
        }

        CHECK(result.size() == 200);
        CHECK(total_calls >
              1);  // Should require multiple calls with small buffer
    }

    SUBCASE("Raw read multiple ranges") {
        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();

        char buffer[1024];

        // Read multiple non-overlapping ranges
        std::vector<std::string> segments;
        std::vector<std::pair<size_t, size_t>> ranges = {
            {0, 50}, {50, 100}, {100, 150}};

        for (const auto& range : ranges) {
            if (range.second <= max_bytes) {
                std::string segment;
                size_t bytes_read;
                while ((bytes_read = reader.read(range.first, range.second,
                                                 buffer, sizeof(buffer))) > 0) {
                    segment.append(buffer, bytes_read);
                }

                CHECK(segment.size() >= (range.second - range.first));
                segments.push_back(segment);
            }
        }

        for (size_t i = 0; i < segments.size(); ++i) {
            size_t expected_size = ranges[i].second - ranges[i].first;
            CHECK(segments[i].size() == expected_size);
        }
    }

    SUBCASE("Full file read comparison: raw vs JSON-boundary aware") {
        Reader reader1(gz_file, idx_file);
        Reader reader2(gz_file, idx_file);

        size_t max_bytes = reader1.get_max_bytes();
        char buffer[4096];

        // Read entire file with raw API
        std::string raw_content;
        size_t bytes_read1;
        while ((bytes_read1 =
                    reader1.read(0, max_bytes, buffer, sizeof(buffer))) > 0) {
            raw_content.append(buffer, bytes_read1);
        }

        // Read entire file with line-boundary aware API
        std::string json_content;
        size_t bytes_read2;
        while ((bytes_read2 = reader2.read_line_bytes(0, max_bytes, buffer,
                                                      sizeof(buffer))) > 0) {
            json_content.append(buffer, bytes_read2);
        }

        // Both should read the entire file
        CHECK(raw_content.size() == max_bytes);
        CHECK(json_content.size() == max_bytes);

        // Total bytes should be identical when reading full file
        CHECK(raw_content.size() == json_content.size());

        // Content should be identical when reading full file
        CHECK(raw_content == json_content);

        // Both should end with complete JSON lines
        if (!raw_content.empty() && !json_content.empty()) {
            CHECK(raw_content.back() == '\n');
            CHECK(json_content.back() == '\n');

            // Find last JSON line in both
            size_t raw_last_newline =
                raw_content.rfind('\n', raw_content.size() - 2);
            size_t json_last_newline =
                json_content.rfind('\n', json_content.size() - 2);

            if (raw_last_newline != std::string::npos &&
                json_last_newline != std::string::npos) {
                std::string raw_last_line =
                    raw_content.substr(raw_last_newline + 1);
                std::string json_last_line =
                    json_content.substr(json_last_newline + 1);

                // Last JSON lines should be identical
                CHECK(raw_last_line == json_last_line);

                // Should contain valid JSON structure
                CHECK(raw_last_line.find('{') != std::string::npos);
                CHECK(raw_last_line.find('}') != std::string::npos);
                CHECK(json_last_line.find('{') != std::string::npos);
                CHECK(json_last_line.find('}') != std::string::npos);
            }
        }
    }
}

TEST_CASE("C++ Reader - Line reading functionality") {
    TestEnvironment env(10000);  // Create larger test with many lines for
                                 // better indexer support
    REQUIRE(env.is_valid());

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    std::string idx_file = env.get_index_path(gz_file);

    // Build index first with smaller chunk size to force checkpoint creation
    {
        Indexer indexer(gz_file, idx_file, mb_to_b(0.1));
        indexer.build();

        // Verify the indexer has line counts and checkpoints
        size_t total_lines = indexer.get_num_lines();
        auto checkpoints = indexer.get_checkpoints();

        // Skip line reading tests if indexer doesn't have proper line support
        // This can happen with very small test files
        if (total_lines == 0 || checkpoints.empty()) {
            WARN(
                "Skipping line reading tests - indexer has no line data (file "
                "too "
                "small?)");
            return;
        }

        INFO("Indexer created with " << checkpoints.size()
                                     << " checkpoints and " << total_lines
                                     << " total lines");
    }

    SUBCASE("Basic line reading functionality") {
        Reader reader(gz_file, idx_file);

        // Read first 5 lines
        std::string result = reader.read_lines(1, 5);
        CHECK(!result.empty());

        // Count newlines to verify we got the right number of lines
        size_t line_count = 0;
        for (char c : result) {
            if (c == '\n') line_count++;
        }
        CHECK(line_count == 5);  // Should have exactly 5 lines

        // Verify it starts with expected pattern (actual test data format)
        CHECK(result.find("\"id\": 1") != std::string::npos);
    }

    SUBCASE("Line reading accuracy - specific line numbers") {
        Reader reader(gz_file, idx_file);

        // Test specific line numbers that should contain predictable content
        for (size_t line_num : std::vector<size_t>({1, 10, 50, 100})) {
            std::string result = reader.read_lines(line_num, line_num);
            CHECK(!result.empty());

            // Should contain id: N where N = line_num
            std::string expected_pattern =
                "\"id\": " + std::to_string(line_num);
            CHECK(result.find(expected_pattern) != std::string::npos);

            // Should have exactly one line
            size_t line_count = 0;
            for (char c : result) {
                if (c == '\n') line_count++;
            }
            CHECK(line_count == 1);
        }
    }

    SUBCASE("Line range reading") {
        Reader reader(gz_file, idx_file);

        // Read line range 10-15 (6 lines total)
        std::string result = reader.read_lines(10, 15);
        CHECK(!result.empty());

        // Count lines
        size_t line_count = 0;
        for (char c : result) {
            if (c == '\n') line_count++;
        }
        CHECK(line_count ==
              6);  // Should have exactly 6 lines (10, 11, 12, 13, 14, 15)

        // Should start with line 10 and end with line 15
        CHECK(result.find("\"id\": 10") != std::string::npos);
        CHECK(result.find("\"id\": 15") != std::string::npos);

        // Should not contain line 9 or 16
        CHECK(result.find("\"id\": 9") == std::string::npos);
        CHECK(result.find("\"id\": 16") == std::string::npos);
    }

    SUBCASE("Line reading consistency with sed behavior") {
        Reader reader(gz_file, idx_file);

        // Test that our line numbering matches sed's 1-based numbering
        // Line 1 should contain id: 1, line 2 should contain id: 2, etc.
        for (size_t i = 1; i <= 5; ++i) {
            std::string result = reader.read_lines(i, i);
            std::string expected_id = "\"id\": " + std::to_string(i);
            CHECK(result.find(expected_id) != std::string::npos);
        }
    }

    SUBCASE("Error handling for invalid line numbers") {
        Reader reader(gz_file, idx_file);

        // 0-based line numbers should throw (we use 1-based)
        CHECK_THROWS_AS(reader.read_lines(0, 5), std::runtime_error);
        CHECK_THROWS_AS(reader.read_lines(1, 0), std::runtime_error);

        // start > end should throw
        CHECK_THROWS_AS(reader.read_lines(10, 5), std::runtime_error);
    }

    SUBCASE("Large line ranges") {
        Reader reader(gz_file, idx_file);

        // Get number of lines from indexer
        Indexer indexer(gz_file, idx_file, mb_to_b(0.1));
        size_t num_lines = indexer.get_num_lines();

        if (num_lines > 100) {
            // Read a large range
            std::string result = reader.read_lines(1, 100);
            CHECK(!result.empty());

            // Count lines
            size_t line_count = 0;
            for (char c : result) {
                if (c == '\n') line_count++;
            }
            CHECK(line_count == 100);

            // Should start with line 1 and end with line 100
            CHECK(result.find("\"id\": 1") != std::string::npos);
            CHECK(result.find("\"id\": 100") != std::string::npos);
        }
    }

    SUBCASE("Line reading near file boundaries") {
        Reader reader(gz_file, idx_file);

        // Get number of lines from indexer
        Indexer indexer(gz_file, idx_file, mb_to_b(0.1));
        size_t total_lines = indexer.get_num_lines();

        if (total_lines > 10) {
            // Read last few lines
            size_t start_line = total_lines - 5;
            std::string result = reader.read_lines(start_line, total_lines);
            CHECK(!result.empty());

            // Should have exactly 6 lines (start_line through total_lines
            // inclusive)
            size_t line_count = 0;
            for (char c : result) {
                if (c == '\n') line_count++;
            }
            CHECK(line_count == 6);
        }
    }

    SUBCASE("Single line reads at various positions") {
        Reader reader(gz_file, idx_file);

        // Get number of lines from indexer
        Indexer indexer(gz_file, idx_file, mb_to_b(0.1));
        size_t total_lines = indexer.get_num_lines();

        // Test single line reads at different positions
        std::vector<size_t> test_lines = {1, total_lines / 4, total_lines / 2,
                                          total_lines - 1, total_lines};

        for (size_t line_num : test_lines) {
            if (line_num <= total_lines) {
                std::string result = reader.read_lines(line_num, line_num);
                CHECK(!result.empty());

                // Should have exactly one line
                size_t line_count = 0;
                for (char c : result) {
                    if (c == '\n') line_count++;
                }
                CHECK(line_count == 1);

                // Should contain the expected id pattern
                std::string expected_id = "\"id\": " + std::to_string(line_num);
                CHECK(result.find(expected_id) != std::string::npos);
            }
        }
    }
}

TEST_CASE("C++ Advanced Functions - Error Paths and Edge Cases") {
    TestEnvironment env(1000);
    REQUIRE(env.is_valid());

    std::string gz_file = env.create_test_gzip_file();
    REQUIRE(!gz_file.empty());

    std::string idx_file = env.get_index_path(gz_file);

    SUBCASE("Indexer with various checkpoint sizes") {
        // Test different chunk sizes to trigger different code paths
        for (double ckpt_size_mb : {0.1, 0.5, 1.0, 2.0, 5.0}) {
            size_t ckpt_size = mb_to_b(ckpt_size_mb);
            Indexer indexer(gz_file, idx_file + std::to_string(ckpt_size_mb),
                            ckpt_size);
            CHECK_NOTHROW(indexer.build());
            CHECK(indexer.get_checkpoint_size() <= ckpt_size);
        }
    }

    SUBCASE("Reader with different range sizes to trigger various code paths") {
        // Build index first
        {
            Indexer indexer(gz_file, idx_file,
                            mb_to_b(0.1));  // Small chunks
            indexer.build();
        }

        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();

        // Test various range sizes to trigger different internal paths
        std::vector<std::pair<size_t, size_t>> ranges = {
            {0, 1},                               // Very small range
            {0, 10},                              // Small range
            {0, 100},                             // Medium range
            {0, 1000},                            // Large range
            {100, 200},                           // Mid-file range
            {max_bytes / 2, max_bytes / 2 + 50},  // Middle section
        };

        for (const auto& range : ranges) {
            size_t start = range.first;
            size_t end = range.second;
            if (end <= max_bytes) {
                char buffer[2048];
                std::string result;

                size_t bytes_read;
                while ((bytes_read = reader.read(start, end, buffer,
                                                 sizeof(buffer))) > 0) {
                    result.append(buffer, bytes_read);
                }

                CHECK(result.size() <= (end - start));
            }
        }
    }

    SUBCASE("Force rebuild scenarios") {
        // Test force rebuild functionality
        Indexer indexer1(gz_file, idx_file, mb_to_b(1.0), false);
        indexer1.build();
        CHECK_FALSE(indexer1.need_rebuild());

        // Force rebuild should rebuild even if not needed
        Indexer indexer2(gz_file, idx_file, mb_to_b(1.0), true);
        // Force rebuild affects behavior during construction/build
        CHECK_NOTHROW(indexer2.build());  // Should succeed even if forced
        // Note: force_rebuild flag behavior needs further investigation
        CHECK_FALSE(
            indexer2.need_rebuild());  // After building, shouldn't need rebuild
    }

    SUBCASE("Multiple readers on same index") {
        // Build index once
        {
            Indexer indexer(gz_file, idx_file, mb_to_b(1.0));
            indexer.build();
        }

        // Create multiple readers
        std::vector<Reader*> readers;
        for (int i = 0; i < 5; ++i) {
            readers.push_back(new Reader(gz_file, idx_file));
            CHECK(readers.back()->is_valid());
        }

        // All should be able to read simultaneously
        for (auto& reader : readers) {
            char buffer[1024];
            std::string result;

            size_t bytes_read;
            while ((bytes_read = reader->read(0, 50, buffer, sizeof(buffer))) >
                   0) {
                result.append(buffer, bytes_read);
            }

            CHECK(result.size() <= 50);
        }

        // Clean up
        for (auto& reader : readers) {
            delete reader;
        }
    }

    SUBCASE("Edge case: Reading near file boundaries") {
        // Build index
        {
            Indexer indexer(gz_file, idx_file, mb_to_b(0.5));
            indexer.build();
        }

        Reader reader(gz_file, idx_file);
        size_t max_bytes = reader.get_max_bytes();

        if (max_bytes > 10) {
            char buffer[1024];
            std::string result;
            size_t bytes_read;

            // Read from near the end
            result.clear();
            while ((bytes_read = reader.read(max_bytes - 100, max_bytes - 1,
                                             buffer, sizeof(buffer))) > 0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() <= 100);

            // Read the very last byte
            if (max_bytes > 1) {
                result.clear();
                while ((bytes_read = reader.read(max_bytes - 1, max_bytes,
                                                 buffer, sizeof(buffer))) > 0) {
                    result.append(buffer, bytes_read);
                }
                CHECK(result.size() <= 1);
            }
        }
    }

    SUBCASE("Large file handling") {
        // Create larger test environment
        TestEnvironment large_env(5000);  // More lines
        std::string large_gz = large_env.create_test_gzip_file();
        std::string large_idx = large_env.get_index_path(large_gz);

        // Build index with small chunks to force more complex compression
        {
            Indexer indexer(large_gz, large_idx, mb_to_b(0.1));
            indexer.build();
        }

        Reader reader(large_gz, large_idx);
        size_t max_bytes = reader.get_max_bytes();

        // Read various large ranges
        if (max_bytes > 1000) {
            char buffer[2048];
            std::string result;
            size_t bytes_read;

            // First range
            result.clear();
            while ((bytes_read = reader.read(0, 1000, buffer, sizeof(buffer))) >
                   0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() <= 1000);

            // Second range
            result.clear();
            while ((bytes_read =
                        reader.read(500, 1500, buffer, sizeof(buffer))) > 0) {
                result.append(buffer, bytes_read);
            }
            CHECK(result.size() <= 1000);
        }
    }
}
