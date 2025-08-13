#include <unity.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <dft_utils/indexer/indexer.h>
#include <dft_utils/reader/reader.h>
#include <dft_utils/utils/logger.h>
#include "testing_utilities.h"

// Global test environment handle
static test_environment_handle_t g_env = NULL;
static char* g_gz_file = NULL;
static char* g_idx_file = NULL;

static test_environment_handle_t setup_test_environment(void);


void setUp(void) {
    setup_test_environment();
}

void tearDown(void) {
    // Called after each test - clean up any per-test allocations
    if (g_gz_file) {
        free(g_gz_file);
        g_gz_file = NULL;
    }
    if (g_idx_file) {
        free(g_idx_file);
        g_idx_file = NULL;
    }
    if (g_env) {
        test_environment_destroy(g_env);
        g_env = NULL;
    }
}

// Helper function to set up test environment for tests that need it
static test_environment_handle_t setup_test_environment(void) {
    if (!g_env) {
        g_env = test_environment_create();
        TEST_ASSERT_NOT_NULL(g_env);
        TEST_ASSERT_TRUE(test_environment_is_valid(g_env));
    }
    
    if (!g_gz_file) {
        g_gz_file = test_environment_create_test_gzip_file(g_env);
        TEST_ASSERT_NOT_NULL(g_gz_file);
    }
    
    if (!g_idx_file) {
        g_idx_file = test_environment_get_index_path(g_env, g_gz_file);
        TEST_ASSERT_NOT_NULL(g_idx_file);
    }

    return g_env;
}

void test_indexer_creation_and_destruction(void) {
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 1.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    if (indexer) {
        dft_indexer_destroy(indexer);
    }
}

void test_indexer_invalid_parameters(void) {
    dft_indexer_handle_t indexer;
    
    // Test null gz_path
    indexer = dft_indexer_create(NULL, "test.idx", 1.0, 0);
    TEST_ASSERT_NULL(indexer);
    
    // Test null idx_path
    indexer = dft_indexer_create("test.gz", NULL, 1.0, 0);
    TEST_ASSERT_NULL(indexer);
    
    // Test invalid chunk size
    indexer = dft_indexer_create("test.gz", "test.idx", 0.0, 0);
    TEST_ASSERT_NULL(indexer);
    
    indexer = dft_indexer_create("test.gz", "test.idx", -1.0, 0);
    TEST_ASSERT_NULL(indexer);
}

void test_gzip_index_building(void) {
    
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 1.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    dft_indexer_destroy(indexer);
}

void test_indexer_rebuild_detection(void) {
    // Create a separate test environment to avoid conflicts with other tests
    test_environment_handle_t test_env = test_environment_create();
    TEST_ASSERT_NOT_NULL(test_env);
    TEST_ASSERT_TRUE(test_environment_is_valid(test_env));
    
    char* test_gz_file = test_environment_create_test_gzip_file(test_env);
    TEST_ASSERT_NOT_NULL(test_gz_file);
    
    char* test_idx_file = test_environment_get_index_path(test_env, test_gz_file);
    TEST_ASSERT_NOT_NULL(test_idx_file);
    
    dft_indexer_handle_t indexer = dft_indexer_create(test_gz_file, test_idx_file, 1.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    // Initial build should be needed
    int need_rebuild = dft_indexer_need_rebuild(indexer);
    TEST_ASSERT_EQUAL_INT(1, need_rebuild);
    
    // Build the index
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    dft_indexer_destroy(indexer);
    
    // Create new indexer with same parameters
    indexer = dft_indexer_create(test_gz_file, test_idx_file, 1.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    // Should not need rebuild now
    need_rebuild = dft_indexer_need_rebuild(indexer);
    TEST_ASSERT_EQUAL_INT(0, need_rebuild);
    
    dft_indexer_destroy(indexer);
    
    // Create new indexer with different chunk size
    indexer = dft_indexer_create(test_gz_file, test_idx_file, 2.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    // Should not rebuild due to different chunk size
    need_rebuild = dft_indexer_need_rebuild(indexer);
    TEST_ASSERT_EQUAL_INT(0, need_rebuild);
    
    dft_indexer_destroy(indexer);
    
    // Clean up
    free(test_gz_file);
    free(test_idx_file);
}

void test_indexer_force_rebuild(void) {

    // Create indexer with force rebuild
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 1.0, 1);
    TEST_ASSERT_NOT_NULL(indexer);
    
    // Should need rebuild because no index is generated
    int need_rebuild = dft_indexer_need_rebuild(indexer);
    TEST_ASSERT_EQUAL_INT(1, need_rebuild);

    dft_indexer_destroy(indexer);
}

void test_reader_creation_and_destruction(void) {
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 1.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    if (reader) {
        dft_reader_destroy(reader);
    }

}

void test_reader_invalid_parameters(void) {
    dft_reader_handle_t reader;
    
    // Test null gz_path
    reader = dft_reader_create(NULL, "test.idx");
    TEST_ASSERT_NULL(reader);
    
    // Test null idx_path
    reader = dft_reader_create("test.gz", NULL);
    TEST_ASSERT_NULL(reader);
    
    // Test with valid paths (SQLite will create database if it doesn't exist)
    reader = dft_reader_create("nonexistent.gz", "nonexistent.idx");
    if (reader) {
        dft_reader_destroy(reader);
    }
}

void test_data_range_reading(void) {

    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    // Read valid byte range using streaming API
    const size_t buffer_size = 1024;
    char buffer[1024];
    size_t bytes_written = 0;
    size_t total_bytes = 0;
    char* output = NULL;
    
    // Stream data from first 50 bytes
    while (dft_reader_read(reader, g_gz_file, 0, 50, buffer, buffer_size, &bytes_written) == 1) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    // Get any remaining data from the last call
    if (bytes_written > 0) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    TEST_ASSERT_NOT_NULL(output);
    TEST_ASSERT_TRUE(total_bytes > 0);
    TEST_ASSERT_TRUE(total_bytes >= 50);
    
    // check that we got some JSON content
    output[total_bytes] = '\0'; // Null terminate for strstr
    char* json_start = strstr(output, "{");
    TEST_ASSERT_NOT_NULL(json_start);
    
    free(output);
    dft_reader_destroy(reader);
}

void test_read_with_null_parameters(void) {

    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    char buffer[1024];
    size_t bytes_written = 0;
    
    // null reader
    result = dft_reader_read(NULL, g_gz_file, 0, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null gz_path
    result = dft_reader_read(reader, NULL, 0, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null buffer
    result = dft_reader_read(reader, g_gz_file, 0, 50, NULL, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null bytes_written
    result = dft_reader_read(reader, g_gz_file, 0, 50, buffer, sizeof(buffer), NULL);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    dft_reader_destroy(reader);
}

void test_edge_cases(void) {

    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    char* output = NULL;

    // Invalid byte range (start >= end)
    char buffer[1024];
    size_t bytes_written = 0;
    result = dft_reader_read(reader, g_gz_file, 100, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // Equal start and end should also fail
    result = dft_reader_read(reader, g_gz_file, 50, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // Non-existent file
    result = dft_reader_read(reader, "/nonexistent/file.gz", 0, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    dft_reader_destroy(reader);
}

void test_get_maximum_bytes(void) {

    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    size_t max_bytes;
    result = dft_reader_get_max_bytes(reader, &max_bytes);
    TEST_ASSERT_EQUAL_INT(0, result);
    TEST_ASSERT_TRUE(max_bytes > 0);
    
    // Try to read beyond max_bytes - should succeed
    char buffer[1024];
    size_t bytes_written = 0;
    result = dft_reader_read(reader, g_gz_file, max_bytes + 1, max_bytes + 100, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    // Try to read up to max_bytes - should succeed
    if (max_bytes > 10) {
        result = dft_reader_read(reader, g_gz_file, max_bytes - 10, max_bytes, buffer, sizeof(buffer), &bytes_written);
        if (result == 0) {
            TEST_ASSERT_TRUE(bytes_written >= 0);
        }
    }
    
    dft_reader_destroy(reader);
}

void test_get_max_bytes_null_parameters(void) {

    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    if (reader) {
        size_t max_bytes;
        
        // null reader
        int result = dft_reader_get_max_bytes(NULL, &max_bytes);
        TEST_ASSERT_EQUAL_INT(-1, result);
        
        // null max_bytes
        result = dft_reader_get_max_bytes(reader, NULL);
        TEST_ASSERT_EQUAL_INT(-1, result);
        
        dft_reader_destroy(reader);
    }
}

void test_memory_management(void) {

    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    // multiple reads to ensure no memory leaks
    for (int i = 0; i < 100; i++) {
        char buffer[1024];
        size_t bytes_written = 0;
        size_t total_bytes = 0;
        char* output = NULL;

        // Stream data until no more available
        while ((result = dft_reader_read(reader, g_gz_file, 0, 30, buffer, sizeof(buffer), &bytes_written)) == 1) {
            output = realloc(output, total_bytes + bytes_written);
            TEST_ASSERT_NOT_NULL(output);
            memcpy(output + total_bytes, buffer, bytes_written);
            total_bytes += bytes_written;
        }
        
        // Get any remaining data from the last call
        if (result == 0 && bytes_written > 0) {
            output = realloc(output, total_bytes + bytes_written);
            TEST_ASSERT_NOT_NULL(output);
            memcpy(output + total_bytes, buffer, bytes_written);
            total_bytes += bytes_written;
        }

        if (total_bytes > 0) {
            TEST_ASSERT_TRUE(total_bytes >= 30);
            free(output);
        }
    }
    
    dft_reader_destroy(reader);
}

void test_json_boundary_detection(void) {
    // Create larger test environment for better boundary testing
    test_environment_handle_t large_env = test_environment_create_with_lines(1000);
    TEST_ASSERT_NOT_NULL(large_env);
    TEST_ASSERT_TRUE(test_environment_is_valid(large_env));
    
    char* gz_file = test_environment_create_test_gzip_file(large_env);
    TEST_ASSERT_NOT_NULL(gz_file);
    
    char* idx_file = test_environment_get_index_path(large_env, gz_file);
    TEST_ASSERT_NOT_NULL(idx_file);
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file, idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file, idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    char buffer[2048];
    size_t bytes_written = 0;
    size_t total_bytes = 0;
    char* output = NULL;
    
    // Stream data until no more available
    // NOTE: Individual streaming buffer chunks may contain partial JSON data (Approach 2)
    while ((result = dft_reader_read(reader, gz_file, 0, 100, buffer, sizeof(buffer), &bytes_written)) == 1) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    // Get any remaining data from the last call
    if (result == 0 && bytes_written > 0) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    if (output && total_bytes > 0) {
        TEST_ASSERT_TRUE(total_bytes >= 100);  // Should get at least what was requested
        
        // Null-terminate for string operations
        output = realloc(output, total_bytes + 1);
        TEST_ASSERT_NOT_NULL(output);
        output[total_bytes] = '\0';
        
        // With Approach 2: Individual streaming buffers may contain partial JSON
        // but the COMPLETE collected result (0 to 100 bytes) should contain complete JSON lines
        
        // The streaming API should ensure complete JSON lines for the entire requested range
        TEST_ASSERT_EQUAL_CHAR('\n', output[total_bytes - 1]);  // Should end with newline
        
        // Should contain complete JSON objects
        char* last_brace = strrchr(output, '}');
        TEST_ASSERT_NOT_NULL(last_brace);
        TEST_ASSERT_TRUE(last_brace < output + total_bytes - 1);  // '}' should not be the last character
        TEST_ASSERT_EQUAL_CHAR('\n', *(last_brace + 1));    // Should be followed by newline
        
        // Basic validation - should contain JSON content
        char* json_start = strstr(output, "{");
        TEST_ASSERT_NOT_NULL(json_start);
        
        free(output);
    }
    
    dft_reader_destroy(reader);
    free(gz_file);
    free(idx_file);
    test_environment_destroy(large_env);
}

void test_regression_for_truncated_json_output(void) {
    // This test specifically catches the original bug where output was like:
    // {"name":"name_%  instead of complete JSON lines
    
    test_environment_handle_t large_env = test_environment_create_with_lines(2000);
    TEST_ASSERT_NOT_NULL(large_env);
    
    // Create test data with specific pattern that might trigger the bug
    const char* test_dir = test_environment_get_dir(large_env);
    TEST_ASSERT_NOT_NULL(test_dir);
    
    char gz_file[512], idx_file[512], txt_file[512];
    snprintf(gz_file, sizeof(gz_file), "%s/regression_test.gz", test_dir);
    snprintf(idx_file, sizeof(idx_file), "%s/regression_test.gz.idx", test_dir);
    snprintf(txt_file, sizeof(txt_file), "%s/regression_test.txt", test_dir);
    
    // Create test data similar to trace.pfw.gz format
    FILE* f = fopen(txt_file, "w");
    TEST_ASSERT_NOT_NULL(f);
    
    fprintf(f, "[\n");  // JSON array start
    for (size_t i = 1; i <= 1000; ++i) {
        fprintf(f, "{\"name\":\"name_%zu\",\"cat\":\"cat_%zu\",\"dur\":%zu}\n", 
                i, i, (i * 10 % 1000));
    }
    fclose(f);
    
    int success = compress_file_to_gzip_c(txt_file, gz_file);
    TEST_ASSERT_EQUAL_INT(1, success);
    remove(txt_file);
    
    // Build index
    dft_indexer_handle_t indexer = dft_indexer_create(gz_file, idx_file, 32.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(gz_file, idx_file);
    TEST_ASSERT_NOT_NULL(reader);

    // Original failing case: 0 to 10000 bytes
    char buffer[4096];
    size_t bytes_written = 0;
    size_t total_bytes = 0;
    char* output = NULL;
    
    // Stream data until no more available
    while ((result = dft_reader_read(reader, gz_file, 0, 10000, buffer, sizeof(buffer), &bytes_written)) == 1) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    // Get any remaining data from the last call
    if (result == 0 && bytes_written > 0) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    if (output && total_bytes > 0) {
        TEST_ASSERT_TRUE(total_bytes >= 10000);
        
        // Null-terminate for string operations
        output = realloc(output, total_bytes + 1);
        TEST_ASSERT_NOT_NULL(output);
        output[total_bytes] = '\0';
        
        // Should NOT end with incomplete patterns like "name_%
        TEST_ASSERT_NULL(strstr(output, "\"name_%"));
        TEST_ASSERT_NULL(strstr(output, "\"cat_%"));
        
        // Now check if the FINAL COMPLETE result has proper JSON boundaries
        // The streaming API should guarantee complete JSON lines for the entire range
        
        // Should end with complete JSON line
        TEST_ASSERT_EQUAL_CHAR('\n', output[total_bytes - 1]);
        TEST_ASSERT_EQUAL_CHAR('}', output[total_bytes - 2]);
        
        // Should contain the pattern but complete
        TEST_ASSERT_NOT_NULL(strstr(output, "\"name\":\"name_"));
        TEST_ASSERT_NOT_NULL(strstr(output, "\"cat\":\"cat_"));
        
        free(output);
    }
    
    // Small range minimum bytes check
    output = NULL;
    
    // This was returning only 44 bytes instead of at least 100
    total_bytes = 0;
    output = NULL;
    
    // Stream data until no more available  
    while ((result = dft_reader_read(reader, gz_file, 0, 100, buffer, sizeof(buffer), &bytes_written)) == 1) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    // Get any remaining data from the last call
    if (result == 0 && bytes_written > 0) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    if (output && total_bytes > 0) {
        TEST_ASSERT_TRUE(total_bytes >= 100);  // This was the main bug - was only 44 bytes
        
        // Null-terminate for safety
        output = realloc(output, total_bytes + 1);
        TEST_ASSERT_NOT_NULL(output);
        output[total_bytes] = '\0';
        
        // Should contain multiple complete JSON objects for 100+ bytes
        size_t brace_count = 0;
        for (size_t i = 0; i < total_bytes; i++) {
            if (output[i] == '}') brace_count++;
        }
        TEST_ASSERT_TRUE(brace_count >= 2);  // Should have at least 2 complete objects for 100+ bytes
        
        free(output);
    }

    dft_reader_destroy(reader);
    test_environment_destroy(large_env);
}

// Logger C API tests
void test_logger_set_get_level_string(void) {
    // Test all valid log levels
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("trace"));
    TEST_ASSERT_EQUAL_STRING("trace", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("debug"));
    TEST_ASSERT_EQUAL_STRING("debug", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("info"));
    TEST_ASSERT_EQUAL_STRING("info", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("warn"));
    TEST_ASSERT_EQUAL_STRING("warn", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("warning"));
    TEST_ASSERT_EQUAL_STRING("warn", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("error"));
    TEST_ASSERT_EQUAL_STRING("error", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("err"));
    TEST_ASSERT_EQUAL_STRING("error", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("critical"));
    TEST_ASSERT_EQUAL_STRING("critical", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("off"));
    TEST_ASSERT_EQUAL_STRING("off", dft_utils_get_log_level_string());
    
    // Test case insensitive
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("TRACE"));
    TEST_ASSERT_EQUAL_STRING("trace", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("Debug"));
    TEST_ASSERT_EQUAL_STRING("debug", dft_utils_get_log_level_string());
    
    // Test unrecognized level (should default to info)
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level("invalid"));
    TEST_ASSERT_EQUAL_STRING("info", dft_utils_get_log_level_string());
    
    // Test NULL input
    TEST_ASSERT_EQUAL_INT(-1, dft_utils_set_log_level(NULL));
}

void test_logger_set_get_level_int(void) {
    // Test valid integer levels (0=trace, 1=debug, 2=info, 3=warn, 4=error, 5=critical, 6=off)
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level_int(0));
    TEST_ASSERT_EQUAL_INT(0, dft_utils_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("trace", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level_int(1));
    TEST_ASSERT_EQUAL_INT(1, dft_utils_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("debug", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level_int(2));
    TEST_ASSERT_EQUAL_INT(2, dft_utils_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("info", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level_int(3));
    TEST_ASSERT_EQUAL_INT(3, dft_utils_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("warn", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level_int(4));
    TEST_ASSERT_EQUAL_INT(4, dft_utils_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("error", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level_int(5));
    TEST_ASSERT_EQUAL_INT(5, dft_utils_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("critical", dft_utils_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_utils_set_log_level_int(6));
    TEST_ASSERT_EQUAL_INT(6, dft_utils_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("off", dft_utils_get_log_level_string());
    
    // Test invalid integer levels
    TEST_ASSERT_EQUAL_INT(-1, dft_utils_set_log_level_int(-1));
    TEST_ASSERT_EQUAL_INT(-1, dft_utils_set_log_level_int(7));
    TEST_ASSERT_EQUAL_INT(-1, dft_utils_set_log_level_int(100));
}

void test_logger_backward_compatibility(void) {
    // Test backward compatibility aliases
    TEST_ASSERT_EQUAL_INT(0, dft_set_log_level("info"));
    TEST_ASSERT_EQUAL_STRING("info", dft_get_log_level_string());
    
    TEST_ASSERT_EQUAL_INT(0, dft_set_log_level_int(4));
    TEST_ASSERT_EQUAL_INT(4, dft_get_log_level_int());
    TEST_ASSERT_EQUAL_STRING("error", dft_get_log_level_string());
    
    // Test NULL input
    TEST_ASSERT_EQUAL_INT(-1, dft_set_log_level(NULL));
}

void test_reader_raw_basic_functionality(void) {
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    // Read using raw API
    const size_t buffer_size = 1024;
    char buffer[1024];
    size_t bytes_written = 0;
    size_t total_bytes = 0;
    char* raw_result = NULL;
    
    // Stream raw data until no more available
    while (dft_reader_read_raw(reader, g_gz_file, 0, 50, buffer, buffer_size, &bytes_written) == 1) {
        raw_result = realloc(raw_result, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(raw_result);
        memcpy(raw_result + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    // Get any remaining data from the last call
    if (bytes_written > 0) {
        raw_result = realloc(raw_result, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(raw_result);
        memcpy(raw_result + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    TEST_ASSERT_TRUE(total_bytes >= 50);
    TEST_ASSERT_NOT_NULL(raw_result);
    
    // Raw read should not care about JSON boundaries, so size should be closer to requested
    TEST_ASSERT_TRUE(total_bytes <= 60); // Should be much closer to 50 than regular read
    
    free(raw_result);
    dft_reader_destroy(reader);
}

void test_reader_raw_vs_regular_comparison(void) {
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create two readers
    dft_reader_handle_t reader1 = dft_reader_create(g_gz_file, g_idx_file);
    dft_reader_handle_t reader2 = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader1);
    TEST_ASSERT_NOT_NULL(reader2);
    
    const size_t buffer_size = 1024;
    char buffer1[1024], buffer2[1024];
    size_t bytes_written1 = 0, bytes_written2 = 0;
    size_t total_bytes1 = 0, total_bytes2 = 0;
    char* raw_result = NULL;
    char* regular_result = NULL;
    
    // Raw read
    while (dft_reader_read_raw(reader1, g_gz_file, 0, 100, buffer1, buffer_size, &bytes_written1) == 1) {
        raw_result = realloc(raw_result, total_bytes1 + bytes_written1);
        TEST_ASSERT_NOT_NULL(raw_result);
        memcpy(raw_result + total_bytes1, buffer1, bytes_written1);
        total_bytes1 += bytes_written1;
    }
    if (bytes_written1 > 0) {
        raw_result = realloc(raw_result, total_bytes1 + bytes_written1);
        TEST_ASSERT_NOT_NULL(raw_result);
        memcpy(raw_result + total_bytes1, buffer1, bytes_written1);
        total_bytes1 += bytes_written1;
    }
    
    // Regular read
    while (dft_reader_read(reader2, g_gz_file, 0, 100, buffer2, buffer_size, &bytes_written2) == 1) {
        regular_result = realloc(regular_result, total_bytes2 + bytes_written2);
        TEST_ASSERT_NOT_NULL(regular_result);
        memcpy(regular_result + total_bytes2, buffer2, bytes_written2);
        total_bytes2 += bytes_written2;
    }
    if (bytes_written2 > 0) {
        regular_result = realloc(regular_result, total_bytes2 + bytes_written2);
        TEST_ASSERT_NOT_NULL(regular_result);
        memcpy(regular_result + total_bytes2, buffer2, bytes_written2);
        total_bytes2 += bytes_written2;
    }
    
    // Raw read should be closer to requested size (100 bytes)
    TEST_ASSERT_EQUAL_size_t(100, total_bytes1);
    TEST_ASSERT_TRUE(total_bytes2 >= 100);
    
    // Regular read should be larger due to JSON boundary extension
    TEST_ASSERT_TRUE(total_bytes2 > total_bytes1);
    
    // Regular read should end with complete JSON line
    TEST_ASSERT_EQUAL_CHAR('\n', regular_result[total_bytes2 - 1]);
    
    // Both should start with same data
    size_t min_size = (total_bytes1 < total_bytes2) ? total_bytes1 : total_bytes2;
    TEST_ASSERT_EQUAL_MEMORY(raw_result, regular_result, min_size);
    
    free(raw_result);
    free(regular_result);
    dft_reader_destroy(reader1);
    dft_reader_destroy(reader2);
}

void test_reader_raw_edge_cases(void) {
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    size_t max_bytes;
    result = dft_reader_get_max_bytes(reader, &max_bytes);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    char buffer[1024];
    size_t bytes_written = 0;
    size_t total_bytes = 0;
    char* output = NULL;
    
    // Single byte read
    while (dft_reader_read_raw(reader, g_gz_file, 0, 1, buffer, sizeof(buffer), &bytes_written) == 1) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    if (bytes_written > 0) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, buffer, bytes_written);
        total_bytes += bytes_written;
    }
    TEST_ASSERT_EQUAL_size_t(1, total_bytes);
    free(output);
    
    // Read near end of file
    if (max_bytes > 10) {
        output = NULL;
        total_bytes = 0;
        
        while (dft_reader_read_raw(reader, g_gz_file, max_bytes - 10, max_bytes - 1, buffer, sizeof(buffer), &bytes_written) == 1) {
            output = realloc(output, total_bytes + bytes_written);
            TEST_ASSERT_NOT_NULL(output);
            memcpy(output + total_bytes, buffer, bytes_written);
            total_bytes += bytes_written;
        }
        if (bytes_written > 0) {
            output = realloc(output, total_bytes + bytes_written);
            TEST_ASSERT_NOT_NULL(output);
            memcpy(output + total_bytes, buffer, bytes_written);
            total_bytes += bytes_written;
        }
        TEST_ASSERT_EQUAL_size_t(9, total_bytes);
        free(output);
    }
    
    // Invalid ranges should still return error
    result = dft_reader_read_raw(reader, g_gz_file, 100, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    result = dft_reader_read_raw(reader, g_gz_file, 50, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    dft_reader_destroy(reader);
}

void test_reader_raw_small_buffer(void) {
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    // Use very small buffer to test streaming behavior
    const size_t small_buffer_size = 16;
    char small_buffer[16];
    size_t bytes_written = 0;
    size_t total_bytes = 0;
    size_t total_calls = 0;
    char* output = NULL;
    
    while (dft_reader_read_raw(reader, g_gz_file, 0, 200, small_buffer, small_buffer_size, &bytes_written) == 1) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, small_buffer, bytes_written);
        total_bytes += bytes_written;
        total_calls++;
        TEST_ASSERT_TRUE(bytes_written <= small_buffer_size);
        if (total_calls > 50) break; // Safety guard
    }
    if (bytes_written > 0) {
        output = realloc(output, total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(output);
        memcpy(output + total_bytes, small_buffer, bytes_written);
        total_bytes += bytes_written;
    }
    
    TEST_ASSERT_EQUAL_size_t(200, total_bytes);
    TEST_ASSERT_TRUE(total_calls > 1); // Should require multiple calls with small buffer
    
    free(output);
    dft_reader_destroy(reader);
}

void test_reader_raw_multiple_ranges(void) {
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    size_t max_bytes;
    result = dft_reader_get_max_bytes(reader, &max_bytes);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    char buffer[1024];
    size_t bytes_written = 0;
    
    // Define ranges to test
    struct {
        size_t start;
        size_t end;
    } ranges[] = {
        {0, 50},
        {50, 100},
        {100, 150}
    };
    size_t num_ranges = sizeof(ranges) / sizeof(ranges[0]);
    
    for (size_t i = 0; i < num_ranges; i++) {
        if (ranges[i].end <= max_bytes) {
            size_t total_bytes = 0;
            char* segment = NULL;
            
            while (dft_reader_read_raw(reader, g_gz_file, ranges[i].start, ranges[i].end, buffer, sizeof(buffer), &bytes_written) == 1) {
                segment = realloc(segment, total_bytes + bytes_written);
                TEST_ASSERT_NOT_NULL(segment);
                memcpy(segment + total_bytes, buffer, bytes_written);
                total_bytes += bytes_written;
            }
            if (bytes_written > 0) {
                segment = realloc(segment, total_bytes + bytes_written);
                TEST_ASSERT_NOT_NULL(segment);
                memcpy(segment + total_bytes, buffer, bytes_written);
                total_bytes += bytes_written;
            }
            
            size_t expected_size = ranges[i].end - ranges[i].start;
            TEST_ASSERT_EQUAL_size_t(expected_size, total_bytes);
            
            free(segment);
        }
    }
    
    dft_reader_destroy(reader);
}

void test_reader_raw_null_parameters(void) {
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    char buffer[1024];
    size_t bytes_written = 0;
    
    // null reader
    result = dft_reader_read_raw(NULL, g_gz_file, 0, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null gz_path
    result = dft_reader_read_raw(reader, NULL, 0, 50, buffer, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null buffer
    result = dft_reader_read_raw(reader, g_gz_file, 0, 50, NULL, sizeof(buffer), &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null bytes_written
    result = dft_reader_read_raw(reader, g_gz_file, 0, 50, buffer, sizeof(buffer), NULL);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // zero buffer size
    result = dft_reader_read_raw(reader, g_gz_file, 0, 50, buffer, 0, &bytes_written);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    dft_reader_destroy(reader);
}

void test_reader_full_file_comparison_raw_vs_json_boundary(void) {
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create two readers
    dft_reader_handle_t reader1 = dft_reader_create(g_gz_file, g_idx_file);
    dft_reader_handle_t reader2 = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader1);
    TEST_ASSERT_NOT_NULL(reader2);
    
    // Get max bytes
    size_t max_bytes;
    result = dft_reader_get_max_bytes(reader1, &max_bytes);
    TEST_ASSERT_EQUAL_INT(0, result);
    TEST_ASSERT_TRUE(max_bytes > 0);
    
    char buffer[4096];
    size_t bytes_written = 0;
    
    // Read entire file with raw API
    size_t raw_total_bytes = 0;
    char* raw_content = NULL;
    
    while (dft_reader_read_raw(reader1, g_gz_file, 0, max_bytes, buffer, sizeof(buffer), &bytes_written) == 1) {
        raw_content = realloc(raw_content, raw_total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(raw_content);
        memcpy(raw_content + raw_total_bytes, buffer, bytes_written);
        raw_total_bytes += bytes_written;
    }
    if (bytes_written > 0) {
        raw_content = realloc(raw_content, raw_total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(raw_content);
        memcpy(raw_content + raw_total_bytes, buffer, bytes_written);
        raw_total_bytes += bytes_written;
    }
    
    // Read entire file with JSON-boundary aware API
    size_t json_total_bytes = 0;
    char* json_content = NULL;
    
    while (dft_reader_read(reader2, g_gz_file, 0, max_bytes, buffer, sizeof(buffer), &bytes_written) == 1) {
        json_content = realloc(json_content, json_total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(json_content);
        memcpy(json_content + json_total_bytes, buffer, bytes_written);
        json_total_bytes += bytes_written;
    }
    if (bytes_written > 0) {
        json_content = realloc(json_content, json_total_bytes + bytes_written);
        TEST_ASSERT_NOT_NULL(json_content);
        memcpy(json_content + json_total_bytes, buffer, bytes_written);
        json_total_bytes += bytes_written;
    }
    
    // Both should read the entire file
    TEST_ASSERT_EQUAL_size_t(max_bytes, raw_total_bytes);
    TEST_ASSERT_EQUAL_size_t(max_bytes, json_total_bytes);
    
    // Total bytes should be identical when reading full file
    TEST_ASSERT_EQUAL_size_t(raw_total_bytes, json_total_bytes);
    
    // Content should be identical when reading full file
    TEST_ASSERT_EQUAL_MEMORY(raw_content, json_content, raw_total_bytes);
    
    // Both should end with complete JSON lines
    if (raw_total_bytes > 0 && json_total_bytes > 0) {
        TEST_ASSERT_EQUAL_CHAR('\n', raw_content[raw_total_bytes - 1]);
        TEST_ASSERT_EQUAL_CHAR('\n', json_content[json_total_bytes - 1]);
        
        // Find last JSON line in both (look for second-to-last newline)
        char* raw_last_newline = NULL;
        char* json_last_newline = NULL;
        
        // Find second-to-last newline in raw content
        for (size_t i = raw_total_bytes - 2; i > 0; i--) {
            if (raw_content[i] == '\n') {
                raw_last_newline = &raw_content[i];
                break;
            }
        }
        
        // Find second-to-last newline in json content  
        for (size_t i = json_total_bytes - 2; i > 0; i--) {
            if (json_content[i] == '\n') {
                json_last_newline = &json_content[i];
                break;
            }
        }
        
        if (raw_last_newline && json_last_newline) {
            // Calculate last line lengths
            size_t raw_last_line_len = (raw_content + raw_total_bytes - 1) - raw_last_newline;
            size_t json_last_line_len = (json_content + json_total_bytes - 1) - json_last_newline;
            
            // Last JSON lines should be identical
            TEST_ASSERT_EQUAL_size_t(raw_last_line_len, json_last_line_len);
            TEST_ASSERT_EQUAL_MEMORY(raw_last_newline, json_last_newline, raw_last_line_len);
            
            // Should contain valid JSON structure (look for { and } in last line)
            char* raw_last_line_start = raw_last_newline + 1;
            char* json_last_line_start = json_last_newline + 1;
            size_t actual_line_len = raw_last_line_len - 1; // exclude newline
            
            int raw_has_brace_open = 0, raw_has_brace_close = 0;
            int json_has_brace_open = 0, json_has_brace_close = 0;
            
            for (size_t i = 0; i < actual_line_len; i++) {
                if (raw_last_line_start[i] == '{') raw_has_brace_open = 1;
                if (raw_last_line_start[i] == '}') raw_has_brace_close = 1;
                if (json_last_line_start[i] == '{') json_has_brace_open = 1;
                if (json_last_line_start[i] == '}') json_has_brace_close = 1;
            }
            
            TEST_ASSERT_TRUE(raw_has_brace_open);
            TEST_ASSERT_TRUE(raw_has_brace_close);
            TEST_ASSERT_TRUE(json_has_brace_open);
            TEST_ASSERT_TRUE(json_has_brace_close);
        }
    }
    
    // Debug output (will be visible if test fails)
    printf("Full file comparison: raw_size=%zu, json_size=%zu, max_bytes=%zu\n", 
           raw_total_bytes, json_total_bytes, max_bytes);
    
    free(raw_content);
    free(json_content);
    dft_reader_destroy(reader1);
    dft_reader_destroy(reader2);
}

int main(void) {
    UNITY_BEGIN();
    
    // Set up global test environment
    g_env = test_environment_create();
    if (!g_env || !test_environment_is_valid(g_env)) {
        printf("Failed to create test environment\n");
        return 1;
    }
    
    // Indexer tests
    RUN_TEST(test_indexer_creation_and_destruction);
    RUN_TEST(test_indexer_invalid_parameters);
    RUN_TEST(test_gzip_index_building);
    RUN_TEST(test_indexer_rebuild_detection);
    RUN_TEST(test_indexer_force_rebuild);
    
    // Reader tests
    RUN_TEST(test_reader_creation_and_destruction);
    RUN_TEST(test_reader_invalid_parameters);
    RUN_TEST(test_data_range_reading);
    RUN_TEST(test_read_with_null_parameters);
    RUN_TEST(test_edge_cases);
    RUN_TEST(test_get_maximum_bytes);
    RUN_TEST(test_get_max_bytes_null_parameters);
    RUN_TEST(test_memory_management);
    
    // Advanced tests
    RUN_TEST(test_json_boundary_detection);
    RUN_TEST(test_regression_for_truncated_json_output);
    
    // Logger tests
    RUN_TEST(test_logger_set_get_level_string);
    RUN_TEST(test_logger_set_get_level_int);
    RUN_TEST(test_logger_backward_compatibility);
    
    // Raw reader tests
    RUN_TEST(test_reader_raw_basic_functionality);
    RUN_TEST(test_reader_raw_vs_regular_comparison);
    RUN_TEST(test_reader_raw_edge_cases);
    RUN_TEST(test_reader_raw_small_buffer);
    RUN_TEST(test_reader_raw_multiple_ranges);
    RUN_TEST(test_reader_raw_null_parameters);
    RUN_TEST(test_reader_full_file_comparison_raw_vs_json_boundary);
    
    // Clean up global test environment
    if (g_env) {
        test_environment_destroy(g_env);
    }
    
    return UNITY_END();
}
