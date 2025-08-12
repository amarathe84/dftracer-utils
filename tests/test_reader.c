#include <unity.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <dft_utils/indexer/indexer.h>
#include <dft_utils/reader/reader.h>
#include "testing_utilities.h"

// Global test environment handle
static test_environment_handle_t g_env = NULL;
static char* g_gz_file = NULL;
static char* g_idx_file = NULL;

void setUp(void) {
    // Called before each test
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
}

// Helper function to set up test environment for tests that need it
static void setup_test_environment(void) {
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
}

void test_indexer_creation_and_destruction(void) {
    setup_test_environment();
    
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
    setup_test_environment();
    
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
    test_environment_destroy(test_env);
}

void test_indexer_force_rebuild(void) {
    setup_test_environment();
    
    // Build initial index
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 1.0, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    dft_indexer_destroy(indexer);
    
    // Create indexer with force rebuild
    indexer = dft_indexer_create(g_gz_file, g_idx_file, 1.0, 1);
    TEST_ASSERT_NOT_NULL(indexer);
    
    // Should need rebuild because force is enabled
    int need_rebuild = dft_indexer_need_rebuild(indexer);
    TEST_ASSERT_EQUAL_INT(1, need_rebuild);
    
    dft_indexer_destroy(indexer);
}

void test_reader_creation_and_destruction(void) {
    setup_test_environment();
    
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
    setup_test_environment();
    
    // Build index first
    dft_indexer_handle_t indexer = dft_indexer_create(g_gz_file, g_idx_file, 0.5, 0);
    TEST_ASSERT_NOT_NULL(indexer);
    
    int result = dft_indexer_build(indexer);
    TEST_ASSERT_EQUAL_INT(0, result);
    dft_indexer_destroy(indexer);
    
    // Create reader
    dft_reader_handle_t reader = dft_reader_create(g_gz_file, g_idx_file);
    TEST_ASSERT_NOT_NULL(reader);
    
    // Read valid byte range
    char* output = NULL;
    size_t output_size = 0;
    
    // read first 50 bytes
    result = dft_reader_read_range_bytes(reader, g_gz_file, 0, 50, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    if (result == 0) {
        TEST_ASSERT_NOT_NULL(output);
        TEST_ASSERT_TRUE(output_size > 0);
        TEST_ASSERT_TRUE(output_size >= 50);
        
        // check that we got some JSON content
        char* json_start = strstr(output, "{");
        TEST_ASSERT_NOT_NULL(json_start);
        
        free(output);
    }
    
    dft_reader_destroy(reader);
}

void test_read_with_null_parameters(void) {
    setup_test_environment();
    
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
    size_t output_size = 0;
    
    // null reader
    result = dft_reader_read_range_bytes(NULL, g_gz_file, 0, 50, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null gz_path
    result = dft_reader_read_range_bytes(reader, NULL, 0, 50, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null output
    result = dft_reader_read_range_bytes(reader, g_gz_file, 0, 50, NULL, &output_size);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // null output_size
    result = dft_reader_read_range_bytes(reader, g_gz_file, 0, 50, &output, NULL);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    dft_reader_destroy(reader);
}

void test_read_megabyte_range(void) {
    setup_test_environment();
    
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
    size_t output_size = 0;
    
    // read first 0.001 MB (about 1024 bytes)
    result = dft_reader_read_range_megabytes(reader, g_gz_file, 0.0, 0.001, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    if (result == 0) {
        TEST_ASSERT_NOT_NULL(output);
        TEST_ASSERT_TRUE(output_size > 0);
        
        free(output);
    }
    
    dft_reader_destroy(reader);
}

void test_edge_cases(void) {
    setup_test_environment();
    
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
    size_t output_size = 0;

    // Invalid byte range (start >= end)
    result = dft_reader_read_range_bytes(reader, g_gz_file, 100, 50, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // Equal start and end should also fail
    result = dft_reader_read_range_bytes(reader, g_gz_file, 50, 50, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    // Non-existent file
    result = dft_reader_read_range_bytes(reader, "/nonexistent/file.gz", 0, 50, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(-1, result);
    
    dft_reader_destroy(reader);
}

void test_get_maximum_bytes(void) {
    setup_test_environment();
    
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
    char* output = NULL;
    size_t output_size = 0;
    result = dft_reader_read_range_bytes(reader, g_gz_file, max_bytes + 1, max_bytes + 100, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    // Try to read up to max_bytes - should succeed
    if (max_bytes > 10) {
        result = dft_reader_read_range_bytes(reader, g_gz_file, max_bytes - 10, max_bytes, &output, &output_size);
        if (result == 0 && output) {
            TEST_ASSERT_TRUE(output_size > 0);
            free(output);
        }
    }
    
    dft_reader_destroy(reader);
}

void test_get_max_bytes_null_parameters(void) {
    setup_test_environment();
    
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
    setup_test_environment();
    
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
        char* output = NULL;
        size_t output_size = 0;

        result = dft_reader_read_range_bytes(reader, g_gz_file, 0, 30, &output, &output_size);

        if (result == 0 && output) {
            TEST_ASSERT_TRUE(output_size > 0);
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

    // Small range should provide minimum requested bytes
    char* output = NULL;
    size_t output_size = 0;
    
    // Request 100 bytes - should get AT LEAST 100 bytes due to boundary extension
    result = dft_reader_read_range_bytes(reader, gz_file, 0, 100, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    if (result == 0) {
        TEST_ASSERT_NOT_NULL(output);
        TEST_ASSERT_TRUE(output_size >= 100);  // Should get at least what was requested
        
        // Verify that output ends with complete JSON line
        TEST_ASSERT_EQUAL_CHAR('\n', output[output_size - 1]);  // Should end with newline
        
        // Should contain complete JSON objects
        char* last_brace = strrchr(output, '}');
        TEST_ASSERT_NOT_NULL(last_brace);
        TEST_ASSERT_TRUE(last_brace < output + output_size - 1);  // '}' should not be the last character
        TEST_ASSERT_EQUAL_CHAR('\n', *(last_brace + 1));    // Should be followed by newline
        
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
    char* output = NULL;
    size_t output_size = 0;
    
    result = dft_reader_read_range_bytes(reader, gz_file, 0, 10000, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    if (result == 0) {
        TEST_ASSERT_NOT_NULL(output);
        TEST_ASSERT_TRUE(output_size >= 10000);
        
        // Should NOT end with incomplete patterns like "name_%
        TEST_ASSERT_NULL(strstr(output, "\"name_%"));
        TEST_ASSERT_NULL(strstr(output, "\"cat_%"));
        
        // Should end with complete JSON line
        TEST_ASSERT_EQUAL_CHAR('\n', output[output_size - 1]);
        TEST_ASSERT_EQUAL_CHAR('}', output[output_size - 2]);
        
        // Should contain the pattern but complete
        TEST_ASSERT_NOT_NULL(strstr(output, "\"name\":\"name_"));
        TEST_ASSERT_NOT_NULL(strstr(output, "\"cat\":\"cat_"));
        
        free(output);
    }
    
    // Small range minimum bytes check
    output = NULL;
    output_size = 0;
    
    // This was returning only 44 bytes instead of at least 100
    result = dft_reader_read_range_bytes(reader, gz_file, 0, 100, &output, &output_size);
    TEST_ASSERT_EQUAL_INT(0, result);
    
    if (result == 0) {
        TEST_ASSERT_NOT_NULL(output);
        TEST_ASSERT_TRUE(output_size >= 100);  // This was the main bug - was only 44 bytes
        
        // Should contain multiple complete JSON objects for 100+ bytes
        size_t brace_count = 0;
        for (size_t i = 0; i < output_size; i++) {
            if (output[i] == '}') brace_count++;
        }
        TEST_ASSERT_TRUE(brace_count >= 2);  // Should have at least 2 complete objects for 100+ bytes
        
        free(output);
    }

    dft_reader_destroy(reader);
    test_environment_destroy(large_env);
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
    RUN_TEST(test_read_megabyte_range);
    RUN_TEST(test_edge_cases);
    RUN_TEST(test_get_maximum_bytes);
    RUN_TEST(test_get_max_bytes_null_parameters);
    RUN_TEST(test_memory_management);
    
    // Advanced tests
    RUN_TEST(test_json_boundary_detection);
    RUN_TEST(test_regression_for_truncated_json_output);
    
    // Clean up global test environment
    if (g_env) {
        test_environment_destroy(g_env);
    }
    
    return UNITY_END();
}
