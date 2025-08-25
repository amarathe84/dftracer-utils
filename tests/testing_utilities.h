#ifndef DFTRACER_UTILS_TESTS_TESTING_UTILITIES_H
#define DFTRACER_UTILS_TESTS_TESTING_UTILITIES_H

#ifdef __cplusplus
#include <string>
extern "C" {
#endif

// C API for testing utilities
typedef struct test_environment* test_environment_handle_t;

/**
 * Create a test environment with default number of lines (100)
 */
test_environment_handle_t test_environment_create(void);

/**
 * Create a test environment with specified number of lines
 */
test_environment_handle_t test_environment_create_with_lines(size_t lines);

/**
 * Destroy a test environment and clean up resources
 */
void test_environment_destroy(test_environment_handle_t env);

/**
 * Check if test environment is valid
 */
int test_environment_is_valid(test_environment_handle_t env);

/**
 * Get the test directory path
 * Returns a pointer to internal string - do not free
 */
const char* test_environment_get_dir(test_environment_handle_t env);

/**
 * Create a test gzip file and return the path
 * Returns allocated string - caller must free
 */
char* test_environment_create_test_gzip_file(test_environment_handle_t env);

/**
 * Get index path for a given gzip file
 * Returns allocated string - caller must free
 */
char* test_environment_get_index_path(test_environment_handle_t env,
                                      const char* gz_file);

/**
 * Compress a file to gzip format
 * Returns 1 on success, 0 on failure
 */
int compress_file_to_gzip_c(const char* input_file, const char* output_file);

size_t mb_to_b(double mb);

#ifdef __cplusplus
}

namespace dft_utils_test {
bool compress_file_to_gzip(const std::string& input_file,
                           const std::string& output_file);
class TestEnvironment {
   public:
    TestEnvironment() : TestEnvironment(100) {}
    TestEnvironment(std::size_t lines);
    TestEnvironment(const TestEnvironment&) = delete;
    TestEnvironment& operator=(const TestEnvironment&) = delete;
    ~TestEnvironment();

    const std::string& get_dir() const;
    bool is_valid() const;
    std::string create_test_gzip_file();
    std::string get_index_path(const std::string& gz_file);

   private:
    std::size_t num_lines;
    std::string test_dir;
};
}  // namespace dft_utils_test
#endif

#endif  // DFTRACER_UTILS_TESTS_TESTING_UTILITIES_H
