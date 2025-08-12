#ifndef __DFTRACER_UTILS_TEST_TESTING_UTILITIES_H
#define __DFTRACER_UTILS_TEST_TESTING_UTILITIES_H

#include <string>

namespace dft_utils_test {
bool compress_file_to_gzip(const std::string& input_file, const std::string& output_file);
class TestEnvironment {
public:
    TestEnvironment(): TestEnvironment(100) {}
    TestEnvironment(size_t lines);
    TestEnvironment(const TestEnvironment&) = delete;
    TestEnvironment& operator=(const TestEnvironment&) = delete;
    ~TestEnvironment();
    
    const std::string& get_dir() const;
    bool is_valid() const;
    std::string create_test_gzip_file();
    std::string get_index_path(const std::string& gz_file);
    
private:
    size_t num_lines;
    std::string test_dir;
};
} // namespace dft_utils_test

#endif // __DFTRACER_UTILS_TEST_TESTING_UTILITIES_H
