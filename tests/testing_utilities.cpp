#include "testing_utilities.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <random>
#include <fstream>
#include <vector>
#include <zlib.h>

#include <dft_utils/utils/filesystem.h>

namespace dft_utils_test {
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


TestEnvironment::TestEnvironment(size_t lines): num_lines(lines) {
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

TestEnvironment::~TestEnvironment() {
    if (!test_dir.empty()) {
        fs::remove_all(test_dir);
    }
}

const std::string& TestEnvironment::get_dir() const { return test_dir; }
bool TestEnvironment::is_valid() const { return !test_dir.empty(); }

std::string TestEnvironment::create_test_gzip_file() {
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

std::string TestEnvironment::get_index_path(const std::string& gz_file) {
    return gz_file + ".idx";
}
} // namespace dft_utils_test

// C API implementations
extern "C" {

test_environment_handle_t test_environment_create(void) {
    return test_environment_create_with_lines(100);
}

test_environment_handle_t test_environment_create_with_lines(size_t lines) {
    try {
        auto* env = new dft_utils_test::TestEnvironment(lines);
        if (env->is_valid()) {
            return reinterpret_cast<test_environment_handle_t>(env);
        } else {
            delete env;
            return nullptr;
        }
    } catch (...) {
        return nullptr;
    }
}

void test_environment_destroy(test_environment_handle_t env) {
    if (env) {
        auto* cpp_env = reinterpret_cast<dft_utils_test::TestEnvironment*>(env);
        delete cpp_env;
    }
}

int test_environment_is_valid(test_environment_handle_t env) {
    if (!env) return 0;
    auto* cpp_env = reinterpret_cast<dft_utils_test::TestEnvironment*>(env);
    return cpp_env->is_valid() ? 1 : 0;
}

const char* test_environment_get_dir(test_environment_handle_t env) {
    if (!env) return nullptr;
    auto* cpp_env = reinterpret_cast<dft_utils_test::TestEnvironment*>(env);
    return cpp_env->get_dir().c_str();
}

char* test_environment_create_test_gzip_file(test_environment_handle_t env) {
    if (!env) return nullptr;
    auto* cpp_env = reinterpret_cast<dft_utils_test::TestEnvironment*>(env);
    std::string gz_file = cpp_env->create_test_gzip_file();
    if (gz_file.empty()) {
        return nullptr;
    }
    char* result = static_cast<char*>(malloc(gz_file.length() + 1));
    if (result) {
        strcpy(result, gz_file.c_str());
    }
    return result;
}

char* test_environment_get_index_path(test_environment_handle_t env, const char* gz_file) {
    if (!env || !gz_file) return nullptr;
    auto* cpp_env = reinterpret_cast<dft_utils_test::TestEnvironment*>(env);
    std::string idx_path = cpp_env->get_index_path(gz_file);
    char* result = static_cast<char*>(malloc(idx_path.length() + 1));
    if (result) {
        strcpy(result, idx_path.c_str());
    }
    return result;
}

int compress_file_to_gzip_c(const char* input_file, const char* output_file) {
    if (!input_file || !output_file) return 0;
    try {
        return dft_utils_test::compress_file_to_gzip(input_file, output_file) ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

}
