#pragma once

// C++11 compatible filesystem header
// This header provides a portable way to use filesystem across different C++ standards

#if defined(__APPLE__) && __has_include(<filesystem>)
    // macOS with __fs filesystem namespace
    #include <filesystem>
    namespace fs = std::__fs::filesystem;
#elif __cplusplus >= 201703L && __has_include(<filesystem>)
    // C++17 or later with std::filesystem support
    #include <filesystem>
    namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
    // Experimental filesystem (C++14/C++11 with libstdc++)
    #include <experimental/filesystem>
    namespace fs = std::experimental::filesystem;
#else
    // Fallback to gulrak/filesystem for C++11 compatibility
    #include <ghc/filesystem.hpp>
    namespace fs = ghc::filesystem;
#endif

// Additional compatibility aliases for common operations
namespace dft {
    using path = fs::path;
    using file_status = fs::file_status;
    using file_type = fs::file_type;
    using perms = fs::perms;
    
    // Common filesystem operations
    inline bool exists(const fs::path& p) {
        return fs::exists(p);
    }
    
    inline bool is_regular_file(const fs::path& p) {
        return fs::is_regular_file(p);
    }
    
    inline bool is_directory(const fs::path& p) {
        return fs::is_directory(p);
    }
    
    inline std::uintmax_t file_size(const fs::path& p) {
        return fs::file_size(p);
    }
    
    inline fs::path absolute(const fs::path& p) {
        return fs::absolute(p);
    }
    
    inline fs::path canonical(const fs::path& p) {
        return fs::canonical(p);
    }
}
