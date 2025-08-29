#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/helpers.h>
#include <dftracer/utils/utils/filesystem.h>
#include <xxhash.h>

// Platform-specific includes for file stats
#ifdef _WIN32
#include <sys/stat.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <vector>

std::string get_logical_path(const std::string &path) {
    auto fs_path = fs::path(path);
    return fs_path.filename().string();
}

time_t get_file_modification_time(const std::string &file_path) {
#if defined(DFTRACER_UTILS_USE_STD_FS)
    // Use std::filesystem when available and working
    auto ftime = fs::last_write_time(file_path);
    auto sctp =
        std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            ftime - fs::file_time_type::clock::now() +
            std::chrono::system_clock::now());
    return std::chrono::system_clock::to_time_t(sctp);
#else
    // Fallback to platform-specific stat
#ifdef _WIN32
    struct _stat64 st;
    if (_stat64(file_path.c_str(), &st) == 0) {
        return st.st_mtime;
    }
#else
    struct stat st;
    if (stat(file_path.c_str(), &st) == 0) {
        return st.st_mtime;
    }
#endif
    return 0;
#endif
}

std::string calculate_file_hash(const std::string &file_path) {
    // Use much larger buffer for better I/O performance on large files
    constexpr size_t HASH_BUFFER_SIZE = 1024 * 1024;  // 1MB buffer

    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        DFTRACER_UTILS_LOG_ERROR("Cannot open file for SHA256 calculation: %s",
                                 file_path.c_str());
        return "";
    }

    XXH3_state_t *state = XXH3_createState();
    if (!state) {
        DFTRACER_UTILS_LOG_ERROR("Failed to create XXH3 state");
        return "";
    }
    const XXH64_hash_t seed = 0;
    if (XXH3_64bits_reset_withSeed(state, seed) == XXH_ERROR) {
        DFTRACER_UTILS_LOG_ERROR("Failed to reset XXH3 state");
        XXH3_freeState(state);
        return "";
    }

    // Pre-allocate larger buffer
    std::vector<unsigned char> buffer(HASH_BUFFER_SIZE);

    while (file.read(reinterpret_cast<char *>(buffer.data()),
                     static_cast<std::streamsize>(buffer.size())) ||
           file.gcount() > 0) {
        XXH3_64bits_update(state, buffer.data(),
                           static_cast<size_t>(file.gcount()));
    }

    XXH64_hash_t hash = XXH3_64bits_digest(state);
    XXH3_freeState(state);

    // Convert hash to hex string
    std::stringstream ss;
    ss << std::hex << hash;
    return ss.str();
}

std::uint64_t file_size_bytes(const std::string &path) {
    struct stat st {};
    if (stat(path.c_str(), &st) == 0) {
#if defined(_WIN32)
        if ((st.st_mode & _S_IFREG) != 0)
            return static_cast<uint64_t>(st.st_size);
#else
        if (S_ISREG(st.st_mode)) return static_cast<uint64_t>(st.st_size);
#endif
    }

    FILE *fp = std::fopen(path.c_str(), "rb");
    if (!fp) return 0;
    if (fseeko(fp, 0, SEEK_END) != 0) {
        std::fclose(fp);
        return 0;
    }
    const auto pos = ftello(fp);
    std::fclose(fp);
    if (pos < 0) return 0;
    return static_cast<uint64_t>(pos);
}

bool index_exists_and_valid(const std::string &idx_path) {
    return fs::exists(idx_path) && fs::is_regular_file(idx_path);
}
