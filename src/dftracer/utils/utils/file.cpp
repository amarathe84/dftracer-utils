#include <dftracer/utils/utils/file.h>
#include <dftracer/utils/utils/filesystem.h>

// Platform-specific includes for file stats
#ifdef _WIN32
#include <sys/stat.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

#include <chrono>

namespace dftracer::utils::utils {

time_t get_file_modification_time(const std::string &file_path) {
#if defined(DFT_USE_STD_FS)
  // Use std::filesystem when available and working
  auto ftime = fs::last_write_time(file_path);
  auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
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

}  // namespace dftracer::utils::utils
