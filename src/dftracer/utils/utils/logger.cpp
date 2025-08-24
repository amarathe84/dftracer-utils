#include <dftracer/utils/utils/logger.h>
#include <dftracer/utils/common/logging.h>

#include <algorithm>
#include <string>

namespace dftracer::utils::logger {

/**
 * Convert string log level to spdlog level enum
 */
static int current_log_level = 2; // Default to info

static int string_to_log_level_internal(const char *level_str) {
  std::string lower_level = level_str;
  std::transform(lower_level.begin(), lower_level.end(), lower_level.begin(),
                 ::tolower);

  if (lower_level == "trace") return 0;
  if (lower_level == "debug") return 1;
  if (lower_level == "info") return 2;
  if (lower_level == "warn" || lower_level == "warning") return 3;
  if (lower_level == "err" || lower_level == "error") return 4;
  if (lower_level == "critical") return 5;
  if (lower_level == "off") return 6;

  // Default to info if unrecognized
  return 2;
}

int set_log_level(const std::string &level_str) {
  if (level_str.empty()) {
    return -1;
  }

  current_log_level = string_to_log_level_internal(level_str.c_str());
  return 0;
}

int set_log_level_int(int level) {
  if (level < 0 || level > 6) {
    return -1;
  }

  current_log_level = level;
  return 0;
}

std::string get_log_level_string() {
  switch (current_log_level) {
    case 0:
      return "trace";
    case 1:
      return "debug";
    case 2:
      return "info";
    case 3:
      return "warn";
    case 4:
      return "error";
    case 5:
      return "critical";
    case 6:
      return "off";
    default:
      return "info";
  }
}

int get_log_level_int() { return current_log_level; }

}  // namespace dftracer::utils::logger

// ==============================================================================
// C API Implementation (wraps C++ implementation)
// ==============================================================================

extern "C" {

int dft_utils_set_log_level(const char *level_str) {
  if (!level_str) {
    return -1;
  }

  return dftracer::utils::logger::set_log_level(level_str);
}

int dft_utils_set_log_level_int(int level) {
  return dftracer::utils::logger::set_log_level_int(level);
}

const char *dft_utils_get_log_level_string() {
  static std::string level_string;
  level_string = dftracer::utils::logger::get_log_level_string();
  return level_string.c_str();
}

int dft_utils_get_log_level_int() {
  return dftracer::utils::logger::get_log_level_int();
}

}  // extern "C"
