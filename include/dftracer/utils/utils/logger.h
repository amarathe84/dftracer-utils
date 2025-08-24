#ifndef DFTRACER_UTILS_UTILS_LOGGER_H
#define DFTRACER_UTILS_UTILS_LOGGER_H

#ifdef __cplusplus
extern "C" {
#endif
/**
 * Set the global spdlog log level programmatically
 * @param level_str String representation of log level (case insensitive)
 *                  Valid values: "trace", "debug", "info", "warn"/"warning",
 *                  "err"/"error", "critical", "off"
 * @return 0 on success, -1 if level_str is invalid (though it defaults to info)
 */
int dft_utils_set_log_level(const char *level_str);

/**
 * Set the global spdlog log level using integer level
 * @param level Integer representation of log level:
 *              0=trace, 1=debug, 2=info, 3=warn, 4=error, 5=critical, 6=off
 * @return 0 on success, -1 if level is out of range
 */
int dft_utils_set_log_level_int(int level);

/**
 * Get the current global spdlog log level as a string
 * @return String representation of current log level (pointer to static string)
 */
const char *dft_utils_get_log_level_string(void);

/**
 * Get the current global spdlog log level as an integer
 * @return Integer representation of current log level (0-6)
 */
int dft_utils_get_log_level_int(void);

#ifdef __cplusplus
}

#include <string>

namespace dftracer::utils::logger {
/**
 * Set the log level for the utils module
 * @param level_str String representation of log level
 * @return 0 on success, -1 on failure
 */
int set_log_level(const std::string &level_str);

/**
 * Set the log level for the utils module
 * @param level Integer representation of log level
 * @return 0 on success, -1 on failure
 */
int set_log_level_int(int level);

/**
 * Get the log level for the utils module
 * @return String representation of log level
 */
std::string get_log_level_string();

/**
 * Get the log level for the utils module
 * @return Integer representation of log level
 */
int get_log_level_int();
}  // namespace dftracer::utils::logger
#endif

#endif  // DFTRACER_UTILS_UTILS_LOGGER_H
