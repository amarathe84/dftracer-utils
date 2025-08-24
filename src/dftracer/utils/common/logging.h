#ifndef DFTRACER_UTILS_COMMON_LOGGING_H
#define DFTRACER_UTILS_COMMON_LOGGING_H

#include <dftracer/utils/common/config.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <string>

namespace dftracer::utils {
inline std::string dftracer_utils_macro_get_time() {
  auto dftracer_utils_ts_millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() %
      1000;
  auto dftracer_utils_ts_t = std::time(0);
  auto now = std::localtime(&dftracer_utils_ts_t);
  char dftracer_utils_ts_time_str[256];
  snprintf(dftracer_utils_ts_time_str, sizeof(dftracer_utils_ts_time_str),
           "%04d-%02d-%02d %02d:%02d:%02d.%lld", now->tm_year + 1900,
           now->tm_mon + 1, now->tm_mday, now->tm_hour, now->tm_min,
           now->tm_sec, dftracer_utils_ts_millis);
  return dftracer_utils_ts_time_str;
}
}  // namespace dftracer::utils

#define DFTRACER_UTILS_NOOP_MACRO \
  do {                            \
  } while (0)

#if defined(DFTRACER_UTILS_LOGGER_CPP_LOGGER)
#include <cpp-logger/clogger.h>

#define DFTRACER_UTILS_LOG_STDOUT_REDIRECT(fpath) \
  freopen((fpath), "a+", stdout);
#define DFTRACER_UTILS_LOG_STDERR_REDIRECT(fpath) \
  freopen((fpath), "a+", stderr);
#define DFTRACER_UTILS_LOGGER_NAME "DFTRACER_UTILS"

#define DFTRACER_UTILS_INTERNAL_TRACE(file, line, function, name,           \
                                      logger_level)                         \
  cpp_logger_clog(logger_level, name, "[%s] %s [%s:%d]",                    \
                  dftracer::utils::dftracer_utils_macro_get_time().c_str(), \
                  function, file, line)

#define DFTRACER_UTILS_INTERNAL_TRACE_SIMPLE(file, line, function, name,    \
                                             logger_level, message)         \
  cpp_logger_clog(logger_level, name, "[%s] %s %s [%s:%d]",                 \
                  dftracer::utils::dftracer_utils_macro_get_time().c_str(), \
                  function, message, file, line)

#define DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(file, line, function, name,    \
                                             logger_level, format, ...)     \
  cpp_logger_clog(logger_level, name, "[%s] %s " format " [%s:%d]",         \
                  dftracer::utils::dftracer_utils_macro_get_time().c_str(), \
                  function, ##__VA_ARGS__, file, line)

#ifdef DFTRACER_UTILS_LOGGER_LEVEL_TRACE
#define DFTRACER_UTILS_LOGGER_INIT() \
  cpp_logger_clog_level(CPP_LOGGER_TRACE, DFTRACER_UTILS_LOGGER_NAME);
#elif defined(DFTRACER_UTILS_LOGGER_LEVEL_DEBUG)
#define DFTRACER_UTILS_LOGGER_INIT() \
  cpp_logger_clog_level(CPP_LOGGER_DEBUG, DFTRACER_UTILS_LOGGER_NAME);
#elif defined(DFTRACER_UTILS_LOGGER_LEVEL_INFO)
#define DFTRACER_UTILS_LOGGER_INIT() \
  cpp_logger_clog_level(CPP_LOGGER_INFO, DFTRACER_UTILS_LOGGER_NAME);
#elif defined(DFTRACER_UTILS_LOGGER_LEVEL_WARN)
#define DFTRACER_UTILS_LOGGER_INIT() \
  cpp_logger_clog_level(CPP_LOGGER_WARN, DFTRACER_UTILS_LOGGER_NAME);
#else
#define DFTRACER_UTILS_LOGGER_INIT() \
  cpp_logger_clog_level(CPP_LOGGER_ERROR, DFTRACER_UTILS_LOGGER_NAME);
#endif

#define DFTRACER_UTILS_LOGGER_LEVEL(level) \
  cpp_logger_clog_level(level, DFTRACER_UTILS_LOGGER_NAME);

#ifdef DFTRACER_UTILS_LOGGER_LEVEL_TRACE
#define DFTRACER_UTILS_LOG_TRACE()                                \
  DFTRACER_UTILS_INTERNAL_TRACE(__FILE__, __LINE__, __FUNCTION__, \
                                DFTRACER_UTILS_LOGGER_NAME, CPP_LOGGER_TRACE);
#define DFTRACER_UTILS_LOG_TRACE_FORMAT(...)                             \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_TRACE, __VA_ARGS__)
#else
#define DFTRACER_UTILS_LOG_TRACE() DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOG_TRACE_FORMAT(...) DFTRACER_UTILS_NOOP_MACRO
#endif

// Helper macro to count arguments
#define DFTRACER_UTILS_GET_ARG_COUNT(...) \
  DFTRACER_UTILS_GET_ARG_COUNT_(__VA_ARGS__, DFTRACER_UTILS_RSEQ_N())
#define DFTRACER_UTILS_GET_ARG_COUNT_(...) DFTRACER_UTILS_ARG_N(__VA_ARGS__)
#define DFTRACER_UTILS_ARG_N(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, N, ...) N
#define DFTRACER_UTILS_RSEQ_N() 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0

// Macro to choose between simple and format versions
#define DFTRACER_UTILS_LOG_DEBUG_CHOOSE(count) \
  DFTRACER_UTILS_LOG_DEBUG_CHOOSE_(count)
#define DFTRACER_UTILS_LOG_DEBUG_CHOOSE_(count) DFTRACER_UTILS_LOG_DEBUG_##count

#define DFTRACER_UTILS_LOG_DEBUG_1(message)                              \
  DFTRACER_UTILS_INTERNAL_TRACE_SIMPLE(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_DEBUG, message)
#define DFTRACER_UTILS_LOG_DEBUG_2(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_DEBUG, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_DEBUG_3(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_DEBUG, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_DEBUG_4(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_DEBUG, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_DEBUG_5(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_DEBUG, format, __VA_ARGS__)

// Debug macros
#ifdef DFTRACER_UTILS_LOGGER_LEVEL_DEBUG
#define DFTRACER_UTILS_LOG_DEBUG(...)                                        \
  DFTRACER_UTILS_LOG_DEBUG_CHOOSE(DFTRACER_UTILS_GET_ARG_COUNT(__VA_ARGS__)) \
  (__VA_ARGS__)
#else
#define DFTRACER_UTILS_LOG_DEBUG(...) DFTRACER_UTILS_NOOP_MACRO
#endif

// Info macros
#define DFTRACER_UTILS_LOG_INFO_1(message)                               \
  DFTRACER_UTILS_INTERNAL_TRACE_SIMPLE(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_INFO, message)
#define DFTRACER_UTILS_LOG_INFO_2(format, ...)                           \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_INFO, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_INFO_3(format, ...)                           \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_INFO, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_INFO_4(format, ...)                           \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_INFO, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_INFO_5(format, ...)                           \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_INFO, format, __VA_ARGS__)

#define DFTRACER_UTILS_LOG_INFO_CHOOSE(count) \
  DFTRACER_UTILS_LOG_INFO_CHOOSE_(count)
#define DFTRACER_UTILS_LOG_INFO_CHOOSE_(count) DFTRACER_UTILS_LOG_INFO_##count

#ifdef DFTRACER_UTILS_LOGGER_LEVEL_INFO
#define DFTRACER_UTILS_LOG_INFO(...)                                        \
  DFTRACER_UTILS_LOG_INFO_CHOOSE(DFTRACER_UTILS_GET_ARG_COUNT(__VA_ARGS__)) \
  (__VA_ARGS__)
#else
#define DFTRACER_UTILS_LOG_INFO(...) DFTRACER_UTILS_NOOP_MACRO
#endif

// Warning macros
#ifdef DFTRACER_UTILS_LOGGER_LEVEL_WARN
#define DFTRACER_UTILS_LOG_WARN(format, ...)                             \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_WARN, format, ##__VA_ARGS__)
#else
#define DFTRACER_UTILS_LOG_WARN(...) DFTRACER_UTILS_NOOP_MACRO
#endif

// Error macros
#define DFTRACER_UTILS_LOG_ERROR_1(message)                              \
  DFTRACER_UTILS_INTERNAL_TRACE_SIMPLE(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_ERROR, message)
#define DFTRACER_UTILS_LOG_ERROR_2(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_ERROR, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_ERROR_3(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_ERROR, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_ERROR_4(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_ERROR, format, __VA_ARGS__)
#define DFTRACER_UTILS_LOG_ERROR_5(format, ...)                          \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME,       \
                                       CPP_LOGGER_ERROR, format, __VA_ARGS__)

#define DFTRACER_UTILS_LOG_ERROR_CHOOSE(count) \
  DFTRACER_UTILS_LOG_ERROR_CHOOSE_(count)
#define DFTRACER_UTILS_LOG_ERROR_CHOOSE_(count) DFTRACER_UTILS_LOG_ERROR_##count

#ifdef DFTRACER_UTILS_LOGGER_LEVEL_ERROR
#define DFTRACER_UTILS_LOG_ERROR(...)                                        \
  DFTRACER_UTILS_LOG_ERROR_CHOOSE(DFTRACER_UTILS_GET_ARG_COUNT(__VA_ARGS__)) \
  (__VA_ARGS__)
#else
#define DFTRACER_UTILS_LOG_ERROR(...) DFTRACER_UTILS_NOOP_MACRO
#endif

// Print macro
#define DFTRACER_UTILS_LOG_PRINT(format, ...)                       \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(                             \
      __FILE__, __LINE__, __FUNCTION__, DFTRACER_UTILS_LOGGER_NAME, \
      CPP_LOGGER_PRINT, format, ##__VA_ARGS__)

#else
// Non-cpp-logger fallback
#define DFTRACER_UTILS_LOGGER_INIT() DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOGGER_LEVEL(level) DFTRACER_UTILS_NOOP_MACRO

#define DFTRACER_UTILS_LOG_PRINT(format, ...) \
  fprintf(stdout, format, ##__VA_ARGS__)
#define DFTRACER_UTILS_LOG_ERROR(format, ...) \
  fprintf(stderr, format, ##__VA_ARGS__)

#define DFTRACER_UTILS_LOG_WARN(...) DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOG_INFO(...) DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOG_DEBUG(...) DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOG_TRACE(...) DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOG_TRACE_FORMAT(...) DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOG_STDOUT_REDIRECT(fpath) DFTRACER_UTILS_NOOP_MACRO
#define DFTRACER_UTILS_LOG_STDERR_REDIRECT(fpath) DFTRACER_UTILS_NOOP_MACRO
#endif  // DFTRACER_UTILS_LOGGER_CPP_LOGGER

#endif  // DFTRACER_UTILS_COMMON_LOGGING_H
