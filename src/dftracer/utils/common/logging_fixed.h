#ifndef USERS_RAYANDREW_PROJECTS_DFTRACER_DFTRACER_UTILS_SRC_DFTRACER_UTILS_COMMON_LOGGING_H
#define USERS_RAYANDREW_PROJECTS_DFTRACER_DFTRACER_UTILS_SRC_DFTRACER_UTILS_COMMON_LOGGING_H

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
  sprintf(dftracer_utils_ts_time_str, "%04d-%02d-%02d %02d:%02d:%02d.%lld",
          now->tm_year + 1900, now->tm_mon + 1, now->tm_mday, now->tm_hour,
          now->tm_min, now->tm_sec, dftracer_utils_ts_millis);
  return dftracer_utils_ts_time_str;
}
}  // namespace dftracer::utils

#if defined(DFTRACER_UTILS_LOGGER_CPP_LOGGER)
#include <cpp-logger/clogger.h>

// === Variadic dispatch helpers (C++17, no __VA_OPT__) ===
#define DFTR_UTILS_EXPAND(x) x
#define DFTR_UTILS_OVERLOAD_2(_1, _2, NAME, ...) NAME
// If there are 2+ args, choose FMT call; if exactly 1, choose MSG call.
#define DFTR_UTILS_SELECT_MSG_OR_FMT(...) DFTR_UTILS_EXPAND(DFTR_UTILS_OVERLOAD_2(__VA_ARGS__, DFTR_UTILS_FMT_CALL, DFTR_UTILS_MSG_CALL))

// Message-only internal emitter (no variadic ...)
#define DFTR_UTILS_INTERNAL_TRACE_MSG(file, line, function, name, logger_level, msg) \
  cpp_logger_clog(logger_level, name, "[%s] %s %s [%s:%d]", \
                  dftracer::utils::dftracer_utils_macro_get_time().c_str(), \
                  function, msg, file, line)

// Wrapper thunks to inject site info and level for dispatcher
#define DFTR_UTILS_MSG_CALL(msg) \
  DFTR_UTILS_INTERNAL_TRACE_MSG(__FILE__, __LINE__, __FUNCTION__, DFTRACER_UTILS_LOGGER_NAME, DFTR_UTILS_LOG_LEVEL, msg)
#define DFTR_UTILS_FMT_CALL(fmt, ...) \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, DFTRACER_UTILS_LOGGER_NAME, DFTR_UTILS_LOG_LEVEL, fmt, __VA_ARGS__)

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

// Swallow the preceding comma when __VA_ARGS__ is empty (Clang/GCC)
#define DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(file, line, function, name, logger_level, format, ...) \
  cpp_logger_clog(                                                                                  \
      logger_level,                                                                                 \
      name,                                                                                         \
      "[%s] %s " format " [%s:%d]",                                                                 \
      dftracer::utils::dftracer_utils_macro_get_time().c_str(),                                     \
      function,                                                                                     \
      ##__VA_ARGS__,                                                                                \
      file,                                                                                         \
      line)

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
#define DFTRACER_UTILS_LOG_TRACE()                                      \
  DFTRACER_UTILS_INTERNAL_TRACE(__FILE__, __LINE__, __FUNCTION__, \
                                DFTRACER_UTILS_LOGGER_NAME, CPP_LOGGER_TRACE);
#define DFTRACER_UTILS_LOG_TRACE_FORMAT(...) \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME, \
                                       CPP_LOGGER_TRACE, __VA_ARGS__)
#else
#define DFTRACER_UTILS_LOG_TRACE() 
#define DFTRACER_UTILS_LOG_TRACE_FORMAT(...) 
#endif

// Debug macros
#ifdef DFTRACER_UTILS_LOGGER_LEVEL_DEBUG
#define DFTRACER_UTILS_LOG_DEBUG(...) \
  do { enum { DFTR_UTILS_LOG_LEVEL = CPP_LOGGER_DEBUG }; DFTR_UTILS_EXPAND(DFTR_UTILS_SELECT_MSG_OR_FMT(__VA_ARGS__))(__VA_ARGS__); } while (0)
#else
#define DFTRACER_UTILS_LOG_DEBUG(...) 
#endif

// Info macros
#ifdef DFTRACER_UTILS_LOGGER_LEVEL_INFO
#define DFTRACER_UTILS_LOG_INFO(...) \
  do { enum { DFTR_UTILS_LOG_LEVEL = CPP_LOGGER_INFO }; DFTR_UTILS_EXPAND(DFTR_UTILS_SELECT_MSG_OR_FMT(__VA_ARGS__))(__VA_ARGS__); } while (0)
#else
#define DFTRACER_UTILS_LOG_INFO(...) 
#endif

// Warning macros
#ifdef DFTRACER_UTILS_LOGGER_LEVEL_WARN
#define DFTRACER_UTILS_LOG_WARN(...) \
  do { enum { DFTR_UTILS_LOG_LEVEL = CPP_LOGGER_WARN }; DFTR_UTILS_EXPAND(DFTR_UTILS_SELECT_MSG_OR_FMT(__VA_ARGS__))(__VA_ARGS__); } while (0)
#else
#define DFTRACER_UTILS_LOG_WARN(...) 
#endif

// Error macros
#ifdef DFTRACER_UTILS_LOGGER_LEVEL_ERROR
#define DFTRACER_UTILS_LOG_ERROR(...) \
  do { enum { DFTR_UTILS_LOG_LEVEL = CPP_LOGGER_ERROR }; DFTR_UTILS_EXPAND(DFTR_UTILS_SELECT_MSG_OR_FMT(__VA_ARGS__))(__VA_ARGS__); } while (0)
#else
#define DFTRACER_UTILS_LOG_ERROR(...) 
#endif

// Print macro
#define DFTRACER_UTILS_LOG_PRINT(...) \
  DFTRACER_UTILS_INTERNAL_TRACE_FORMAT(__FILE__, __LINE__, __FUNCTION__, \
                                       DFTRACER_UTILS_LOGGER_NAME, \
                                       CPP_LOGGER_PRINT, __VA_ARGS__)

#else
// Non-cpp-logger fallback
#define DFTRACER_UTILS_LOGGER_INIT() 
#define DFTRACER_UTILS_LOGGER_LEVEL(level) 

#define DFTRACER_UTILS_LOG_PRINT(format, ...) \
  fprintf(stdout, format, ##__VA_ARGS__)
#define DFTRACER_UTILS_LOG_ERROR(format, ...) \
  fprintf(stderr, format, ##__VA_ARGS__)

#define DFTRACER_UTILS_LOG_WARN(...) 
#define DFTRACER_UTILS_LOG_INFO(...) 
#define DFTRACER_UTILS_LOG_DEBUG(...) 
#define DFTRACER_UTILS_LOG_TRACE(...) 
#define DFTRACER_UTILS_LOG_TRACE_FORMAT(...) 
#define DFTRACER_UTILS_LOG_STDOUT_REDIRECT(fpath) 
#define DFTRACER_UTILS_LOG_STDERR_REDIRECT(fpath) 
#endif  // DFTRACER_UTILS_LOGGER_CPP_LOGGER

#endif  // USERS_RAYANDREW_PROJECTS_DFTRACER_DFTRACER_UTILS_SRC_DFTRACER_UTILS_COMMON_LOGGING_H
