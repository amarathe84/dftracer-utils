#ifndef DFTRACER_UTILS_COMMON_PLATFORM_COMPAT_H
#define DFTRACER_UTILS_COMMON_PLATFORM_COMPAT_H

// Cross-platform compatibility definitions

#ifdef _WIN32
// Windows specific includes and definitions
#include <fcntl.h>
#include <io.h>

// Map POSIX functions to Windows equivalents
#define fseeko _fseeki64
#define ftello _ftelli64
#define popen _popen
#define pclose _pclose
#define fileno _fileno
#define stat _stat64

// For large file support on Windows
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

#else
// POSIX systems (Linux, macOS, etc.)
#include <unistd.h>

// Enable large file support on 32-bit systems
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

// Ensure we have the large file variants
#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#endif

#endif  // DFTRACER_UTILS_COMMON_PLATFORM_COMPAT_H
