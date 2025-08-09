cmake_minimum_required(VERSION 3.5)
project(dftracer_utils)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable optimization
set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Release flags" FORCE)

include(cmake/CompilerWarnings.cmake)
include(cmake/InstallHelpers.cmake)

# Enable cross-platform large file support
add_definitions(-D_FILE_OFFSET_BITS=64)
if(NOT WIN32)
    add_definitions(-D_LARGEFILE64_SOURCE)
endif()

# Constants
set(INSTALL_BINARY_DIR bin)
set(INSTALL_LIB_DIR lib)
set(INSTALL_INCLUDE_DIR include/${CMAKE_PROJECT_NAME})
option(DFTRACER_UTILS_USE_LOCAL_PACKAGES "Use local packages" OFF)
option(DFTRACER_UTILS_TESTS "Build tests" OFF)
option(DFTRACER_UTILS_COVERAGE "Enable coverage reporting" OFF)

# Dependencies
include(cmake/CPM.cmake)

set(CPM_USE_LOCAL_PACKAGES ${DFTRACER_UTILS_USE_LOCAL_PACKAGES})
set(CPM_SOURCE_CACHE "${CMAKE_SOURCE_DIR}/.cpmsource")

CPMAddPackage(
  NAME spdlog
  GITHUB_REPOSITORY gabime/spdlog
  VERSION 1.12.0
  OPTIONS
    "SPDLOG_COMPILED_LIB ON"
    "SPDLOG_BUILD_SHARED ON"
    "SPDLOG_INSTALL ON"
)

CPMAddPackage(
  NAME argparse
  GITHUB_REPOSITORY p-ranav/argparse
  VERSION 3.1
  OPTIONS
    "ARGPARSE_BUILD_TESTS OFF"
    "ARGPARSE_BUILD_SAMPLES OFF"
)

CPMAddPackage(
  NAME filesystem
  GITHUB_REPOSITORY gulrak/filesystem
  VERSION 1.5.14
  OPTIONS
    "GHC_FILESYSTEM_WITH_INSTALL ON"
)

if(DFTRACER_UTILS_TESTS AND DFTRACER_UTILS_COVERAGE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 --coverage" CACHE STRING "Debug flags with coverage" FORCE)
        set(CMAKE_C_FLAGS_DEBUG "-g -O0 --coverage" CACHE STRING "Debug flags with coverage" FORCE)
        set(CMAKE_EXE_LINKER_FLAGS_DEBUG "--coverage" CACHE STRING "Linker flags for coverage" FORCE)
        set(CMAKE_SHARED_LINKER_FLAGS_DEBUG "--coverage" CACHE STRING "Shared linker flags for coverage" FORCE)
    endif()
endif()

add_subdirectory(src/reader)

if(DFTRACER_UTILS_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
