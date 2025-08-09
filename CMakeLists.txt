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

if(DFTRACER_UTILS_TESTS)
  CPMAddPackage(
    NAME doctest
    GITHUB_REPOSITORY doctest/doctest
    VERSION 2.4.11
  )
  
  # Add zlib for cross-platform gzip compression in tests
  find_package(ZLIB REQUIRED)
endif()

add_subdirectory(reader)

if(DFTRACER_UTILS_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
