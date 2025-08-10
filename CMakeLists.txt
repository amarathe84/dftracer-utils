cmake_minimum_required(VERSION 3.5)
project(dftracer_utils)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable optimization
set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Release flags" FORCE)
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

include(cmake/CompilerWarnings.cmake)
include(cmake/InstallHelpers.cmake)
include(cmake/CPM.cmake)
include(cmake/Dependencies.cmake)
include(cmake/Utils.cmake)

set_coverage_compiler_flags(DFTRACER_UTILS_TESTS AND DFTRACER_UTILS_COVERAGE)

set(DFTRACER_PYTHON_BUILD OFF CACHE BOOL "Building for Python" FORCE)
add_subdirectory(src/reader)

if(DFTRACER_UTILS_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
