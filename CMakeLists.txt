cmake_minimum_required(VERSION 3.5)


set(DFTRACER_UTILS_VERSION_MAJOR 0)
set(DFTRACER_UTILS_VERSION_MINOR 1)
set(DFTRACER_UTILS_VERSION_PATCH 0)
set(DFTRACER_UTILS_VERSION "${DFTRACER_UTILS_VERSION_MAJOR}.${DFTRACER_UTILS_VERSION_MINOR}.${DFTRACER_UTILS_VERSION_PATCH}")

project(dftrace_utils VERSION ${DFTRACER_UTILS_VERSION} LANGUAGES C CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable optimization
# set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Release flags" FORCE)
# Enable cross-platform large file support
add_definitions(-D_FILE_OFFSET_BITS=64)
if(NOT WIN32)
    add_definitions(-D_LARGEFILE64_SOURCE)
endif()

# Constants

if (SKBUILD)
  # Override standard CMake install directories for Python package
  set(CMAKE_INSTALL_LIBDIR "dftracer/utils/lib" CACHE STRING "Library install directory" FORCE)
  set(CMAKE_INSTALL_BINDIR "${SKBUILD_SCRIPTS_DIR}" CACHE STRING "Binary install directory" FORCE)
  set(CMAKE_INSTALL_INCLUDEDIR "dftracer/utils/include" CACHE STRING "Include install directory" FORCE)
  set(CMAKE_INSTALL_RPATH "@loader_path/../dftracer/utils/lib" CACHE STRING "Library install directory" FORCE)
  # Set RPATH properties for all targets
  set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()


option(DFTRACER_UTILS_USE_LOCAL_PACKAGES "Use local packages" OFF)
option(DFTRACER_UTILS_TESTS "Build tests" OFF)
option(DFTRACER_UTILS_COVERAGE "Enable coverage reporting" OFF)

include(cmake/CompilerWarnings.cmake)
include(cmake/InstallHelpers.cmake)
include(cmake/CPM.cmake)
include(cmake/Dependencies.cmake)
include(cmake/Utils.cmake)

set_coverage_compiler_flags(DFTRACER_UTILS_TESTS AND DFTRACER_UTILS_COVERAGE)

add_subdirectory(src)

if(DFTRACER_UTILS_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
