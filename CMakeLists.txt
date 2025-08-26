cmake_minimum_required(VERSION 3.5)

set(DFTRACER_UTILS_VERSION_MAJOR 0)
set(DFTRACER_UTILS_VERSION_MINOR 1)
set(DFTRACER_UTILS_VERSION_PATCH 0)
set(DFTRACER_UTILS_VERSION "${DFTRACER_UTILS_VERSION_MAJOR}.${DFTRACER_UTILS_VERSION_MINOR}.${DFTRACER_UTILS_VERSION_PATCH}")

project(dftracer_utils VERSION ${DFTRACER_UTILS_VERSION} LANGUAGES C CXX)

set(DFTRACER_UTILS_PACKAGE ${PROJECT_NAME})
set(DFTRACER_UTILS_PACKAGE_NAME ${PROJECT_NAME})
set(DFTRACER_UTILS_PACKAGE_VERSION "${PROJECT_VERSION}")
set(DFTRACER_UTILS_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")
set(DFTRACER_UTILS_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_PATCH}")
set(DFTRACER_UTILS_PACKAGE_STRING "${DFTRACER_UTILS_PACKAGE_NAME} ${DFTRACER_UTILS_PACKAGE_VERSION}")
set(DFTRACER_UTILS_PACKAGE_TARNAME "${DFTRACER_UTILS_PACKAGE}")

####################################################################################
# Options
####################################################################################

option(DFTRACER_UTILS_TESTS "Build tests" OFF)
option(DFTRACER_UTILS_COVERAGE "Enable coverage reporting" OFF)
option(DFTRACER_UTILS_DEBUG "Enable debug mode including verbose logging" OFF)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# Enable optimization

if (DFTRACER_UTILS_DEBUG)
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG" CACHE STRING "Debug flags" FORCE)
else()
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Release flags" FORCE)
endif()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

####################################################################################
# Internal Paths for cmake libraries and Setup install and output Directories
####################################################################################

# This sets where to look for dependent libraries
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_BINARY_DIR} ${CMAKE_INSTALL_PREFIX})
# This sets where to look for dependent library's cmake files
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR}/${DFTRACER_LIBDIR}/cmake)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR}/share/cmake)

include(CompilerWarnings)
include(InstallHelpers)
include(CPM)
include(Dependencies)
include(Utils)
include(Rpath)
# include(CrossPlatform)

if (SKBUILD)
  set(CMAKE_INSTALL_LIBDIR "dftracer/utils/lib" CACHE STRING "Library install directory" FORCE)
  set(CMAKE_INSTALL_BINDIR "${SKBUILD_SCRIPTS_DIR}" CACHE STRING "Binary install directory" FORCE)
  set(CMAKE_INSTALL_INCLUDEDIR "dftracer/utils/include" CACHE STRING "Include install directory" FORCE)
endif()

find_package(MPI COMPONENTS CXX)
if (MPI_FOUND)
  # message(FATAL_ERROR "MPI found: ${MPI_CXX_COMPILER}, Include dirs: ${MPI_CXX_INCLUDE_DIRS}")
  include_directories(${MPI_CXX_INCLUDE_DIRS})
  set(DFTRACER_UTILS_MPI_ENABLE 1)
endif()

if (DFTRACER_UTILS_DEBUG)
  configure_file("${CMAKE_SOURCE_DIR}/cmake/configure_files/dftracer_utils_config.dbg.h.in"
                 "${CMAKE_BINARY_DIR}/include/dftracer/utils/common/config.h" @ONLY)
else()
  configure_file("${CMAKE_SOURCE_DIR}/cmake/configure_files/dftracer_utils_config.h.in"
                 "${CMAKE_BINARY_DIR}/include/dftracer/utils/common/config.h" @ONLY)
endif()

set_coverage_compiler_flags(DFTRACER_UTILS_TESTS AND DFTRACER_UTILS_COVERAGE)

check_std_filesystem()
add_subdirectory(src)

if(DFTRACER_UTILS_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
