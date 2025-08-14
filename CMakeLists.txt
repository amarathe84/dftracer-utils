cmake_minimum_required(VERSION 3.5)

set(DFTRACER_UTILS_VERSION_MAJOR 0)
set(DFTRACER_UTILS_VERSION_MINOR 1)
set(DFTRACER_UTILS_VERSION_PATCH 0)
set(DFTRACER_UTILS_VERSION "${DFTRACER_UTILS_VERSION_MAJOR}.${DFTRACER_UTILS_VERSION_MINOR}.${DFTRACER_UTILS_VERSION_PATCH}")

project(dftrace_utils VERSION ${DFTRACER_UTILS_VERSION} LANGUAGES C CXX)

####################################################################################
# Options
####################################################################################

option(DFTRACER_UTILS_TESTS "Build tests" OFF)
option(DFTRACER_UTILS_COVERAGE "Enable coverage reporting" OFF)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# Enable optimization
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Release flags" FORCE)

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

if (SKBUILD)
  # message(FATAL_ERROR "CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}")
  # Override standard CMake install directories for Python package
  set(CMAKE_INSTALL_LIBDIR "dftracer/utils/lib" CACHE STRING "Library install directory" FORCE)
  set(CMAKE_INSTALL_BINDIR "${SKBUILD_SCRIPTS_DIR}" CACHE STRING "Binary install directory" FORCE)
  set(CMAKE_INSTALL_INCLUDEDIR "dftracer/utils/include" CACHE STRING "Include install directory" FORCE)

  # set(PYTHON_SITE_PACKAGES ${CMAKE_INSTALL_PREFIX})
  # execute_process (COMMAND ${DFTRACER_PYTHON_EXE} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())" OUTPUT_VARIABLE MAIN_PYTHON_SITE_PACKAGES OUTPUT_STRIP_TRAILING_WHITESPACE)

  # set(SITE_PACKAGES_LOC_DIR "${SKBUILD_PLATLIB_DIR}/")
  # set(CMAKE_INSTALL_RPATH "@loader_path/../dftracer/utils/lib" CACHE STRING "Library install directory" FORCE)
endif()

set_coverage_compiler_flags(DFTRACER_UTILS_TESTS AND DFTRACER_UTILS_COVERAGE)

add_subdirectory(src)

if(DFTRACER_UTILS_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
