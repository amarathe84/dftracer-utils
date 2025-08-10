cmake_minimum_required(VERSION 3.15...3.26)

project(dft_reader C CXX)

if (NOT SKBUILD)
  message(WARNING "\
  This CMake file is meant to be executed using 'scikit-build'. Running
  it directly will almost certainly not produce the desired result. If
  you are a user trying to install this package, please use the command
  below, which will install all necessary build dependencies, compile
  the package in an isolated environment, and then install it.
  =====================================================================
   $ pip install .
  =====================================================================
  If you are a software developer, and this is your own package, then
  it is usually much more efficient to install the build dependencies
  in your environment once and use the following command that avoids
  a costly creation of a new virtual environment at every compilation:
  =====================================================================
   $ pip install nanobind scikit-build-core[pyproject]
   $ pip install --no-build-isolation -ve .
  =====================================================================
  You may optionally add -Ceditable.rebuild=true to auto-rebuild when
  the package is imported. Otherwise, you need to re-run the above
  after editing C++ files.")
endif()

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable optimization
set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)
set(CMAKE_C_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)

# Signal that this is a Python build to avoid export issues
set(DFTRACER_PYTHON_BUILD ON CACHE BOOL "Building for Python" FORCE)

if (CMAKE_VERSION VERSION_LESS 3.18)
  set(DEV_MODULE Development)
else()
  set(DEV_MODULE Development.Module)
endif()

find_package(Python 3.8 
REQUIRED COMPONENTS Interpreter ${DEV_MODULE}
OPTIONAL_COMPONENTS Development.SABIModule)

execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

# Include the utility modules before adding subdirectory
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/CompilerWarnings.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/InstallHelpers.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/CPM.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/Dependencies.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/Utils.cmake)

# Set RPATH settings to avoid library path conflicts
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Override installation destinations for Python build BEFORE add_subdirectory
# Move libraries and headers to dft_reader directory instead of site-packages root
set(CMAKE_INSTALL_LIBDIR "dft_reader/lib" CACHE STRING "Library install directory" FORCE)
set(CMAKE_INSTALL_INCLUDEDIR "dft_reader/include" CACHE STRING "Include install directory" FORCE)

# For binaries, install them to a subdirectory to avoid conflicts
# This works regardless of whether user has venv or not
set(CMAKE_INSTALL_BINDIR "dft_reader/bin" CACHE STRING "Binary install directory" FORCE)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../.. ${CMAKE_CURRENT_BINARY_DIR}/dftracer_utils)

# Create the package-style include directory structure in build directory
set(PYREADER_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/include)
file(MAKE_DIRECTORY ${PYREADER_INCLUDE_DIR}/dftracer_utils/reader)

# Copy headers to the package-style directory structure
set(READER_HEADERS
    indexer.h
    reader.h
    platform_compat.h
    filesystem.h
)

foreach(header ${READER_HEADERS})
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../reader/${header}
        ${PYREADER_INCLUDE_DIR}/dftracer_utils/reader/${header}
        COPYONLY
    )
endforeach()

nanobind_add_module(
  dft_reader_ext
  NB_STATIC STABLE_ABI LTO
  src/reader_ext.cpp
)
target_include_directories(dft_reader_ext PRIVATE ${PYREADER_INCLUDE_DIR})
target_link_libraries(dft_reader_ext PRIVATE dft_reader::shared)

# Set RPATH for the extension module
set_target_properties(dft_reader_ext PROPERTIES 
    INSTALL_RPATH_USE_LINK_PATH TRUE
    BUILD_WITH_INSTALL_RPATH FALSE
    INSTALL_RPATH "$ORIGIN:$ORIGIN/lib:$ORIGIN/lib64:$ORIGIN/../lib:$ORIGIN/../lib64"
)

install(TARGETS dft_reader_ext LIBRARY DESTINATION dft_reader)
