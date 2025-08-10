cmake_minimum_required(VERSION 3.15...3.26)

project(dft_reader C CXX)

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

find_package(PythonExtensions REQUIRED)

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

add_library(reader MODULE dft_reader/_reader.cpp)
target_include_directories(reader PRIVATE
    ${PYREADER_INCLUDE_DIR}
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(reader
    dft_reader::shared
)
python_extension_module(reader
                        LINKED_MODULES_VAR linked_module_list
                        FORWARD_DECL_MODULES_VAR fdecl_module_list)
install(TARGETS reader LIBRARY DESTINATION hello)
