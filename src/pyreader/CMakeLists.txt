cmake_minimum_required(VERSION 3.5)

project(pyreader 
    VERSION 1.0.0
    LANGUAGES C CXX
)

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

find_package(Python3 3.9 EXACT REQUIRED COMPONENTS Interpreter Development)

# Include the cmake modules that reader needs
# set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../cmake" ${CMAKE_MODULE_PATH})
# message(STATUS "${CMAKE_CURRENT_SOURCE_DIR}../../cmake")
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/CPM.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/Dependencies.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/Utils.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/CompilerWarnings.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/InstallHelpers.cmake)

# Use the existing reader build system
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../reader ${CMAKE_CURRENT_BINARY_DIR}/reader)

# Find Cython
# message(STATUS "Looking for Python = ${Python3_EXECUTABLE}")
# execute_process(
#     COMMAND ${Python3_EXECUTABLE} -c "import Cython; print(Cython.__version__)"
#     RESULT_VARIABLE CYTHON_NOT_FOUND
#     OUTPUT_QUIET ERROR_QUIET
# )

# if(CYTHON_NOT_FOUND)
#     message(FATAL_ERROR "Cython not found. Install with: pip install Cython")
# endif()

# Custom target to compile .pyx to .cpp using Cython
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/reader_cy.cpp
    COMMAND cython
        --cplus
        --3str
        -I ${CMAKE_CURRENT_SOURCE_DIR}
        -o ${CMAKE_CURRENT_BINARY_DIR}/reader_cy.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/reader.pyx
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/reader.pyx ${CMAKE_CURRENT_SOURCE_DIR}/reader.pxd
    COMMENT "Cythonizing reader.pyx"
)

# Create the Python extension module
add_library(reader_py MODULE
    ${CMAKE_CURRENT_BINARY_DIR}/reader_cy.cpp
)

# Set the output name to just "reader" (without lib prefix)
set_target_properties(reader_py PROPERTIES
    PREFIX ""
    OUTPUT_NAME "reader"
    SUFFIX "${Python3_EXTENSION_MODULE_SUFFIX}"
    MACOSX_RPATH ON
    BUILD_RPATH "@loader_path;@loader_path/lib"
    INSTALL_RPATH "@loader_path;@loader_path/lib"
)

# Include directories
target_include_directories(reader_py PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../
    ${Python3_INCLUDE_DIRS}
)

# Link against our reader library and Python
target_link_libraries(reader_py PRIVATE
    dft_reader::static
    Python3::Module
)

# Platform-specific settings

# Install targets
install(TARGETS reader_py
    LIBRARY DESTINATION .
    RUNTIME DESTINATION .
)

# Install the reader library dependencies
install(TARGETS dft_reader
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION lib
)

# Install spdlog if it's a shared library
if(TARGET spdlog AND NOT TARGET spdlog::spdlog_header_only)
    install(TARGETS spdlog
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION lib
    )
endif()

set_target_properties(reader_py PROPERTIES
    PREFIX ""
    OUTPUT_NAME "reader"
    # Use CMake’s Python-defined suffix instead of an undefined variable
    SUFFIX "${Python3_SOABI}"
    MACOSX_RPATH ON
    BUILD_RPATH "@loader_path;@loader_path/lib"
    INSTALL_RPATH "@loader_path;@loader_path/lib"
)

# Create __init__.py
install(CODE "
    file(WRITE \"\${CMAKE_INSTALL_PREFIX}/__init__.py\" 
        \"# DFTracer Python Reader Package\\n\"
    )
")

# # Configure build config for Python (if still needed)
# configure_file(
#     ${CMAKE_CURRENT_SOURCE_DIR}/build_config.py.in
#     ${CMAKE_CURRENT_SOURCE_DIR}/build_config.py
#     @ONLY
# )
