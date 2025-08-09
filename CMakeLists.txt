cmake_minimum_required(VERSION 3.5)
project(dftracer_utils)

# Constants
set(INSTALL_BINARY_DIR bin)
set(INSTALL_LIB_DIR lib)
set(INSTALL_INCLUDE_DIR include/${CMAKE_PROJECT_NAME})

add_subdirectory(reader)
