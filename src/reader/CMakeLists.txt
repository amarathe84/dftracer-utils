cmake_minimum_required(VERSION 3.10)

project(reader 
    VERSION 1.0.0
    LANGUAGES CXX
)

# Configure output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})

##########################################
# Dependencies
##########################################

find_package(ZLIB REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

##########################################
# Constants
##########################################

set(DFTRACER_UTILS_${PROJECT_NAME}_HEADERS
  indexer.h
  reader.h
  platform_compat.h
)

set(DFTRACER_UTILS_${PROJECT_NAME}_SOURCES
  indexer.cpp 
  reader.cpp
)

##########################################
# Targets
##########################################

# static library
add_library(dft_reader STATIC ${DFTRACER_UTILS_${PROJECT_NAME}_SOURCES})
set_target_properties(dft_reader PROPERTIES 
    OUTPUT_NAME dft_reader
)
target_link_libraries(dft_reader PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES} spdlog::spdlog)
target_include_directories(dft_reader 
    PRIVATE ${SQLITE3_INCLUDE_DIRS}
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_set_warnings(dft_reader)

# shared library
add_library(dft_reader_shared SHARED ${DFTRACER_UTILS_${PROJECT_NAME}_SOURCES})
set_target_properties(dft_reader_shared PROPERTIES 
    OUTPUT_NAME dft_reader
    EXPORT_NAME shared
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)
target_link_libraries(dft_reader_shared PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES} spdlog::spdlog)
target_include_directories(dft_reader_shared 
    PRIVATE ${SQLITE3_INCLUDE_DIRS}
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_set_warnings(dft_reader_shared)

# executable
add_executable(dft_reader_exe main.cpp ${DFTRACER_UTILS_${PROJECT_NAME}_SOURCES})
set_target_properties(dft_reader_exe PROPERTIES OUTPUT_NAME "dft_reader")
target_link_libraries(dft_reader_exe PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES} spdlog::spdlog argparse::argparse)
target_include_directories(dft_reader_exe PRIVATE ${SQLITE3_INCLUDE_DIRS})
target_set_warnings(dft_reader_exe)


##########################################
# Installations
##########################################

# Aliases for the targets
add_library(dft_reader::static ALIAS dft_reader)
add_library(dft_reader::shared ALIAS dft_reader_shared)

# Install directive for libraries
install(TARGETS dft_reader dft_reader_shared
    EXPORT dft_readerTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
# Install headers
install(FILES ${DFTRACER_UTILS_${PROJECT_NAME}_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${CMAKE_PROJECT_NAME}/${PROJECT_NAME}
)

# Install executable
install(TARGETS dft_reader_exe RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

create_package_config(
    TARGET dft_reader
    VERSION ${PROJECT_VERSION}
    DESCRIPTION "DFTracer reader library for processing gzipped files"
    URL "https://github.com/LLNL/dftracer-utils"
    REQUIRES "zlib sqlite3"
    LIBS_PRIVATE "-lspdlog"
)
