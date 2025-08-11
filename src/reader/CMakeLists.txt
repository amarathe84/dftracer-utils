project(reader 
    VERSION 1.0.0
    LANGUAGES C CXX
)

# Configure output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})

##########################################
# Dependencies
##########################################

need_zlib()
need_sqlite3()
need_spdlog()
need_argparse()
need_ghc_filesystem()

##########################################
# Constants
##########################################

set(DFTRACER_UTILS_${PROJECT_NAME}_HEADERS
  indexer.h
  reader.h
  platform_compat.h
  filesystem.h
  utils.h
)

set(DFTRACER_UTILS_${PROJECT_NAME}_SOURCES
  indexer.cpp 
  reader.cpp
  utils.cpp
)

##########################################
# Targets
##########################################

# static library
add_library(dft_reader STATIC ${DFTRACER_UTILS_${PROJECT_NAME}_SOURCES})
set_target_properties(dft_reader PROPERTIES 
    OUTPUT_NAME dft_reader
)
target_link_libraries(dft_reader 
    PRIVATE spdlog::spdlog
    PUBLIC ghc_filesystem
)

# Link SQLite3 and zlib with appropriate variants for static library
link_sqlite3(dft_reader STATIC)
link_zlib(dft_reader STATIC)

target_include_directories(dft_reader 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_set_warnings(dft_reader)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_reader PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_reader PRIVATE --coverage)
endif()

# shared library
add_library(dft_reader_shared SHARED ${DFTRACER_UTILS_${PROJECT_NAME}_SOURCES})
set_target_properties(dft_reader_shared PROPERTIES 
    OUTPUT_NAME dft_reader
    EXPORT_NAME shared
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)
target_link_libraries(dft_reader_shared 
    PRIVATE spdlog::spdlog
    PUBLIC ghc_filesystem
)

# Link SQLite3 and zlib with appropriate variants for shared library
link_sqlite3(dft_reader_shared SHARED)
link_zlib(dft_reader_shared SHARED)

target_include_directories(dft_reader_shared 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_set_warnings(dft_reader_shared)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_reader_shared PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_reader_shared PRIVATE --coverage)
endif()

# executable
add_executable(dft_reader_exe main.cpp ${DFTRACER_UTILS_${PROJECT_NAME}_SOURCES})
set_target_properties(dft_reader_exe PROPERTIES OUTPUT_NAME "dft_reader")
target_link_libraries(dft_reader_exe PRIVATE spdlog::spdlog argparse::argparse ghc_filesystem)

# Link SQLite3 and zlib for executable (treat as shared since it's an executable)
link_sqlite3(dft_reader_exe STATIC)
link_zlib(dft_reader_exe STATIC)

target_set_warnings(dft_reader_exe)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_reader_exe PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_reader_exe PRIVATE --coverage)
endif()

# Make sure spdlog include directories are available for all targets
get_target_property(SPDLOG_INCLUDE_DIRS spdlog INTERFACE_INCLUDE_DIRECTORIES)
if(SPDLOG_INCLUDE_DIRS)
    target_include_directories(dft_reader PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dft_reader_shared PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dft_reader_exe PRIVATE ${SPDLOG_INCLUDE_DIRS})
endif()


##########################################
# Installations
##########################################

# Aliases for the targets
add_library(dft_reader::static ALIAS dft_reader)
add_library(dft_reader::shared ALIAS dft_reader_shared)

# Install directive for libraries  
# Note: Only install targets that we can actually install (not imported/system targets)
# Don't install zlib targets as they are built by CPM and not designed for export
set(INSTALLABLE_TARGETS dft_reader dft_reader_shared)

# Add sqlite3 if it's our own target (CPM-built)
if(TARGET sqlite3)
    list(APPEND INSTALLABLE_TARGETS sqlite3)
endif()

if(TARGET sqlite3_static)
    list(APPEND INSTALLABLE_TARGETS sqlite3_static)
endif()

# Add ghc_filesystem if it's installable
if(TARGET ghc_filesystem)
    list(APPEND INSTALLABLE_TARGETS ghc_filesystem)
endif()

install(TARGETS ${INSTALLABLE_TARGETS}
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

# Only create package config for non-Python builds
# if(NOT (DEFINED DFTRACER_PYTHON_BUILD AND DFTRACER_PYTHON_BUILD))
message(STATUS "Creating package config for dft_reader")
create_package_config(
    TARGET dft_reader
    VERSION ${PROJECT_VERSION}
    DESCRIPTION "DFTracer reader library for processing gzipped files"
    URL "https://github.com/LLNL/dftracer-utils"
    REQUIRES "sqlite3 spdlog"
    LIBS_PRIVATE "-lspdlog -lsqlite3 -lz"
)
# endif()
