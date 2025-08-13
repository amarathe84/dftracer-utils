project(dft_utils 
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
need_picosha2()

##########################################
# Constants
##########################################

set(DFTRACER_UTILS_HEADERS
  indexer/indexer.h
  reader/reader.h
  utils/platform_compat.h
  utils/filesystem.h
  utils/file.h
  utils/logger.h
)

set(DFTRACER_UTILS_SOURCES
  indexer/indexer.cpp
  reader/reader.cpp
  utils/file.cpp
  utils/logger.cpp
)

##########################################
# Libraries
##########################################

# static library
add_library(dft_utils STATIC ${DFTRACER_UTILS_SOURCES})
set_target_properties(dft_utils PROPERTIES 
    OUTPUT_NAME dft_utils
)
target_link_libraries(dft_utils 
    PRIVATE spdlog::spdlog picosha2
    PUBLIC ghc_filesystem
)

# Link SQLite3 and zlib with appropriate variants for static library
link_sqlite3(dft_utils STATIC)
link_zlib(dft_utils STATIC)

# Create dft_utils include mapping using configure_file
foreach(header ${DFTRACER_UTILS_HEADERS})
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/${header} ${CMAKE_BINARY_DIR}/include/dft_utils/${header} COPYONLY)
endforeach()

target_include_directories(dft_utils 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_set_warnings(dft_utils)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_utils PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_utils PRIVATE --coverage)
endif()

# shared library
add_library(dft_utils_shared SHARED ${DFTRACER_UTILS_SOURCES})
set_target_properties(dft_utils_shared PROPERTIES 
    OUTPUT_NAME dft_utils
    EXPORT_NAME shared
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)
target_link_libraries(dft_utils_shared 
    PRIVATE spdlog::spdlog picosha2
    PUBLIC ghc_filesystem
)

# Link SQLite3 and zlib with appropriate variants for shared library
link_sqlite3(dft_utils_shared SHARED)
link_zlib(dft_utils_shared SHARED)

target_include_directories(dft_utils_shared 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_set_warnings(dft_utils_shared)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_utils_shared PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_utils_shared PRIVATE --coverage)
endif()


##########################################
# Executables
##########################################

## DFT Reader

add_executable(dft_reader bin/dft_reader.cpp ${DFTRACER_UTILS_SOURCES})
set_target_properties(dft_reader PROPERTIES OUTPUT_NAME "dft_reader")
target_link_libraries(dft_reader PRIVATE spdlog::spdlog argparse::argparse ghc_filesystem picosha2)

# Link SQLite3 and zlib for executable (treat as shared since it's an executable)
link_sqlite3(dft_reader STATIC)
link_zlib(dft_reader STATIC)

target_include_directories(dft_reader 
    PRIVATE 
        ${CMAKE_BINARY_DIR}/include
)

target_set_warnings(dft_reader)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_reader PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_reader PRIVATE --coverage)
endif()

# Make sure spdlog include directories are available for all targets
get_target_property(SPDLOG_INCLUDE_DIRS spdlog INTERFACE_INCLUDE_DIRECTORIES)
if(SPDLOG_INCLUDE_DIRS)
    target_include_directories(dft_utils PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dft_utils_shared PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dft_reader PRIVATE ${SPDLOG_INCLUDE_DIRS})
endif()


# Install executable
install(TARGETS dft_reader RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})


##########################################
# Installations
##########################################

# Aliases for the targets
add_library(dft_utils::static ALIAS dft_utils)
add_library(dft_utils::shared ALIAS dft_utils_shared)

# Install directive for libraries  
# Note: Only install targets that we can actually install (not imported/system targets)
# Don't install zlib targets as they are built by CPM and not designed for export
set(INSTALLABLE_TARGETS dft_utils dft_utils_shared)

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

if(TARGET picosha2)
    list(APPEND INSTALLABLE_TARGETS picosha2)
endif()

install(TARGETS ${INSTALLABLE_TARGETS}
    EXPORT dft_utilsTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Install headers
install(FILES ${DFTRACER_UTILS_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${CMAKE_PROJECT_NAME}/${PROJECT_NAME}
)

# Only create package config for non-Python builds
# if(NOT (DEFINED DFTRACER_PYTHON_BUILD AND DFTRACER_PYTHON_BUILD))
message(STATUS "Creating package config for dft_utils")
create_package_config(
    TARGET dft_utils
    VERSION ${PROJECT_VERSION}
    DESCRIPTION "DFTracer utils library for processing gzipped files"
    URL "https://github.com/LLNL/dftracer-utils"
    REQUIRES "sqlite3 spdlog"
    LIBS_PRIVATE "-lspdlog -lsqlite3 -lz"
)
# endif()
