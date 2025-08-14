# Configure output directories
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
# set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
# set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})

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

#+++++++++++++++++++++++++++++++++++++++++
# Static library
#+++++++++++++++++++++++++++++++++++++++++

add_library(dftracer_utils STATIC ${DFTRACER_UTILS_SOURCES})
add_library(dftracer_utils::static ALIAS dftracer_utils)

set_target_properties(dftracer_utils PROPERTIES 
    OUTPUT_NAME dftracer_utils
)
target_link_libraries(dftracer_utils 
    PRIVATE spdlog::spdlog_header_only picosha2
    PUBLIC ghc_filesystem
)
target_add_rpath(dftracer_utils)

# Link SQLite3 and zlib with appropriate variants for static library
link_sqlite3(dftracer_utils STATIC)
link_zlib(dftracer_utils STATIC)

target_include_directories(dftracer_utils 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
target_set_warnings(dftracer_utils)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dftracer_utils PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dftracer_utils PRIVATE --coverage)
endif()

#+++++++++++++++++++++++++++++++++++++++++
# Shared Library
#+++++++++++++++++++++++++++++++++++++++++

add_library(dftracer_utils_shared SHARED ${DFTRACER_UTILS_SOURCES})
add_library(dftracer_utils::shared ALIAS dftracer_utils_shared)

set_target_properties(dftracer_utils_shared PROPERTIES 
    OUTPUT_NAME dftracer_utils
    EXPORT_NAME shared
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)
target_link_libraries(dftracer_utils_shared 
    PRIVATE spdlog::spdlog_header_only picosha2
    PUBLIC ghc_filesystem
)
target_add_rpath(dftracer_utils_shared)

# Link SQLite3 and zlib with appropriate variants for shared library
link_sqlite3(dftracer_utils_shared SHARED)
link_zlib(dftracer_utils_shared SHARED)

target_include_directories(dftracer_utils_shared 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
)
target_set_warnings(dftracer_utils_shared)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dftracer_utils_shared PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dftracer_utils_shared PRIVATE --coverage)
endif()

##########################################
# Executables
##########################################

## DFT Reader

add_executable(dft_reader bin/dft_reader.cpp ${DFTRACER_UTILS_SOURCES})
set_target_properties(dft_reader PROPERTIES OUTPUT_NAME "dft_reader")
target_link_libraries(dft_reader PRIVATE dftracer_utils spdlog::spdlog_header_only argparse::argparse ghc_filesystem picosha2)

# Link SQLite3 and zlib for executable (treat as shared since it's an executable)
link_sqlite3(dft_reader STATIC)
link_zlib(dft_reader STATIC)

target_include_directories(dft_reader 
    PRIVATE 
        ${CMAKE_SOURCE_DIR}/include
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
    target_include_directories(dftracer_utils PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dftracer_utils_shared PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dft_reader PRIVATE ${SPDLOG_INCLUDE_DIRS})
endif()


# Install executable
install(TARGETS dft_reader RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})


##########################################
# Python binding
##########################################

if (SKBUILD)
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

  set(PYTHON_BINDINGS_SOURCES python/reader_ext.cpp)

  foreach (source_file ${PYTHON_BINDINGS_SOURCES})
      get_filename_component(source_name ${source_file} NAME_WE)
      if (APPLE)
        nanobind_add_module(
          ${source_name}
          NB_STATIC STABLE_ABI
          ${source_file}
        )
      else()
        nanobind_add_module(
          ${source_name}
          NB_STATIC STABLE_ABI LTO
          ${source_file}
        )
      endif()
      target_link_libraries(${source_name} PRIVATE dftracer_utils::shared)
      target_add_rpath(${source_name})
      install(TARGETS ${source_name} LIBRARY DESTINATION dftracer/utils)
  endforeach()
endif()

##########################################
# Installations
##########################################

# Install directive for libraries  

set(INSTALLABLE_TARGETS dftracer_utils dftracer_utils_shared)
if(TARGET sqlite3)
    list(APPEND INSTALLABLE_TARGETS sqlite3)
endif()
if(TARGET sqlite3_static)
    list(APPEND INSTALLABLE_TARGETS sqlite3_static)
endif()
if(TARGET ghc_filesystem)
    list(APPEND INSTALLABLE_TARGETS ghc_filesystem)
endif()
if(TARGET picosha2)
    list(APPEND INSTALLABLE_TARGETS picosha2)
endif()
# if(TARGET spdlog)
#     list(APPEND INSTALLABLE_TARGETS spdlog)
# endif()
if (TARGET spdlog_header_only)
    list(APPEND INSTALLABLE_TARGETS spdlog_header_only)
endif()

install(TARGETS ${INSTALLABLE_TARGETS} EXPORT dftracer_utilsTargets)

# Install headers
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/dftracer/utils/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dftracer/utils
    FILES_MATCHING PATTERN "*.h"
)

message(STATUS "Creating package config for dftracer_utils")
create_package_config(
    TARGET dftracer_utils
    VERSION ${PROJECT_VERSION}
    DESCRIPTION "DFTracer utils library for processing gzipped files"
    URL "https://github.com/LLNL/dftracer-utils"
    REQUIRES "sqlite3 spdlog"
    LIBS_PRIVATE "-lspdlog -lsqlite3 -lz"
)
