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
need_simdjson()
need_arrow()
need_diy()

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
  utils/json.h
  pipeline/pipeline.h
  pipeline/execution_context/sequential.h
  pipeline/execution_context/threaded.h
  pipeline/execution_context/mpi.h
  analyzers/dftracer.h
)

set(DFTRACER_UTILS_SOURCES
  indexer/indexer.cpp
  reader/reader.cpp
  utils/file.cpp
  utils/logger.cpp
  utils/json.cpp
  analyzers/dftracer.cpp
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
    PUBLIC ghc_filesystem simdjson::simdjson_static
)
target_add_rpath(dftracer_utils)

# Link SQLite3, zlib, and Arrow with appropriate variants for static library
link_sqlite3(dftracer_utils STATIC)
link_zlib(dftracer_utils STATIC)
link_arrow(dftracer_utils STATIC)

target_include_directories(dftracer_utils 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
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
    PUBLIC ghc_filesystem simdjson::simdjson
)
target_add_rpath(dftracer_utils_shared)

# Link SQLite3, zlib, and Arrow with appropriate variants for shared library
link_sqlite3(dftracer_utils_shared SHARED)
link_zlib(dftracer_utils_shared SHARED)
link_arrow(dftracer_utils_shared SHARED)

target_include_directories(dftracer_utils_shared 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
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

# ----------------------------------------
# DFT Reader
# ----------------------------------------

add_executable(dft_reader bin/dft_reader.cpp ${DFTRACER_UTILS_SOURCES})
set_target_properties(dft_reader PROPERTIES OUTPUT_NAME "dft_reader")
target_link_libraries(dft_reader PRIVATE dftracer_utils spdlog::spdlog_header_only argparse::argparse ghc_filesystem picosha2 simdjson::simdjson)

# Link SQLite3, zlib, and Arrow for executable (treat as static since it's an executable)
link_sqlite3(dft_reader STATIC)
link_zlib(dft_reader STATIC)
link_arrow(dft_reader STATIC)

target_include_directories(dft_reader 
    PRIVATE ${CMAKE_SOURCE_DIR}/include)

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

# ----------------------------------------
# DFT Map
# ----------------------------------------

add_executable(dft_map bin/dft_map.cpp ${DFTRACER_UTILS_SOURCES})
set_target_properties(dft_map PROPERTIES OUTPUT_NAME "dft_map")
target_link_libraries(dft_map PRIVATE 
  dftracer_utils
  spdlog::spdlog_header_only 
  argparse::argparse
  ghc_filesystem
  picosha2
  simdjson::simdjson
  diy
)
add_mpi_deps_if_needed(dft_map)

# Link SQLite3, zlib, and Arrow for executable (treat as static since it's an executable)
link_sqlite3(dft_map STATIC)
link_zlib(dft_map STATIC)
link_arrow(dft_map STATIC)

target_include_directories(dft_map 
    PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_set_warnings(dft_map)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_map PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_map PRIVATE --coverage)
endif()

# Make sure spdlog include directories are available for all targets
get_target_property(SPDLOG_INCLUDE_DIRS spdlog INTERFACE_INCLUDE_DIRECTORIES)
if(SPDLOG_INCLUDE_DIRS)
    target_include_directories(dftracer_utils PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dftracer_utils_shared PRIVATE ${SPDLOG_INCLUDE_DIRS})
    target_include_directories(dft_map PRIVATE ${SPDLOG_INCLUDE_DIRS})
endif()

# Install executable
install(TARGETS dft_map RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

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

  set(PYTHON_BINDINGS_SOURCES
    python/dftracer_utils_ext.cpp
    python/indexer_ext.h 
    python/indexer_ext.cpp
    python/reader_ext.cpp
    python/utils_ext.cpp
    python/json_ext.h
    python/json_ext.cpp
  )

  if (APPLE)
    nanobind_add_module(
      dftracer_utils_ext
      NB_STATIC STABLE_ABI
      ${PYTHON_BINDINGS_SOURCES}
    )
  else()
    nanobind_add_module(
      dftracer_utils_ext
      NB_STATIC STABLE_ABI LTO
      ${PYTHON_BINDINGS_SOURCES}
    )
  endif()

  target_link_libraries(dftracer_utils_ext PRIVATE dftracer_utils::shared)
  target_add_rpath(dftracer_utils_ext)
  install(TARGETS dftracer_utils_ext LIBRARY DESTINATION dftracer/utils)
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
if (TARGET spdlog_header_only)
    list(APPEND INSTALLABLE_TARGETS spdlog_header_only)
endif()
if (TARGET simdjson)
    list(APPEND INSTALLABLE_TARGETS simdjson)
endif()
if (TARGET simdjson_static)
    list(APPEND INSTALLABLE_TARGETS simdjson_static)
endif()
if (TARGET dftracer_zlib)
  list(APPEND INSTALLABLE_TARGETS dftracer_zlib)
endif()
if (TARGET dftracer_zlibstatic)
  list(APPEND INSTALLABLE_TARGETS dftracer_zlibstatic)
endif()
if (TARGET arrow_array)
    list(APPEND INSTALLABLE_TARGETS arrow_array)
endif()
if (TARGET arrow_ipc)
    list(APPEND INSTALLABLE_TARGETS arrow_ipc)
endif()
if (TARGET arrow_util)
    list(APPEND INSTALLABLE_TARGETS arrow_util)
endif()
if (TARGET arrow_compute_core)
    list(APPEND INSTALLABLE_TARGETS arrow_compute_core)
endif()
if (TARGET arrow_vendored)
    list(APPEND INSTALLABLE_TARGETS arrow_vendored)
endif()
foreach(boost_target IN ITEMS
    boost_headers boost_algorithm boost_align boost_array boost_assert boost_bind boost_concept_check boost_config boost_container_hash boost_conversion boost_core boost_crc boost_describe boost_detail boost_exception boost_function boost_function_types boost_functional boost_fusion boost_integer boost_intrusive boost_io boost_iterator boost_lexical_cast boost_move boost_mp11 boost_mpl boost_optional boost_predef boost_preprocessor boost_range boost_ratio boost_regex boost_scope_exit boost_smart_ptr boost_static_assert boost_system boost_throw_exception boost_tokenizer boost_tuple boost_type_traits boost_typeof boost_unordered boost_utility boost_uuid boost_variant2 boost_winapi boost_numeric_conversion boost_comptarget_disable_autolinking boost_container)
    if(TARGET ${boost_target})
        list(APPEND INSTALLABLE_TARGETS ${boost_target})
    endif()
endforeach()
if (TARGET arrow_memory_pool)
    list(APPEND INSTALLABLE_TARGETS arrow_memory_pool)
endif()
if (TARGET arrow_io)
    list(APPEND INSTALLABLE_TARGETS arrow_io)
endif()
if (TARGET arrow_static)
    list(APPEND INSTALLABLE_TARGETS arrow_static)
endif()
if (TARGET arrow_shared)
    list(APPEND INSTALLABLE_TARGETS arrow_shared)
endif()
if (TARGET parquet_static)
    list(APPEND INSTALLABLE_TARGETS parquet_static)
endif()
if (TARGET parquet_shared)
    list(APPEND INSTALLABLE_TARGETS parquet_shared)
endif()

install(TARGETS ${INSTALLABLE_TARGETS} EXPORT dftracer_utilsTargets)

# Install headers
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/dftracer/utils/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dftracer/utils
    FILES_MATCHING PATTERN "*.h"
)

install(DIRECTORY ${CMAKE_BINARY_DIR}/include/dftracer/utils/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dftracer/utils
    FILES_MATCHING PATTERN "*.h"
)

message(STATUS "Creating package config for dftracer_utils")

# Determine which dependencies to require based on what was found vs built with CPM
set(PKG_CONFIG_REQUIRES "")
set(PKG_CONFIG_LIBS_PRIVATE "")

# Add spdlog to requirements (always needed)
set(PKG_CONFIG_REQUIRES "spdlog")
set(PKG_CONFIG_LIBS_PRIVATE "-lspdlog")

# Only add sqlite3 to pkg-config requirements if it was found on system
if(SQLite3_FOUND AND NOT SQLite3_CPM)
    set(PKG_CONFIG_REQUIRES "${PKG_CONFIG_REQUIRES} sqlite3")
    set(PKG_CONFIG_LIBS_PRIVATE "${PKG_CONFIG_LIBS_PRIVATE} -lsqlite3")
endif()

# Only add zlib to pkg-config requirements if it was found on system
if(ZLIB_FOUND AND NOT ZLIB_CPM)
    set(PKG_CONFIG_LIBS_PRIVATE "${PKG_CONFIG_LIBS_PRIVATE} -lz")
endif()

create_package_config(
    TARGET dftracer_utils
    VERSION ${DFTRACER_UTILS_PACKAGE_VERSION}
    DESCRIPTION "DFTracer utils library for processing gzipped files"
    URL "https://github.com/LLNL/dftracer-utils"
    REQUIRES "${PKG_CONFIG_REQUIRES}"
    LIBS_PRIVATE "${PKG_CONFIG_LIBS_PRIVATE}"
)
