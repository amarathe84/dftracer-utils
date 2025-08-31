# Configure output directories
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
# set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})
# set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${PROJECT_NAME})

##########################################
# Dependencies
##########################################

need_zlib()
need_sqlite3()
need_argparse()
need_ghc_filesystem()
need_xxhash()
need_simdjson()
need_cpplogger()
need_yyjson()
find_package(Threads REQUIRED)

##########################################
# Libraries
##########################################

#-----------------------------------------
# Utils Base library
#-----------------------------------------

set(DFTRACER_UTILS_SOURCES
  # Common
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/common/constants.cpp

  # Indexer
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/indexer.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/indexer_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/indexer_c.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/helpers.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/checkpoint_size.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/error.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/delete_file_record.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/insert_checkpoint_record.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/insert_file_metadata_record.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/insert_file_record.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_checkpoint.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_checkpoints.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_file_id.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_max_bytes.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_num_lines.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_schema_validity.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_stored_file_info.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/indexer/queries/query_checkpoint_size.cpp

  # Reader
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/reader.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/reader_impl.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/reader_c.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/reader/error.cpp

  # Utilities
  # ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/utils/json.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/utils/timer.cpp

  # Pipeline
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/error.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/pipeline.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/executor_type.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/sequential_executor.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/executors/thread_executor.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/pipeline/tasks/task_type.cpp

  # Analyzers
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/constants.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/analyzer.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/pipeline/trace_reader.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/derive_io_cat.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/bins.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/ignore_events.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/analyzers/helpers/parse_trace.cpp
)


#+++++++++++++++++++++++++++++++++++++++++
# Static library
#+++++++++++++++++++++++++++++++++++++++++

add_library(dftracer_utils STATIC ${DFTRACER_UTILS_SOURCES})
add_library(dftracer_utils::static ALIAS dftracer_utils)

set_target_properties(dftracer_utils PROPERTIES 
    OUTPUT_NAME dftracer_utils
)
target_link_libraries(dftracer_utils 
    PRIVATE xxHash::xxhash
    PUBLIC ghc_filesystem simdjson::simdjson_static cpp-logger Threads::Threads yyjson
)
add_mpi_deps_if_needed(dftracer_utils)
target_add_rpath(dftracer_utils)
add_stdfs_if_needed(dftracer_utils)

# Link SQLite3, zlib, and Arrow with appropriate variants for static library
link_sqlite3(dftracer_utils STATIC)
link_zlib(dftracer_utils STATIC)
# link_arrow(dftracer_utils STATIC)

target_include_directories(dftracer_utils 
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
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
    PRIVATE xxHash::xxhash
    PUBLIC ghc_filesystem simdjson::simdjson cpp-logger Threads::Threads yyjson
)
add_mpi_deps_if_needed(dftracer_utils_shared)
target_add_rpath(dftracer_utils_shared)
add_stdfs_if_needed(dftracer_utils_shared)

# Link SQLite3, zlib, and Arrow with appropriate variants for shared library
link_sqlite3(dftracer_utils_shared SHARED)
link_zlib(dftracer_utils_shared SHARED)
# link_arrow(dftracer_utils_shared SHARED)

target_include_directories(dftracer_utils_shared 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
)
target_set_warnings(dftracer_utils_shared)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dftracer_utils_shared PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dftracer_utils_shared PRIVATE --coverage)
endif()

#-----------------------------------------
# Analyzer library
#-----------------------------------------

# set(DFTRACER_UTILS_ANALYZER_HEADERS
#   pipeline/pipeline.h
#   analyzers/analyzer.h
# )

# set(DFTRACER_UTILS_ANALYZER_SOURCES
#   analyzers/analyzer.cpp
# )

#+++++++++++++++++++++++++++++++++++++++++
# Static library
#+++++++++++++++++++++++++++++++++++++++++

# add_library(dftracer_utils_analyzer STATIC ${DFTRACER_UTILS_ANALYZER_SOURCES})
# add_library(dftracer_utils_analyzer::static ALIAS dftracer_utils_analyzer)

# set_target_properties(dftracer_utils_analyzer PROPERTIES 
#     OUTPUT_NAME dftracer_utils
#     EXPORT_NAME shared
#     VERSION ${PROJECT_VERSION}
#     SOVERSION ${PROJECT_VERSION_MAJOR}
# )
# target_link_libraries(dftracer_utils_analyzer 
#     PUBLIC dftracer_utils
# )
# target_add_rpath(dftracer_utils_analyzer)

# target_include_directories(dftracer_utils_analyzer 
#     PUBLIC 
#         $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include/analyzers>
#         $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include/pipeline>
#         $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
# )
# target_set_warnings(dftracer_utils_analyzer)

# if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
#     target_compile_options(dftracer_utils_analyzer PRIVATE --coverage -fprofile-arcs -ftest-coverage)
#     target_link_libraries(dftracer_utils_analyzer PRIVATE --coverage)
# endif()

##########################################
# Executables
##########################################

# ----------------------------------------
# DFT Reader
# ----------------------------------------

add_executable(dft_reader ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dft_reader.cpp)
set_target_properties(dft_reader PROPERTIES OUTPUT_NAME "dft_reader")
target_link_libraries(dft_reader PRIVATE dftracer_utils argparse::argparse ghc_filesystem xxHash::xxhash simdjson::simdjson yyjson)

# Link SQLite3, zlib, and Arrow for executable (treat as static since it's an executable)
link_sqlite3(dft_reader STATIC)
link_zlib(dft_reader STATIC)
# link_arrow(dft_reader STATIC)

target_include_directories(dft_reader 
    PRIVATE ${CMAKE_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/src
            ${CMAKE_BINARY_DIR}/include
)

target_set_warnings(dft_reader)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_reader PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_reader PRIVATE --coverage)
endif()

# Install executable
install(TARGETS dft_reader RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# ----------------------------------------
# DFT Map
# ----------------------------------------

add_executable(dft_map ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/bin/dft_map.cpp)
set_target_properties(dft_map PROPERTIES OUTPUT_NAME "dft_map")
target_link_libraries(dft_map PRIVATE 
  dftracer_utils
  argparse::argparse
  ghc_filesystem
  xxHash::xxhash
  simdjson::simdjson
  yyjson
)
# add_mpi_deps_if_needed(dft_map)

link_sqlite3(dft_map STATIC)
link_zlib(dft_map STATIC)

target_include_directories(dft_map 
    PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_set_warnings(dft_map)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dft_map PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(dft_map PRIVATE --coverage)
endif()

# Install executable
install(TARGETS dft_map RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

##########################################
# Python binding
##########################################

# if (SKBUILD)
#   if (CMAKE_VERSION VERSION_LESS 3.18)
#     set(DEV_MODULE Development)
#   else()
#     set(DEV_MODULE Development.Module)
#   endif()

#   find_package(Python 3.8 
#     REQUIRED COMPONENTS Interpreter ${DEV_MODULE}
#     OPTIONAL_COMPONENTS Development.SABIModule)

#   execute_process(
#     COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
#     OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
#   find_package(nanobind CONFIG REQUIRED)

#   if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
#     set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
#     set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
#   endif()

#   set(PYTHON_BINDINGS_SOURCES
#     ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/dftracer_utils_ext.cpp
#     ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer_ext.h
#     ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer_ext.cpp
#     ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/reader_ext.cpp
#     ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/utils_ext.cpp

#     # json
#     ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/json/helpers.h
#     ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/json/helpers.cpp
#   )

#   if (APPLE)
#     nanobind_add_module(
#       dftracer_utils_ext
#       NB_STATIC STABLE_ABI
#       ${PYTHON_BINDINGS_SOURCES}
#     )
#   else()
#     nanobind_add_module(
#       dftracer_utils_ext
#       NB_STATIC STABLE_ABI LTO
#       ${PYTHON_BINDINGS_SOURCES}
#     )
#   endif()

#   target_link_libraries(dftracer_utils_ext PRIVATE dftracer_utils::shared)
#   target_add_rpath(dftracer_utils_ext)
#   install(TARGETS dftracer_utils_ext LIBRARY DESTINATION dftracer/utils)
# endif()

# Python C API bindings (new approach)
if (SKBUILD)
  if (CMAKE_VERSION VERSION_LESS 3.18)
    set(DEV_MODULE Development)
  else()
    set(DEV_MODULE Development.Module)
  endif()

  find_package(Python 3.8 
    REQUIRED COMPONENTS Interpreter ${DEV_MODULE}
    OPTIONAL_COMPONENTS Development.SABIModule)

  if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
  endif()

  # Python C API bindings - Combined module
  add_library(dftracer_utils_ext MODULE
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/dftracer_utils_ext.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/indexer.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/reader.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/json.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dftracer/utils/python/json.cpp
  )
  
  target_include_directories(dftracer_utils_ext PRIVATE
    ${Python_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
  )
  
  target_link_libraries(dftracer_utils_ext PRIVATE 
    dftracer_utils::shared
    Python::Module
  )
  
  set_target_properties(dftracer_utils_ext PROPERTIES
    PREFIX ""
    SUFFIX ".${Python_SOABI}${CMAKE_SHARED_MODULE_SUFFIX}"
  )

  target_add_rpath(dftracer_utils_ext)
  
  install(TARGETS dftracer_utils_ext LIBRARY DESTINATION dftracer/utils)
endif()

##########################################
# Installations
##########################################

# Install directive for libraries  

set(INSTALLABLE_TARGETS dftracer_utils dftracer_utils_shared) # dftracer_utils_analyzer)
if(TARGET sqlite3)
    list(APPEND INSTALLABLE_TARGETS sqlite3)
endif()
if(TARGET sqlite3_static)
    list(APPEND INSTALLABLE_TARGETS sqlite3_static)
endif()
if(TARGET ghc_filesystem)
    list(APPEND INSTALLABLE_TARGETS ghc_filesystem)
endif()
if(TARGET xxhash)
    list(APPEND INSTALLABLE_TARGETS xxhash)
endif()
if (TARGET simdjson)
    list(APPEND INSTALLABLE_TARGETS simdjson)
endif()
if (TARGET simdjson_static)
    list(APPEND INSTALLABLE_TARGETS simdjson_static)
endif()
if (TARGET yyjson)
    list(APPEND INSTALLABLE_TARGETS yyjson)
endif()
if (TARGET dftracer_zlib)
  list(APPEND INSTALLABLE_TARGETS dftracer_zlib)
endif()
if (TARGET dftracer_zlibstatic)
  list(APPEND INSTALLABLE_TARGETS dftracer_zlibstatic)
endif()
if (TARGET cpp-logger)
  list(APPEND INSTALLABLE_TARGETS cpp-logger)
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
