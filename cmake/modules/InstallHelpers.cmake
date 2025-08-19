# InstallHelpers.cmake - Utilities for creating pkg-config and CMake package configs

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

#[=[
Creates pkg-config (.pc) and CMake package configuration files for a target.

Usage:
  create_package_config(
    TARGET target_name
    [VERSION version_string]
    [DESCRIPTION "Package description"]
    [URL "https://example.com"]
    [REQUIRES "dep1 dep2"]
    [LIBS_PRIVATE "private_libs"]
    [CFLAGS_PRIVATE "private_cflags"]
  )

Arguments:
  TARGET - The target name (used for package name)
  VERSION - Package version (defaults to PROJECT_VERSION)
  DESCRIPTION - Package description
  URL - Package URL
  REQUIRES - Public dependencies for pkg-config
  LIBS_PRIVATE - Private libraries for pkg-config
  CFLAGS_PRIVATE - Private compile flags for pkg-config
#]=]
function(create_package_config)
    set(options "")
    set(oneValueArgs TARGET VERSION DESCRIPTION URL REQUIRES LIBS_PRIVATE CFLAGS_PRIVATE)
    set(multiValueArgs "")
    
    cmake_parse_arguments(PKG "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    
    if(NOT PKG_TARGET)
        message(FATAL_ERROR "TARGET is required")
    endif()
    
    if(NOT PKG_VERSION)
        set(PKG_VERSION ${PROJECT_VERSION})
    endif()
    
    if(NOT PKG_DESCRIPTION)
        set(PKG_DESCRIPTION "${PKG_TARGET} library")
    endif()

    get_target_property(TARGET_TYPE ${PKG_TARGET} TYPE)

    create_pkgconfig_file(
        TARGET ${PKG_TARGET}
        VERSION ${PKG_VERSION}
        DESCRIPTION ${PKG_DESCRIPTION}
        URL ${PKG_URL}
        REQUIRES ${PKG_REQUIRES}
        LIBS_PRIVATE ${PKG_LIBS_PRIVATE}
        CFLAGS_PRIVATE ${PKG_CFLAGS_PRIVATE}
    )
    
    create_cmake_config_files(
        TARGET ${PKG_TARGET}
        VERSION ${PKG_VERSION}
    )
endfunction()

#[=[
Internal function to create pkg-config file
#]=]
function(create_pkgconfig_file)
    set(options "")
    set(oneValueArgs TARGET VERSION DESCRIPTION URL REQUIRES LIBS_PRIVATE CFLAGS_PRIVATE)
    set(multiValueArgs "")
    
    cmake_parse_arguments(PKG "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    
    # Generate pkg-config file content
    set(PC_CONTENT "")
    string(APPEND PC_CONTENT "prefix=${CMAKE_INSTALL_PREFIX}\n")
    string(APPEND PC_CONTENT "exec_prefix=\${prefix}\n")
    string(APPEND PC_CONTENT "libdir=\${exec_prefix}/${CMAKE_INSTALL_LIBDIR}\n")
    string(APPEND PC_CONTENT "includedir=\${prefix}/${CMAKE_INSTALL_INCLUDEDIR}\n")
    string(APPEND PC_CONTENT "\n")
    string(APPEND PC_CONTENT "Name: ${PKG_TARGET}\n")
    string(APPEND PC_CONTENT "Description: ${PKG_DESCRIPTION}\n")
    string(APPEND PC_CONTENT "Version: ${PKG_VERSION}\n")
    
    if(PKG_URL)
        string(APPEND PC_CONTENT "URL: ${PKG_URL}\n")
    endif()
    
    if(PKG_REQUIRES)
        string(APPEND PC_CONTENT "Requires: ${PKG_REQUIRES}\n")
    endif()
    
    if(PKG_LIBS_PRIVATE)
        string(APPEND PC_CONTENT "Libs.private: ${PKG_LIBS_PRIVATE}\n")
    endif()
    
    if(PKG_CFLAGS_PRIVATE)
        string(APPEND PC_CONTENT "Cflags.private: ${PKG_CFLAGS_PRIVATE}\n")
    endif()
    
    string(APPEND PC_CONTENT "Libs: -L\${libdir} -l${PKG_TARGET}\n")
    string(APPEND PC_CONTENT "Cflags: -I\${includedir}\n")
    
    # Write pkg-config file
    set(PC_FILE "${CMAKE_CURRENT_BINARY_DIR}/${PKG_TARGET}.pc")
    file(WRITE ${PC_FILE} ${PC_CONTENT})
    
    # Install pkg-config file
    install(FILES ${PC_FILE}
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    )
endfunction()

#[=[
Internal function to create CMake package config files
#]=]
function(create_cmake_config_files)
    set(options "")
    set(oneValueArgs TARGET VERSION)
    set(multiValueArgs "")
    
    cmake_parse_arguments(PKG "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    
    # Create the config file template
    set(CONFIG_TEMPLATE "${CMAKE_CURRENT_BINARY_DIR}/${PKG_TARGET}Config.cmake.in")
    file(WRITE ${CONFIG_TEMPLATE} "
@PACKAGE_INIT@

include(CMakeFindDependencyMacro)

# Find dependencies - handle both CPM-built and system packages

# ZLIB dependency
find_library(ZLIB_LIBRARY_BUNDLED
    NAMES z libz zlib
    PATHS \${_IMPORT_PREFIX}/lib
    NO_DEFAULT_PATH
)

if(ZLIB_LIBRARY_BUNDLED)
    # Found zlib that was built with this package
    find_path(ZLIB_INCLUDE_DIR_BUNDLED
        NAMES zlib.h
        PATHS \${_IMPORT_PREFIX}/include
        NO_DEFAULT_PATH
    )
    
    if(ZLIB_INCLUDE_DIR_BUNDLED AND NOT TARGET ZLIB::ZLIB)
        add_library(ZLIB::ZLIB UNKNOWN IMPORTED)
        set_target_properties(ZLIB::ZLIB PROPERTIES
            IMPORTED_LOCATION \"\${ZLIB_LIBRARY_BUNDLED}\"
            INTERFACE_INCLUDE_DIRECTORIES \"\${ZLIB_INCLUDE_DIR_BUNDLED}\"
        )
    endif()
else()
    # Fall back to system zlib
    find_dependency(ZLIB REQUIRED)
endif()

# SQLITE3 dependency
find_library(SQLITE3_LIBRARY_BUNDLED
    NAMES sqlite3 libsqlite3
    PATHS \${_IMPORT_PREFIX}/lib
    NO_DEFAULT_PATH
)

if(SQLITE3_LIBRARY_BUNDLED)
    # Found sqlite3 that was built with this package
    find_path(SQLITE3_INCLUDE_DIR_BUNDLED
        NAMES sqlite3.h
        PATHS \${_IMPORT_PREFIX}/include
        NO_DEFAULT_PATH
    )
    
    if(SQLITE3_INCLUDE_DIR_BUNDLED AND NOT TARGET SQLite::SQLite3)
        add_library(SQLite::SQLite3 UNKNOWN IMPORTED)
        set_target_properties(SQLite::SQLite3 PROPERTIES
            IMPORTED_LOCATION \"\${SQLITE3_LIBRARY_BUNDLED}\"
            INTERFACE_INCLUDE_DIRECTORIES \"\${SQLITE3_INCLUDE_DIR_BUNDLED}\"
        )
    endif()
else()
    # Fall back to system sqlite3 via pkg-config
    find_dependency(PkgConfig REQUIRED)
    pkg_check_modules(SQLITE3 REQUIRED sqlite3)
    
    if(SQLITE3_FOUND AND NOT TARGET SQLite::SQLite3)
        add_library(SQLite::SQLite3 UNKNOWN IMPORTED)
        set_target_properties(SQLite::SQLite3 PROPERTIES
            IMPORTED_LOCATION \"\${SQLITE3_LIBRARIES}\"
            INTERFACE_INCLUDE_DIRECTORIES \"\${SQLITE3_INCLUDE_DIRS}\"
        )
    endif()
endif()

# SPDLOG dependency
find_library(SPDLOG_LIBRARY_BUNDLED
    NAMES spdlog libspdlog
    PATHS \${_IMPORT_PREFIX}/lib
    NO_DEFAULT_PATH
)

if(SPDLOG_LIBRARY_BUNDLED)
    # Found spdlog that was built with this package
    find_path(SPDLOG_INCLUDE_DIR_BUNDLED
        NAMES spdlog/spdlog.h
        PATHS \${_IMPORT_PREFIX}/include
        NO_DEFAULT_PATH
    )
    
    if(SPDLOG_INCLUDE_DIR_BUNDLED AND NOT TARGET spdlog::spdlog)
        add_library(spdlog::spdlog UNKNOWN IMPORTED)
        set_target_properties(spdlog::spdlog PROPERTIES
            IMPORTED_LOCATION \"\${SPDLOG_LIBRARY_BUNDLED}\"
            INTERFACE_INCLUDE_DIRECTORIES \"\${SPDLOG_INCLUDE_DIR_BUNDLED}\"
        )
    endif()
    
    # Also create header-only alias if not exists
    if(NOT TARGET spdlog::spdlog_header_only)
        add_library(spdlog::spdlog_header_only INTERFACE IMPORTED)
        set_target_properties(spdlog::spdlog_header_only PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES \"\${SPDLOG_INCLUDE_DIR_BUNDLED}\"
        )
    endif()
else()
    # Try to find system spdlog
    find_dependency(spdlog QUIET)
    if(NOT spdlog_FOUND)
        # If spdlog is not found, create an interface target for header-only usage
        if(NOT TARGET spdlog::spdlog_header_only)
            add_library(spdlog::spdlog_header_only INTERFACE IMPORTED)
            # Try to find the library in system locations
            find_library(SPDLOG_LIB spdlog)
            if(SPDLOG_LIB)
                set_target_properties(spdlog::spdlog_header_only PROPERTIES
                    INTERFACE_LINK_LIBRARIES \"\${SPDLOG_LIB}\"
                )
            else()
                # Fallback to just the library name for header-only usage
                set_target_properties(spdlog::spdlog_header_only PROPERTIES
                    INTERFACE_COMPILE_DEFINITIONS \"SPDLOG_HEADER_ONLY\"
                )
            endif()
        endif()
        
        if(NOT TARGET spdlog::spdlog)
            add_library(spdlog::spdlog ALIAS spdlog::spdlog_header_only)
        endif()
    endif()
endif()

# SIMDJSON dependency
find_library(SIMDJSON_LIBRARY_BUNDLED
    NAMES simdjson libsimdjson
    PATHS \${_IMPORT_PREFIX}/lib
    NO_DEFAULT_PATH
)

if(SIMDJSON_LIBRARY_BUNDLED)
    # Found simdjson that was built with this package
    find_path(SIMDJSON_INCLUDE_DIR_BUNDLED
        NAMES simdjson.h
        PATHS \${_IMPORT_PREFIX}/include
        NO_DEFAULT_PATH
    )
    
    if(SIMDJSON_INCLUDE_DIR_BUNDLED)
        # Create shared target if not exists
        if(NOT TARGET simdjson::simdjson)
            add_library(simdjson::simdjson UNKNOWN IMPORTED)
            set_target_properties(simdjson::simdjson PROPERTIES
                IMPORTED_LOCATION \"\${SIMDJSON_LIBRARY_BUNDLED}\"
                INTERFACE_INCLUDE_DIRECTORIES \"\${SIMDJSON_INCLUDE_DIR_BUNDLED}\"
            )
        endif()
        
        # Also look for static version
        find_library(SIMDJSON_STATIC_LIBRARY_BUNDLED
            NAMES simdjson_static libsimdjson_static
            PATHS \${_IMPORT_PREFIX}/lib
            NO_DEFAULT_PATH
        )
        
        if(SIMDJSON_STATIC_LIBRARY_BUNDLED AND NOT TARGET simdjson::simdjson_static)
            add_library(simdjson::simdjson_static UNKNOWN IMPORTED)
            set_target_properties(simdjson::simdjson_static PROPERTIES
                IMPORTED_LOCATION \"\${SIMDJSON_STATIC_LIBRARY_BUNDLED}\"
                INTERFACE_INCLUDE_DIRECTORIES \"\${SIMDJSON_INCLUDE_DIR_BUNDLED}\"
            )
        endif()
    endif()
else()
    # Try to find system simdjson
    find_dependency(simdjson QUIET)
endif()

# GHC_FILESYSTEM dependency (header-only)
find_path(GHC_FILESYSTEM_INCLUDE_DIR_BUNDLED
    NAMES ghc/filesystem.hpp
    PATHS \${_IMPORT_PREFIX}/include
    NO_DEFAULT_PATH
)

if(GHC_FILESYSTEM_INCLUDE_DIR_BUNDLED AND NOT TARGET ghc_filesystem)
    add_library(ghc_filesystem INTERFACE IMPORTED)
    set_target_properties(ghc_filesystem PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES \"\${GHC_FILESYSTEM_INCLUDE_DIR_BUNDLED}\"
    )
else()
    # Try to find system ghc_filesystem
    find_dependency(ghc_filesystem QUIET)
endif()

# PICOSHA2 dependency (header-only)
find_path(PICOSHA2_INCLUDE_DIR_BUNDLED
    NAMES picosha2.h
    PATHS \${_IMPORT_PREFIX}/include
    NO_DEFAULT_PATH
)

if(PICOSHA2_INCLUDE_DIR_BUNDLED AND NOT TARGET picosha2)
    add_library(picosha2 INTERFACE IMPORTED)
    set_target_properties(picosha2 PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES \"\${PICOSHA2_INCLUDE_DIR_BUNDLED}\"
    )
endif()

# Include the targets file
include(\"\${CMAKE_CURRENT_LIST_DIR}/${PKG_TARGET}Targets.cmake\")

# Main target (no namespace): ${PKG_TARGET} -> points to static
if(TARGET ${PKG_TARGET}::${PKG_TARGET} AND NOT TARGET ${PKG_TARGET})
    add_library(${PKG_TARGET} ALIAS ${PKG_TARGET}::${PKG_TARGET})
endif()

# Static alias: ${PKG_TARGET}::static -> points to main static target
if(TARGET ${PKG_TARGET}::${PKG_TARGET} AND NOT TARGET ${PKG_TARGET}::static)
    add_library(${PKG_TARGET}::static ALIAS ${PKG_TARGET}::${PKG_TARGET})
endif()

# Shared alias: ${PKG_TARGET}::shared -> points to shared target (if it exists)
if(TARGET ${PKG_TARGET}::shared AND NOT TARGET ${PKG_TARGET}::shared)
    # Target already exists, no alias needed
elseif(TARGET ${PKG_TARGET}::dft_reader_shared AND NOT TARGET ${PKG_TARGET}::shared)
    add_library(${PKG_TARGET}::shared ALIAS ${PKG_TARGET}::dft_reader_shared)
endif()

check_required_components(${PKG_TARGET})
")
    
    # Configure the config file
    set(CONFIG_FILE "${CMAKE_CURRENT_BINARY_DIR}/${PKG_TARGET}Config.cmake")
    configure_package_config_file(
        ${CONFIG_TEMPLATE}
        ${CONFIG_FILE}
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PKG_TARGET}
    )
    
    # Create version file
    set(VERSION_FILE "${CMAKE_CURRENT_BINARY_DIR}/${PKG_TARGET}ConfigVersion.cmake")
    write_basic_package_version_file(
        ${VERSION_FILE}
        VERSION ${PKG_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    
    # Install config files
    install(FILES ${CONFIG_FILE} ${VERSION_FILE}
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PKG_TARGET}
    )
    
    # Export targets
    install(EXPORT ${PKG_TARGET}Targets
        FILE ${PKG_TARGET}Targets.cmake
        NAMESPACE ${PKG_TARGET}::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PKG_TARGET}
    )
    
    # Export targets for build tree
    export(EXPORT ${PKG_TARGET}Targets
        FILE "${CMAKE_CURRENT_BINARY_DIR}/${PKG_TARGET}Targets.cmake"
        NAMESPACE ${PKG_TARGET}::
    )
endfunction()
