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

# Find dependencies
find_dependency(ZLIB REQUIRED)
find_dependency(PkgConfig REQUIRED)

# Find sqlite3 using pkg-config
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

# Try to find spdlog, but don't fail if not found since it might be a private dependency
find_dependency(spdlog QUIET)
if(NOT spdlog_FOUND)
    # If spdlog is not found, create an interface target
    # This assumes spdlog is available as a system library
    if(NOT TARGET spdlog::spdlog)
        add_library(spdlog::spdlog INTERFACE IMPORTED)
        # Try to find the library in system locations
        find_library(SPDLOG_LIB spdlog)
        if(SPDLOG_LIB)
            set_target_properties(spdlog::spdlog PROPERTIES
                INTERFACE_LINK_LIBRARIES \"\${SPDLOG_LIB}\"
            )
        else()
            # Fallback to just the library name
            set_target_properties(spdlog::spdlog PROPERTIES
                INTERFACE_LINK_LIBRARIES \"spdlog\"
            )
        endif()
    endif()
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
