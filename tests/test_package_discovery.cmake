# Test different discovery methods based on TEST_TYPE
if(TEST_TYPE STREQUAL "pkgconfig")
    # Test pkg-config discovery
    message(STATUS "Testing pkg-config discovery...")
    
    # Set PKG_CONFIG_PATH environment variable
    set(ENV{PKG_CONFIG_PATH} "${PKG_CONFIG_PATH}")
    
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(DFTRACER_UTILS dftracer_utils)

    if(NOT DFTRACER_UTILS_FOUND)
        message(FATAL_ERROR "❌ pkg-config discovery failed: dftracer_utils not found")
    endif()
    
    message(STATUS "✅ pkg-config discovery successful")
    message(STATUS "   Version: ${DFTRACER_UTILS_VERSION}")
    message(STATUS "   Include dirs: ${DFTRACER_UTILS_INCLUDE_DIRS}")
    message(STATUS "   Libraries: ${DFTRACER_UTILS_LIBRARIES}")

elseif(TEST_TYPE STREQUAL "cmake")
    # Test CMake find_package discovery
    message(STATUS "Testing CMake find_package discovery...")
    
    # Check if config file exists
    find_file(DFTRACER_UTILS_CONFIG 
        NAMES dftracer_utilsConfig.cmake
        PATHS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib/cmake/dftracer_utils
        NO_DEFAULT_PATH
    )

    if(NOT DFTRACER_UTILS_CONFIG)
        message(FATAL_ERROR "❌ CMake discovery failed: dftracer_utilsConfig.cmake not found")
    endif()
    
    message(STATUS "✅ CMake find_package discovery successful")
    message(STATUS "   Config file: ${DFTRACER_UTILS_CONFIG}")

elseif(TEST_TYPE STREQUAL "target")
    # Test specific target alias by checking config files
    message(STATUS "Testing target alias: ${TARGET_ALIAS}")
    
    # Check if config file exists
    find_file(DFTRACER_UTILS_CONFIG 
        NAMES dftracer_utilsConfig.cmake
        PATHS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib/cmake/dftracer_utils
        NO_DEFAULT_PATH
    )

    if(NOT DFTRACER_UTILS_CONFIG)
        message(FATAL_ERROR "❌ Target alias test failed: dftracer_utilsConfig.cmake not found")
    endif()
    
    # Also check targets file
    find_file(DFTRACER_UTILS_TARGETS 
        NAMES dftracer_utilsTargets.cmake
        PATHS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib/cmake/dftracer_utils
        NO_DEFAULT_PATH
    )

    if(NOT DFTRACER_UTILS_TARGETS)
        message(FATAL_ERROR "❌ Target alias test failed: dftracer_utilsTargets.cmake not found")
    endif()
    
    # Read both config files to check if the target alias exists
    file(READ ${DFTRACER_UTILS_CONFIG} CONFIG_CONTENT)
    file(READ ${DFTRACER_UTILS_TARGETS} TARGETS_CONTENT)
    
    string(FIND "${CONFIG_CONTENT}" "${TARGET_ALIAS}" CONFIG_ALIAS_FOUND)
    string(FIND "${TARGETS_CONTENT}" "${TARGET_ALIAS}" TARGETS_ALIAS_FOUND)
    
    if(CONFIG_ALIAS_FOUND EQUAL -1 AND TARGETS_ALIAS_FOUND EQUAL -1)
        message(FATAL_ERROR "❌ Target alias test failed: ${TARGET_ALIAS} not found in config or targets files")
    endif()
    
    message(STATUS "✅ Target alias test successful: ${TARGET_ALIAS}")
    if(CONFIG_ALIAS_FOUND GREATER -1)
        message(STATUS "   Found in config file: ${DFTRACER_UTILS_CONFIG}")
    endif()
    if(TARGETS_ALIAS_FOUND GREATER -1)
        message(STATUS "   Found in targets file: ${DFTRACER_UTILS_TARGETS}")
    endif()

else()
    message(FATAL_ERROR "Unknown TEST_TYPE: ${TEST_TYPE}")
endif()
