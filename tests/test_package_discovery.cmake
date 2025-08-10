# Test different discovery methods based on TEST_TYPE
if(TEST_TYPE STREQUAL "pkgconfig")
    # Test pkg-config discovery
    message(STATUS "Testing pkg-config discovery...")
    
    # Set PKG_CONFIG_PATH environment variable
    set(ENV{PKG_CONFIG_PATH} "${PKG_CONFIG_PATH}")
    
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(DFT_READER dft_reader)
    
    if(NOT DFT_READER_FOUND)
        message(FATAL_ERROR "❌ pkg-config discovery failed: dft_reader not found")
    endif()
    
    message(STATUS "✅ pkg-config discovery successful")
    message(STATUS "   Version: ${DFT_READER_VERSION}")
    message(STATUS "   Include dirs: ${DFT_READER_INCLUDE_DIRS}")
    message(STATUS "   Libraries: ${DFT_READER_LIBRARIES}")

elseif(TEST_TYPE STREQUAL "cmake")
    # Test CMake find_package discovery
    message(STATUS "Testing CMake find_package discovery...")
    
    # Check if config file exists
    find_file(DFT_READER_CONFIG 
        NAMES dft_readerConfig.cmake
        PATHS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib/cmake/dft_reader
        NO_DEFAULT_PATH
    )
    
    if(NOT DFT_READER_CONFIG)
        message(FATAL_ERROR "❌ CMake discovery failed: dft_readerConfig.cmake not found")
    endif()
    
    message(STATUS "✅ CMake find_package discovery successful")
    message(STATUS "   Config file: ${DFT_READER_CONFIG}")

elseif(TEST_TYPE STREQUAL "target")
    # Test specific target alias by checking config files
    message(STATUS "Testing target alias: ${TARGET_ALIAS}")
    
    # Check if config file exists
    find_file(DFT_READER_CONFIG 
        NAMES dft_readerConfig.cmake
        PATHS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib/cmake/dft_reader
        NO_DEFAULT_PATH
    )
    
    if(NOT DFT_READER_CONFIG)
        message(FATAL_ERROR "❌ Target alias test failed: dft_readerConfig.cmake not found")
    endif()
    
    # Also check targets file
    find_file(DFT_READER_TARGETS 
        NAMES dft_readerTargets.cmake
        PATHS ${CMAKE_PREFIX_PATH}
        PATH_SUFFIXES lib/cmake/dft_reader
        NO_DEFAULT_PATH
    )
    
    if(NOT DFT_READER_TARGETS)
        message(FATAL_ERROR "❌ Target alias test failed: dft_readerTargets.cmake not found")
    endif()
    
    # Read both config files to check if the target alias exists
    file(READ ${DFT_READER_CONFIG} CONFIG_CONTENT)
    file(READ ${DFT_READER_TARGETS} TARGETS_CONTENT)
    
    string(FIND "${CONFIG_CONTENT}" "${TARGET_ALIAS}" CONFIG_ALIAS_FOUND)
    string(FIND "${TARGETS_CONTENT}" "${TARGET_ALIAS}" TARGETS_ALIAS_FOUND)
    
    if(CONFIG_ALIAS_FOUND EQUAL -1 AND TARGETS_ALIAS_FOUND EQUAL -1)
        message(FATAL_ERROR "❌ Target alias test failed: ${TARGET_ALIAS} not found in config or targets files")
    endif()
    
    message(STATUS "✅ Target alias test successful: ${TARGET_ALIAS}")
    if(CONFIG_ALIAS_FOUND GREATER -1)
        message(STATUS "   Found in config file: ${DFT_READER_CONFIG}")
    endif()
    if(TARGETS_ALIAS_FOUND GREATER -1)
        message(STATUS "   Found in targets file: ${DFT_READER_TARGETS}")
    endif()

else()
    message(FATAL_ERROR "Unknown TEST_TYPE: ${TEST_TYPE}")
endif()
