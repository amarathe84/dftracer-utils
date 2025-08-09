# Tests for dftracer_utils

##########################################
# Dependencies
##########################################

CPMAddPackage(
  NAME doctest
  GITHUB_REPOSITORY doctest/doctest
  VERSION 2.4.11
)

# Add zlib for cross-platform gzip compression
find_package(ZLIB REQUIRED)

##########################################
# Targets
##########################################

## Test Reader

add_executable(test_reader
    test_reader.cpp
)
target_link_libraries(test_reader
    PRIVATE
    dft_reader
    doctest::doctest
    ZLIB::ZLIB
    ghc_filesystem
)
target_include_directories(test_reader
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/reader
)
target_set_warnings(test_reader)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_reader PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(test_reader PRIVATE --coverage)
endif()

add_test(NAME test_reader COMMAND test_reader)

##########################################
# Package Discovery Tests
##########################################

# Test discovery using the installed package
if(NOT CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    add_test(NAME install_package
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target install
    )
    set_tests_properties(install_package 
        PROPERTIES
        FIXTURES_SETUP "installed_package"
    )
    
    # Create a test for pkg-config discovery
    add_test(NAME test_pkgconfig_discovery
        COMMAND ${CMAKE_COMMAND} 
        -DPKG_CONFIG_PATH=${CMAKE_INSTALL_PREFIX}/lib/pkgconfig
        -DTEST_TYPE=pkgconfig
        -P ${CMAKE_CURRENT_SOURCE_DIR}/test_package_discovery.cmake
    )
    
    # Create a test for CMake find_package discovery
    add_test(NAME test_cmake_discovery
        COMMAND ${CMAKE_COMMAND}
        -DCMAKE_PREFIX_PATH=${CMAKE_INSTALL_PREFIX}
        -DTEST_TYPE=cmake
        -P ${CMAKE_CURRENT_SOURCE_DIR}/test_package_discovery.cmake
    )
    
    # Create tests for all target aliases
    foreach(TARGET_ALIAS "dft_reader" "dft_reader::static" "dft_reader::shared" "dft_reader::dft_reader")
        string(REPLACE "::" "_" TEST_NAME_SUFFIX ${TARGET_ALIAS})
        add_test(NAME test_target_${TEST_NAME_SUFFIX}
            COMMAND ${CMAKE_COMMAND}
            -DCMAKE_PREFIX_PATH=${CMAKE_INSTALL_PREFIX}
            -DTARGET_ALIAS=${TARGET_ALIAS}
            -DTEST_TYPE=target
            -P ${CMAKE_CURRENT_SOURCE_DIR}/test_package_discovery.cmake
        )
    endforeach()
    
    # Set test dependencies - discovery tests should run after installation
    set_tests_properties(
        test_pkgconfig_discovery
        test_cmake_discovery
        test_target_dft_reader
        test_target_dft_reader_static
        test_target_dft_reader_shared
        test_target_dft_reader_dft_reader
        PROPERTIES
        FIXTURES_REQUIRED "installed_package"
    )
endif()
