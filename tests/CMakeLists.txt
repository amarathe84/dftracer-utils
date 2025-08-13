# Tests for dftracer_utils

##########################################
# Dependencies
##########################################

CPMAddPackage(
  NAME doctest
  GITHUB_REPOSITORY doctest/doctest
  VERSION 2.4.11
)

CPMAddPackage(
  NAME unity
  GITHUB_REPOSITORY ThrowTheSwitch/Unity
  VERSION 2.6.0
  DOWNLOAD_ONLY YES
)

if(TARGET unity)
  add_library(unity_lib ALIAS unity)
else()
  add_library(unity_lib STATIC ${unity_SOURCE_DIR}/src/unity.c)
  target_include_directories(unity_lib PUBLIC ${unity_SOURCE_DIR}/src)
endif()

include(${CMAKE_SOURCE_DIR}/cmake/CPM.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Dependencies.cmake)

need_zlib()

##########################################
# Testing Utilities
##########################################

add_library(testing_utilities STATIC
    testing_utilities.cpp
)
target_link_libraries(testing_utilities
    PRIVATE
    dft_utils
    doctest::doctest
    ZLIB::ZLIB
    ghc_filesystem
)
target_include_directories(testing_utilities
    PRIVATE
    ${CMAKE_BINARY_DIR}/include
)

##########################################
# Targets
##########################################

## Test Reader (C++ API)

add_executable(test_reader
    test_reader.cpp
)
target_link_libraries(test_reader
    PRIVATE
    dft_utils
    doctest::doctest
    testing_utilities
)
target_include_directories(test_reader
    PRIVATE
    ${CMAKE_BINARY_DIR}/include
)
target_set_warnings(test_reader)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_reader PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(test_reader PRIVATE --coverage)
endif()

add_test(NAME test_reader COMMAND test_reader)

## Test Reader C API

add_executable(test_reader_c
    test_reader.c
)
target_link_libraries(test_reader_c
    PRIVATE
    dft_utils
    unity_lib
    testing_utilities
)
target_include_directories(test_reader_c
    PRIVATE
    ${CMAKE_BINARY_DIR}/include
)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_reader_c PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(test_reader_c PRIVATE --coverage)
endif()

add_test(NAME test_reader_c COMMAND test_reader_c)

# Test Robustness

add_executable(test_reader_robustness
    test_reader_robustness.cpp
)
target_link_libraries(test_reader_robustness
    PRIVATE
    dft_utils
    doctest::doctest
    testing_utilities
)
target_include_directories(test_reader_robustness
    PRIVATE
    ${CMAKE_BINARY_DIR}/include
)
target_set_warnings(test_reader_robustness)

# Apply coverage flags if enabled
if(DFTRACER_UTILS_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_reader_robustness PRIVATE --coverage -fprofile-arcs -ftest-coverage)
    target_link_libraries(test_reader_robustness PRIVATE --coverage)
endif()

add_test(NAME test_reader_robustness COMMAND test_reader_robustness)

##########################################
# Python Tests (SKBUILD only)
##########################################

if(SKBUILD)
    # Find Python interpreter
    find_package(Python COMPONENTS Interpreter REQUIRED)
    
    # Add Python tests
    add_test(NAME test_python_reader
        COMMAND ${Python_EXECUTABLE} -m pytest ${CMAKE_CURRENT_SOURCE_DIR}/py/test_reader.py -v
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )
    
    # Set test environment
    set_tests_properties(test_python_reader
        PROPERTIES
        ENVIRONMENT "PYTHONPATH=${CMAKE_BINARY_DIR}/py/src"
    )
endif()

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
    foreach(TARGET_ALIAS "dft_utils" "dft_utils::static" "dft_utils::shared")
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
        test_target_dft_utils
        test_target_dft_utils_static
        test_target_dft_utils_shared
        PROPERTIES
        FIXTURES_REQUIRED "installed_package"
    )
endif()
