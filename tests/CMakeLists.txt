# Tests for dftracer_utils

add_executable(test_reader
    test_reader.cpp
)

target_link_libraries(test_reader
    PRIVATE
    reader_static
    doctest::doctest
    ZLIB::ZLIB
)

target_include_directories(test_reader
    PRIVATE
    ${CMAKE_SOURCE_DIR}/reader
)

# Enable warnings for test target
target_set_warnings(test_reader)

# Add the test to CTest
add_test(NAME test_reader COMMAND test_reader)
