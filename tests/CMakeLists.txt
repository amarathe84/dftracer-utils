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
    reader_static
    doctest::doctest
    ZLIB::ZLIB
)
target_include_directories(test_reader
    PRIVATE
    ${CMAKE_SOURCE_DIR}/src/reader
)
target_set_warnings(test_reader)

add_test(NAME test_reader COMMAND test_reader)
