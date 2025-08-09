cmake_minimum_required(VERSION 3.10)

project(reader CXX)

find_package(ZLIB REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

# Platform-specific libraries
if(WIN32)
    set(PLATFORM_LIBS "")
else()
    set(PLATFORM_LIBS "")
endif()

add_library(reader_static STATIC indexer.cpp reader.cpp)
set_target_properties(reader_static PROPERTIES OUTPUT_NAME indexer)
target_link_libraries(reader_static PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES} spdlog::spdlog ${PLATFORM_LIBS})
target_include_directories(reader_static PRIVATE ${SQLITE3_INCLUDE_DIRS})
target_set_warnings(reader_static)

add_library(reader SHARED indexer.cpp reader.cpp)
set_target_properties(reader PROPERTIES OUTPUT_NAME indexer)
target_link_libraries(reader PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES} spdlog::spdlog ${PLATFORM_LIBS})
target_include_directories(reader PRIVATE ${SQLITE3_INCLUDE_DIRS})
target_set_warnings(reader)

install(TARGETS reader_static ARCHIVE DESTINATION ${INSTALL_LIB_DIR})
install(TARGETS reader LIBRARY DESTINATION ${INSTALL_LIB_DIR})
install(FILES indexer.h reader.h platform_compat.h DESTINATION ${INSTALL_INCLUDE_DIR}/${PROJECT_NAME})

add_executable(dft_reader main.cpp indexer.cpp reader.cpp)
set_target_properties(dft_reader PROPERTIES OUTPUT_NAME "dft_reader")
target_link_libraries(dft_reader PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES} spdlog::spdlog argparse::argparse ${PLATFORM_LIBS})
target_include_directories(dft_reader PRIVATE ${SQLITE3_INCLUDE_DIRS})
target_set_warnings(dft_reader)
install(TARGETS dft_reader RUNTIME DESTINATION ${INSTALL_BINARY_DIR})
