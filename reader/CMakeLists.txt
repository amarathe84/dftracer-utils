cmake_minimum_required(VERSION 3.10)

project(reader C)

find_package(ZLIB REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

add_library(reader_static STATIC indexer.c reader.c)
set_target_properties(reader_static PROPERTIES OUTPUT_NAME indexer)
target_link_libraries(reader_static PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES})
target_include_directories(reader_static PRIVATE ${SQLITE3_INCLUDE_DIRS})

add_library(reader SHARED indexer.c reader.c)
set_target_properties(reader PROPERTIES OUTPUT_NAME indexer)
target_link_libraries(reader PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES})
target_include_directories(reader PRIVATE ${SQLITE3_INCLUDE_DIRS})

install(TARGETS reader_static ARCHIVE DESTINATION ${INSTALL_LIB_DIR})
install(TARGETS reader LIBRARY DESTINATION ${INSTALL_LIB_DIR})
install(FILES indexer.h reader.h DESTINATION ${INSTALL_INCLUDE_DIR}/${PROJECT_NAME})

add_executable(dft_reader main.c indexer.c reader.c)
target_link_libraries(dft_reader PRIVATE ZLIB::ZLIB ${SQLITE3_LIBRARIES})
target_include_directories(dft_reader PRIVATE ${SQLITE3_INCLUDE_DIRS})
install(TARGETS dft_reader RUNTIME DESTINATION ${INSTALL_BINARY_DIR})
