#ifndef DFTRACER_UTILS_INDEXER_CONSTANTS_H
#define DFTRACER_UTILS_INDEXER_CONSTANTS_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

namespace constants {

namespace indexer {
static constexpr std::size_t INFLATE_BUFFER_SIZE = 16384;
static constexpr std::size_t PROCESS_BUFFER_SIZE = 65536;
static constexpr std::size_t ZLIB_WINDOW_SIZE = 32768;
static constexpr int ZLIB_GZIP_WINDOW_BITS = 31;  // 15 + 16 for gzip format
static constexpr std::size_t DEFAULT_CHECKPOINT_SIZE =
    32 * 1024 * 1024;  // 32MB
extern const char *SQL_SCHEMA;
}  // namespace indexer

namespace reader {
static constexpr std::size_t DEFAULT_BUFFER_SIZE = 65536;  // 64KB
static constexpr std::size_t DEFAULT_READER_BUFFER_SIZE =
    1 * 1024 * 1024;                                     // 1MB
static constexpr std::size_t SKIP_BUFFER_SIZE = 131072;  // 128KB
static constexpr std::size_t SEARCH_BUFFER_SIZE = 2048;
static constexpr std::size_t LINE_SEARCH_LOOKBACK = 512;
static constexpr std::size_t FIRST_CHECKPOINT_THRESHOLD = 33554401;  // 32MB
static constexpr std::size_t SMALL_RANGE_THRESHOLD = 1048576;        // 1MB
static constexpr std::size_t LARGE_RANGE_LOG_THRESHOLD = 40000;
static constexpr std::size_t FILE_IO_BUFFER_SIZE =
    262144;  // 256KB for file I/O
}  // namespace reader
}  // namespace constants

#endif  // DFTRACER_UTILS_INDEXER_CONSTANTS_H
