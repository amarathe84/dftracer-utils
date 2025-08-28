#ifndef DFTRACER_UTILS_INDEXER_CONSTANTS_H
#define DFTRACER_UTILS_INDEXER_CONSTANTS_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

namespace dftracer::utils::constants {
namespace indexer {
static constexpr std::size_t ZLIB_WINDOW_SIZE = 32768;
static constexpr int ZLIB_GZIP_WINDOW_BITS = 31;  // 15 + 16 for gzip format
static constexpr std::size_t DEFAULT_CHECKPOINT_SIZE =
    32 * 1024 * 1024;  // 32MB
extern const char *SQL_SCHEMA;
}  // namespace indexer

namespace reader {
static constexpr std::size_t DEFAULT_BUFFER_SIZE = 65536;  // 64KB
static constexpr std::size_t SKIP_BUFFER_SIZE = 131072;    // 128KB
static constexpr std::size_t FILE_IO_BUFFER_SIZE =
    262144;  // 256KB for file I/O
}  // namespace reader
}  // namespace dftracer::utils::constants

#endif  // DFTRACER_UTILS_INDEXER_CONSTANTS_H
