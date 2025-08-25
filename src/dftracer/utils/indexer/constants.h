#ifndef DFTRACER_UTILS_INDEXER_CONSTANTS_H
#define DFTRACER_UTILS_INDEXER_CONSTANTS_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

namespace constants {
static constexpr size_t INFLATE_BUFFER_SIZE = 16384;
static constexpr size_t PROCESS_BUFFER_SIZE = 65536;
static constexpr size_t ZLIB_WINDOW_SIZE = 32768;
static constexpr int ZLIB_GZIP_WINDOW_BITS = 31;  // 15 + 16 for gzip format
static const char *SQL_SCHEMA = R"(
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER PRIMARY KEY,
      logical_name TEXT UNIQUE NOT NULL,
      byte_size INTEGER NOT NULL,
      mtime_unix INTEGER NOT NULL,
      sha256_hex TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS checkpoints (
      id INTEGER PRIMARY KEY,
      file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
      checkpoint_idx INTEGER NOT NULL,
      uc_offset INTEGER NOT NULL,
      uc_size INTEGER NOT NULL,
      c_offset INTEGER NOT NULL,
      c_size INTEGER NOT NULL,
      bits INTEGER NOT NULL,
      dict_compressed BLOB NOT NULL,
      num_lines INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS checkpoints_file_idx ON checkpoints(file_id, checkpoint_idx);
    CREATE INDEX IF NOT EXISTS checkpoints_file_uc_off_idx ON checkpoints(file_id, uc_offset);

    CREATE TABLE IF NOT EXISTS metadata (
      file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
      checkpoint_size INTEGER NOT NULL,
      total_lines INTEGER NOT NULL DEFAULT 0,
      total_uc_size INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(file_id)
    );
  )";
}  // namespace constants

#endif  // DFTRACER_UTILS_INDEXER_CONSTANTS_H
