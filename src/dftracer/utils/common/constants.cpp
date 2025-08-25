#include <dftracer/utils/common/constants.h>

namespace constants {

namespace indexer {
const char *SQL_SCHEMA = R"(
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
}
}  // namespace constants
