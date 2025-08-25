#ifndef DFTRACER_UTILS_INDEXER_INDEXER_IMPL_H
#define DFTRACER_UTILS_INDEXER_INDEXER_IMPL_H

#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/checkpoint.h>
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/helpers.h>
#include <dftracer/utils/indexer/sqlite/database.h>
#include <dftracer/utils/utils/filesystem.h>
#include <sqlite3.h>

#include <cstdint>
#include <string>

namespace dftracer::utils {

struct IndexerImplementor {
    std::string gz_path;
    std::string gz_path_logical_path;
    std::string idx_path;
    size_t ckpt_size;
    bool force_rebuild;
    mutable bool cached_is_valid;
    mutable int cached_file_id;
    SqliteDatabase db;

    IndexerImplementor(const std::string &gz_path, const std::string &idx_path,
                       std::size_t ckpt_size, bool force_rebuild);

    void open();
    void build();
    bool need_rebuild() const;
    bool is_valid() const;

    // Metadata
    std::size_t get_checkpoint_size() const;
    std::uint64_t get_max_bytes() const;
    std::uint64_t get_num_lines() const;
    int get_file_id() const;

    // Lookup
    int find_file_id(const std::string &gz_path) const;
    bool find_checkpoint(size_t target_offset,
                         IndexCheckpoint &checkpoint) const;
    std::vector<IndexCheckpoint> get_checkpoints() const;
};
}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_INDEXER_INDEXER_IMPL_H
