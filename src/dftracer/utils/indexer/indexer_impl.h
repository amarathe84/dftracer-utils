#ifndef DFTRACER_UTILS_INDEXER_INDEXER_IMPL_H
#define DFTRACER_UTILS_INDEXER_INDEXER_IMPL_H

#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/helpers.h>
#include <dftracer/utils/indexer/sqlite/database.h>
#include <dftracer/utils/utils/filesystem.h>
#include <sqlite3.h>

#include <string>

namespace dftracer::utils {

struct IndexerImplementor {
    std::string gz_path;
    std::string gz_path_logical_path;
    std::string idx_path;
    size_t ckpt_size;
    bool force_rebuild;
    SqliteDatabase db;
    bool db_opened;
    mutable int cached_file_id;

    IndexerImplementor(const std::string &gz_path, const std::string &idx_path,
                       size_t ckpt_size, bool force_rebuild)
        : gz_path(gz_path),
          gz_path_logical_path(get_logical_path(gz_path)),
          idx_path(idx_path),
          ckpt_size(ckpt_size),
          force_rebuild(force_rebuild),
          db(nullptr),
          db_opened(false),
          cached_file_id(-1) {
        if (ckpt_size == 0) {
            throw IndexerError(IndexerError::Type::INVALID_ARGUMENT,
                               "ckpt_size must be greater than 0");
        }
        DFTRACER_UTILS_LOG_DEBUG("Created DFT indexer for gz: %s and index: %s",
                                 gz_path.c_str(), idx_path.c_str());
    }
};
}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_INDEXER_INDEXER_IMPL_H
