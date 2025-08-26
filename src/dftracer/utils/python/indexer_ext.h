#ifndef DFTRACER_UTILS_PYTHON_INDEXER_EXT_H
#define DFTRACER_UTILS_PYTHON_INDEXER_EXT_H

#include <dftracer/utils/indexer/checkpoint.h>
#include <dftracer/utils/indexer/indexer.h>
#include <nanobind/nanobind.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

using namespace dftracer::utils;

class DFTracerIndexer {
   private:
    std::unique_ptr<Indexer> indexer_;
    std::string gz_path_;
    std::string idx_path_;
    std::size_t checkpoint_size_;

   public:
    DFTracerIndexer(
        const std::string &gz_path,
        const std::optional<std::string> &idx_path = std::nullopt,
        std::size_t checkpoint_size = Indexer::DEFAULT_CHECKPOINT_SIZE,
        bool force_rebuild = false);

    void build();
    bool need_rebuild() const;
    std::uint64_t get_max_bytes() const;
    std::uint64_t get_num_lines() const;
    int find_file_id(const std::string &gz_path) const;
    std::vector<IndexCheckpoint> get_checkpoints() const;
    std::vector<IndexCheckpoint> find_checkpoints_by_line_range(
        std::size_t start_line, std::size_t end_line) const;
    std::optional<IndexCheckpoint> find_checkpoint(
        std::size_t target_offset) const;

    std::string gz_path() const;
    std::string idx_path() const;
    std::size_t checkpoint_size() const;
    Indexer *get_indexer_ptr() const;

    DFTracerIndexer &__enter__();
    bool __exit__(nanobind::args args);
};

#endif
