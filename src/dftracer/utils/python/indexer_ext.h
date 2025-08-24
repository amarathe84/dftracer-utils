#ifndef DFTRACER_UTILS_PYTHON_INDEXER_EXT_H
#define DFTRACER_UTILS_PYTHON_INDEXER_EXT_H

#include <dftracer/utils/indexer/indexer.h>
#include <nanobind/nanobind.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

class DFTracerIndexer {
 private:
  std::unique_ptr<dftracer::utils::indexer::Indexer> indexer_;
  std::string gz_path_;
  std::string idx_path_;
  size_t checkpoint_size_;

 public:
  DFTracerIndexer(
      const std::string &gz_path,
      const std::optional<std::string> &idx_path = std::nullopt,
      size_t checkpoint_size =
          dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
      bool force_rebuild = false);

  void build();
  bool need_rebuild() const;
  bool is_valid() const;
  uint64_t get_max_bytes() const;
  uint64_t get_num_lines() const;
  int find_file_id(const std::string &gz_path) const;
  std::vector<dftracer::utils::indexer::CheckpointInfo> get_checkpoints() const;
  std::vector<dftracer::utils::indexer::CheckpointInfo>
  find_checkpoints_by_line_range(size_t start_line, size_t end_line) const;
  std::optional<dftracer::utils::indexer::CheckpointInfo> find_checkpoint(
      size_t target_offset) const;

  std::string gz_path() const;
  std::string idx_path() const;
  size_t checkpoint_size() const;
  dftracer::utils::indexer::Indexer *get_indexer_ptr() const;

  DFTracerIndexer &__enter__();
  bool __exit__(nanobind::args args);
};

#endif
