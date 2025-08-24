#ifndef USERS_RAYANDREW_PROJECTS_DFTRACER_DFTRACER_UTILS_SRC_DFTRACER_UTILS_PYTHON_INDEXER_EXT_H
#define USERS_RAYANDREW_PROJECTS_DFTRACER_DFTRACER_UTILS_SRC_DFTRACER_UTILS_PYTHON_INDEXER_EXT_H

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
  // Constructor
  DFTracerIndexer(
      const std::string &gz_path,
      const std::optional<std::string> &idx_path = std::nullopt,
      size_t checkpoint_size =
          dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
      bool force_rebuild = false);

  // Build index
  void build();

  // Check if rebuild is needed
  bool need_rebuild() const;

  // Check if indexer is valid
  bool is_valid() const;

  // Get maximum bytes
  uint64_t get_max_bytes() const;

  // Get number of lines
  uint64_t get_num_lines() const;

  // Find file ID
  int find_file_id(const std::string &gz_path) const;

  // Get all checkpoints
  std::vector<dftracer::utils::indexer::CheckpointInfo> get_checkpoints() const;

  // Find checkpoints by line range
  std::vector<dftracer::utils::indexer::CheckpointInfo>
  find_checkpoints_by_line_range(size_t start_line, size_t end_line) const;

  // Find checkpoint for target offset
  std::optional<dftracer::utils::indexer::CheckpointInfo> find_checkpoint(
      size_t target_offset) const;

  // Getters
  std::string gz_path() const;
  std::string idx_path() const;
  size_t checkpoint_size() const;
  dftracer::utils::indexer::Indexer *get_indexer_ptr() const;

  // Context manager methods
  DFTracerIndexer &__enter__();
  bool __exit__(nanobind::args args);
};

#endif
