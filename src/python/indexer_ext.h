#ifndef __DFTRACER_UTILS_PYTHON_INDEXER_H
#define __DFTRACER_UTILS_PYTHON_INDEXER_H

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
  DFTracerIndexer(const std::string &gz_path,
                  const std::optional<std::string> &idx_path = std::nullopt,
                  size_t checkpoint_size = dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
                  bool force_rebuild = false)
      : gz_path_(gz_path), checkpoint_size_(checkpoint_size) {
    if (idx_path.has_value()) {
      idx_path_ = idx_path.value();
    } else {
      idx_path_ = gz_path + ".idx";
    }

    try {
      indexer_ = std::make_unique<dftracer::utils::indexer::Indexer>(
          gz_path_, idx_path_, checkpoint_size, force_rebuild);
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to create DFT indexer for gzip: " +
                               gz_path_ + " and index: " + idx_path_ +
                               " with checkpoint size: " +
                               std::to_string(checkpoint_size) + "B - " +
                               e.what());
    }
  }

  // Build index
  void build() {
    try {
      indexer_->build();
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to build index: " + std::string(e.what()));
    }
  }

  // Check if rebuild is needed
  bool need_rebuild() const {
    try {
      return indexer_->need_rebuild();
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to check rebuild status: " +
                               std::string(e.what()));
    }
  }

  // Check if indexer is valid
  bool is_valid() const { 
    return indexer_->is_valid(); 
  }

  // Get maximum bytes
  uint64_t get_max_bytes() const {
    try {
      return indexer_->get_max_bytes();
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to get maximum bytes: " +
                               std::string(e.what()));
    }
  }

  // Get number of lines
  uint64_t get_num_lines() const {
    try {
      return indexer_->get_num_lines();
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to get number of lines: " +
                               std::string(e.what()));
    }
  }

  // Find file ID
  int find_file_id(const std::string &gz_path) const {
    try {
      return indexer_->find_file_id(gz_path);
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to find file ID: " +
                               std::string(e.what()));
    }
  }

  // Get all checkpoints
  std::vector<dftracer::utils::indexer::CheckpointInfo> get_checkpoints() const {
    try {
      return indexer_->get_checkpoints();
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to get checkpoints: " +
                               std::string(e.what()));
    }
  }

  // Find checkpoints by line range
  std::vector<dftracer::utils::indexer::CheckpointInfo>
  find_checkpoints_by_line_range(size_t start_line, size_t end_line) const {
    try {
      return indexer_->find_checkpoints_by_line_range(start_line, end_line);
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to find checkpoints by line range: " +
                               std::string(e.what()));
    }
  }

  // Find checkpoint for target offset
  std::optional<dftracer::utils::indexer::CheckpointInfo> find_checkpoint(size_t target_offset) const {
    try {
      dftracer::utils::indexer::CheckpointInfo checkpoint;
      bool found = indexer_->find_checkpoint(target_offset, checkpoint);
      if (found) {
        return checkpoint;
      } else {
        return std::nullopt;
      }
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to find checkpoint: " +
                               std::string(e.what()));
    }
  }
  
  // Getters
  std::string gz_path() const { 
    return gz_path_; 
  }

  std::string idx_path() const { 
    return idx_path_; 
  }

  size_t checkpoint_size() const { 
    return checkpoint_size_; 
  }

  dftracer::utils::indexer::Indexer* get_indexer_ptr() const {
    return indexer_.get();
  }

  // Context manager methods
  DFTracerIndexer& __enter__() {
    return *this;
  }

  bool __exit__(nanobind::args args) {
    // Nothing special to do on exit for indexer - it's RAII managed
    return false;  // Don't suppress exceptions
  }
};

#endif
