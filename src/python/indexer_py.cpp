#include "indexer_py.h"
#include <dftracer/utils/indexer/indexer.h>
#include <nanobind/nanobind.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>


// DFTracerIndexer implementation
DFTracerIndexer::DFTracerIndexer(const std::string &gz_path,
                                 const std::optional<std::string> &idx_path,
                                 size_t checkpoint_size,
                                 bool force_rebuild)
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

void DFTracerIndexer::build() {
  try {
    indexer_->build();
  } catch (const std::runtime_error &e) {
    throw std::runtime_error("Failed to build index: " + std::string(e.what()));
  }
}

bool DFTracerIndexer::need_rebuild() const {
  try {
    return indexer_->need_rebuild();
  } catch (const std::runtime_error &e) {
    throw std::runtime_error("Failed to check rebuild status: " +
                             std::string(e.what()));
  }
}

bool DFTracerIndexer::is_valid() const { 
  return indexer_->is_valid(); 
}

uint64_t DFTracerIndexer::get_max_bytes() const {
  try {
    return indexer_->get_max_bytes();
  } catch (const std::runtime_error &e) {
    throw std::runtime_error("Failed to get maximum bytes: " +
                             std::string(e.what()));
  }
}

uint64_t DFTracerIndexer::get_num_lines() const {
  try {
    return indexer_->get_num_lines();
  } catch (const std::runtime_error &e) {
    throw std::runtime_error("Failed to get number of lines: " +
                             std::string(e.what()));
  }
}

int DFTracerIndexer::find_file_id(const std::string &gz_path) const {
  try {
    return indexer_->find_file_id(gz_path);
  } catch (const std::runtime_error &e) {
    throw std::runtime_error("Failed to find file ID: " +
                             std::string(e.what()));
  }
}


std::vector<dftracer::utils::indexer::CheckpointInfo> DFTracerIndexer::get_checkpoints() const {
  try {
    return indexer_->get_checkpoints();
  } catch (const std::runtime_error &e) {
    throw std::runtime_error("Failed to get checkpoints: " +
                             std::string(e.what()));
  }
}

std::vector<dftracer::utils::indexer::CheckpointInfo>
DFTracerIndexer::find_checkpoints_by_line_range(size_t start_line, size_t end_line) const {
  try {
    return indexer_->find_checkpoints_by_line_range(start_line, end_line);
  } catch (const std::runtime_error &e) {
    throw std::runtime_error("Failed to find checkpoints by line range: " +
                             std::string(e.what()));
  }
}

std::optional<dftracer::utils::indexer::CheckpointInfo> DFTracerIndexer::find_checkpoint(size_t target_offset) const {
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

std::string DFTracerIndexer::gz_path() const { 
  return gz_path_; 
}

std::string DFTracerIndexer::idx_path() const { 
  return idx_path_; 
}

size_t DFTracerIndexer::checkpoint_size() const { 
  return checkpoint_size_; 
}

dftracer::utils::indexer::Indexer* DFTracerIndexer::get_indexer_ptr() const {
  return indexer_.get();
}

// Context manager methods
DFTracerIndexer& DFTracerIndexer::__enter__() {
  return *this;
}

bool DFTracerIndexer::__exit__(nanobind::args args) {
  // Nothing special to do on exit for indexer - it's RAII managed
  return false;  // Don't suppress exceptions
}
