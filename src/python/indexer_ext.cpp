#include "indexer_ext.h"
#include <dftracer/utils/indexer/indexer.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace nb = nanobind;

using namespace nb::literals;

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

dftracer::utils::indexer::Indexer* DFTracerIndexer::operator*() {
  return indexer_.get();
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

bool DFTracerIndexer::find_checkpoint(size_t target_offset,
                                     dftracer::utils::indexer::CheckpointInfo &checkpoint) const {
  try {
    return indexer_->find_checkpoint(target_offset, checkpoint);
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

NB_MODULE(indexer_ext, m) {
  m.doc() = "DFTracer utilities indexer extension";

  nb::class_<dftracer::utils::indexer::CheckpointInfo>(m, "CheckpointInfo")
      .def_rw("checkpoint_idx", &dftracer::utils::indexer::CheckpointInfo::checkpoint_idx,
              "Checkpoint index")
      .def_rw("uc_offset", &dftracer::utils::indexer::CheckpointInfo::uc_offset,
              "Uncompressed offset")
      .def_rw("uc_size", &dftracer::utils::indexer::CheckpointInfo::uc_size,
              "Uncompressed size")
      .def_rw("c_offset", &dftracer::utils::indexer::CheckpointInfo::c_offset,
              "Compressed offset")
      .def_rw("c_size", &dftracer::utils::indexer::CheckpointInfo::c_size,
              "Compressed size")
      .def_rw("bits", &dftracer::utils::indexer::CheckpointInfo::bits,
              "Bit position")
      .def_rw("dict_compressed", &dftracer::utils::indexer::CheckpointInfo::dict_compressed,
              "Compressed dictionary")
      .def_rw("num_lines", &dftracer::utils::indexer::CheckpointInfo::num_lines,
              "Number of lines in this chunk");

  nb::class_<DFTracerIndexer>(m, "DFTracerIndexer")
      .def(nb::init<const std::string &, const std::optional<std::string> &,
                    size_t, bool>(),
           "gz_path"_a, "idx_path"_a = nb::none(),
           "checkpoint_size"_a = dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
           "force_rebuild"_a = false,
           "Create a DFTracer indexer for a gzip file and its index")
      .def("build", &DFTracerIndexer::build, "Build or rebuild the index")
      .def("need_rebuild", &DFTracerIndexer::need_rebuild,
           "Check if a rebuild is needed")
      .def("is_valid", &DFTracerIndexer::is_valid,
           "Check if the indexer is valid")
      .def("get_max_bytes", &DFTracerIndexer::get_max_bytes,
           "Get the maximum uncompressed bytes in the indexed file")
      .def("get_num_lines", &DFTracerIndexer::get_num_lines,
           "Get the total number of lines in the indexed file")
      .def("find_file_id", &DFTracerIndexer::find_file_id, "gz_path"_a,
           "Find the database file ID for a given gzip path")
      .def("get_checkpoints", &DFTracerIndexer::get_checkpoints,
           "Get all checkpoints for this file as a list")
      .def("find_checkpoints_by_line_range",
           &DFTracerIndexer::find_checkpoints_by_line_range, "start_line"_a,
           "end_line"_a,
           "Find checkpoints that contain data for a specific line range")
      .def("find_checkpoint", &DFTracerIndexer::find_checkpoint, "target_offset"_a,
           "checkpoint"_a, "Find the best checkpoint for a given uncompressed offset")
      .def_prop_ro("gz_path", &DFTracerIndexer::gz_path, "Path to the gzip file")
      .def_prop_ro("idx_path", &DFTracerIndexer::idx_path, "Path to the index file")
      .def_prop_ro("checkpoint_size", &DFTracerIndexer::checkpoint_size,
                   "Checkpoint size in bytes")
      .def("get_indexer_ptr", &DFTracerIndexer::get_indexer_ptr,
           nb::rv_policy::reference_internal,
           "Get the underlying indexer pointer (for passing to reader)");

  // Alias DFTracerIndexer as DFTracerIndexer for common use
  m.attr("DFTracerIndexer") = m.attr("DFTracerIndexer");
}
