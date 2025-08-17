#include <nanobind/nanobind.h>

#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include "indexer_py.h"

namespace nb = nanobind;
using namespace nb::literals;

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
           "Find the best checkpoint for a given uncompressed offset, returns CheckpointInfo or None")
      .def_prop_ro("gz_path", &DFTracerIndexer::gz_path, "Path to the gzip file")
      .def_prop_ro("idx_path", &DFTracerIndexer::idx_path, "Path to the index file")
      .def_prop_ro("checkpoint_size", &DFTracerIndexer::checkpoint_size,
                   "Checkpoint size in bytes")
      .def("__enter__", &DFTracerIndexer::__enter__,
           nb::rv_policy::reference_internal, "Enter context manager")
      .def("__exit__", &DFTracerIndexer::__exit__, "Exit context manager");

  // Alias DFTracerIndexer as DFTracerIndexer for common use
  m.attr("DFTracerIndexer") = m.attr("DFTracerIndexer");
}
