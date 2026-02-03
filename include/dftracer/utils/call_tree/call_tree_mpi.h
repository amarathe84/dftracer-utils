#ifndef DFTRACER_UTILS_CALL_TREE_MPI_H
#define DFTRACER_UTILS_CALL_TREE_MPI_H

/**
 * @file call_tree_mpi.h
 * @brief Umbrella header for MPI-parallel call tree components
 *
 * This header provides convenient access to all MPI-related call tree
 * functionality. Individual components can also be included separately
 * from the mpi/ subdirectory for finer-grained control.
 *
 * Components included:
 * - PIDIndexInfo: PID index information structure
 * - SerializableCallNode/ProcessGraph: Serializable structures for MPI transfer
 * - MPICallTreeConfig/Result: Configuration and result structures
 * - CallGraphFileHeader: File header for call graph serialization
 * - MPICallTreeBuilder: Main builder class for MPI-parallel call graph
 * generation
 * - MPIFilteredTraceReader: Filtered trace reader for specific PIDs
 * - CallTreeBuildTask: Pipeline task for call tree building
 * - serialization utilities: Read/write primitives for MPI serialization
 */

// Include all MPI call tree components
#include <dftracer/utils/call_tree/mpi/build_task.h>
#include <dftracer/utils/call_tree/mpi/builder.h>
#include <dftracer/utils/call_tree/mpi/config.h>
#include <dftracer/utils/call_tree/mpi/file_header.h>
#include <dftracer/utils/call_tree/mpi/filtered_reader.h>
#include <dftracer/utils/call_tree/mpi/pid_index_info.h>
#include <dftracer/utils/call_tree/mpi/serializable.h>
#include <dftracer/utils/call_tree/mpi/serialization.h>

// Include specific internal call tree components needed by MPI
#include <dftracer/utils/call_tree/internal/call_tree.h>
#include <dftracer/utils/call_tree/internal/process_call_tree.h>
#include <dftracer/utils/call_tree/internal/process_key.h>

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_H
