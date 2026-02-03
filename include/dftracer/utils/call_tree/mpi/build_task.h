#ifndef DFTRACER_UTILS_CALL_TREE_MPI_BUILD_TASK_H
#define DFTRACER_UTILS_CALL_TREE_MPI_BUILD_TASK_H

/**
 * @file build_task.h
 * @brief Pipeline-based call graph builder task
 */

#include <dftracer/utils/call_tree/internal/call_tree.h>
#include <dftracer/utils/call_tree/internal/process_call_tree.h>

#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace dftracer::utils::call_tree {

/**
 * Pipeline-based call graph builder task
 * Input: vector of trace files
 * Output: internal::ProcessCallTree for assigned PIDs
 */
struct CallTreeBuildTask {
    std::set<std::uint32_t> pids;
    std::vector<std::string> trace_files;

    internal::ProcessCallTree execute(internal::CallTree& tree);
};

}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_BUILD_TASK_H
