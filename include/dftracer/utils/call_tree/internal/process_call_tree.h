#ifndef DFTRACER_UTILS_CALL_TREE_INTERNAL_PROCESS_CALL_TREE_H
#define DFTRACER_UTILS_CALL_TREE_INTERNAL_PROCESS_CALL_TREE_H

#include <dftracer/utils/call_tree/internal/node.h>
#include <dftracer/utils/call_tree/internal/process_key.h>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::call_tree {
namespace internal {

/**
 * ProcessCallTree - Call graph for one process/thread/node combination
 * Contains all function calls and their relationships for a single execution
 * context
 */
struct ProcessCallTree {
    ProcessKey key;
    std::unordered_map<std::uint64_t, std::shared_ptr<CallTreeNode>> calls;
    std::vector<std::uint64_t> root_calls;     // top level calls
    std::vector<std::uint64_t> call_sequence;  // order they appear in trace

    ProcessCallTree() = default;
    ~ProcessCallTree() = default;
};

}  // namespace internal
}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_INTERNAL_PROCESS_CALL_TREE_H
