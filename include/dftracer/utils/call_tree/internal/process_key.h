#ifndef DFTRACER_UTILS_CALL_TREE_INTERNAL_PROCESS_KEY_H
#define DFTRACER_UTILS_CALL_TREE_INTERNAL_PROCESS_KEY_H

#include <cstdint>
#include <functional>

namespace dftracer::utils::call_tree {
namespace internal {

/**
 * Composite key for identifying process/thread/node combination
 */
struct ProcessKey {
    std::uint32_t pid;      // Process ID
    std::uint32_t tid;      // Thread ID
    std::uint32_t node_id;  // Node ID (or hash of node name)

    ProcessKey(std::uint32_t p = 0, std::uint32_t t = 0, std::uint32_t n = 0)
        : pid(p), tid(t), node_id(n) {}

    bool operator==(const ProcessKey& other) const {
        return pid == other.pid && tid == other.tid && node_id == other.node_id;
    }

    bool operator!=(const ProcessKey& other) const { return !(*this == other); }
};

}  // namespace internal
}  // namespace dftracer::utils::call_tree

// Hash function for ProcessKey to use in unordered_map
namespace std {
template <>
struct hash<dftracer::utils::call_tree::internal::ProcessKey> {
    size_t operator()(
        const dftracer::utils::call_tree::internal::ProcessKey& k) const {
        return ((hash<uint32_t>()(k.pid) ^ (hash<uint32_t>()(k.tid) << 1)) >>
                1) ^
               (hash<uint32_t>()(k.node_id) << 1);
    }
};
}  // namespace std

#endif  // DFTRACER_UTILS_CALL_TREE_INTERNAL_PROCESS_KEY_H
