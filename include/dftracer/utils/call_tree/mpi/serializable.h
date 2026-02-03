#ifndef DFTRACER_UTILS_CALL_TREE_MPI_SERIALIZABLE_H
#define DFTRACER_UTILS_CALL_TREE_MPI_SERIALIZABLE_H

/**
 * @file serializable.h
 * @brief Serializable structures for MPI transfer of call graph data
 */

#include <dftracer/utils/call_tree/internal/process_key.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::call_tree {

/**
 * Serializable call graph node for MPI transfer
 */
struct SerializableCallNode {
    std::uint64_t id;
    std::string name;
    std::string category;
    std::uint64_t start_time;
    std::uint64_t duration;
    int level;
    std::uint64_t parent_id;
    std::vector<std::uint64_t> children;
    std::unordered_map<std::string, std::string> args;

    // Serialization to bytes
    std::vector<char> serialize() const;
    static SerializableCallNode deserialize(const char* data, size_t& offset);
};

/**
 * Serializable process call graph for MPI transfer
 */
struct SerializableProcessGraph {
    internal::ProcessKey key;
    std::vector<SerializableCallNode> nodes;
    std::vector<std::uint64_t> root_calls;
    std::vector<std::uint64_t> call_sequence;

    // Serialization to bytes
    std::vector<char> serialize() const;
    static SerializableProcessGraph deserialize(const char* data,
                                                size_t& offset);
};

}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_SERIALIZABLE_H
