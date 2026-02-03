#ifndef DFTRACER_UTILS_CALL_TREE_INTERNAL_NODE_H
#define DFTRACER_UTILS_CALL_TREE_INTERNAL_NODE_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::call_tree {
namespace internal {

/**
 * CallTreeNode - Represents a single function call in the trace
 * Follows initialization pattern:
 * 1. Constructor: Initialize internal variables, pointers to defaults (no
 * allocation)
 * 2. initialize(): Initialize state and perform allocations
 * 3. cleanup(): Deallocate memory and clean up state
 * 4. Destructor: Clear all state and reset variables
 */
class CallTreeNode {
   public:
    /**
     * Constructor - initializes internal variables and pointers to defaults
     * No memory allocation or recursion
     */
    CallTreeNode();

    /**
     * Parameterized constructor for setting basic properties
     */
    CallTreeNode(std::uint64_t id, const std::string& name,
                 const std::string& category);

    /**
     * Destructor - clears all state of variables and resets them
     */
    ~CallTreeNode();

    // Disable copy operations to prevent unintended copies
    CallTreeNode(const CallTreeNode&) = delete;
    CallTreeNode& operator=(const CallTreeNode&) = delete;

    // Enable move operations for efficient transfers
    CallTreeNode(CallTreeNode&& other) noexcept;
    CallTreeNode& operator=(CallTreeNode&& other) noexcept;

    /**
     * Initialize the state of class private variables and allocations
     * Called after constructor to set up the node with specific values
     */
    void initialize(std::uint64_t id, const std::string& name,
                    const std::string& category, std::uint64_t start_time,
                    std::uint64_t duration, int level);

    /**
     * Cleanup - deallocates memory and cleans up state
     * Called only at the end, ensures no memory leaks
     */
    void cleanup();

    // Getters
    std::uint64_t get_id() const { return id_; }
    const std::string& get_name() const { return name_; }
    const std::string& get_category() const { return category_; }
    std::uint64_t get_start_time() const { return start_time_; }
    std::uint64_t get_duration() const { return duration_; }
    int get_level() const { return level_; }
    std::uint64_t get_parent_id() const { return parent_id_; }
    const std::unordered_map<std::string, std::string>& get_args() const {
        return args_;
    }
    const std::vector<std::uint64_t>& get_children() const { return children_; }

    // Setters
    void set_parent_id(std::uint64_t parent_id) { parent_id_ = parent_id; }
    void add_child(std::uint64_t child_id) { children_.push_back(child_id); }
    void add_arg(const std::string& key, const std::string& value) {
        args_[key] = value;
    }
    void set_args(const std::unordered_map<std::string, std::string>& args) {
        args_ = args;
    }

   private:
    std::uint64_t id_;
    std::string name_;
    std::string category_;
    std::uint64_t start_time_;
    std::uint64_t duration_;
    int level_;
    std::uint64_t parent_id_;
    std::unordered_map<std::string, std::string> args_;
    std::vector<std::uint64_t> children_;
    bool initialized_;
    bool cleaned_up_;
};

// Keep FunctionCall as alias for backward compatibility
using FunctionCall = CallTreeNode;

}  // namespace internal
}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_INTERNAL_NODE_H
