#ifndef DFTRACER_UTILS_CALL_TREE_INTERNAL_H
#define DFTRACER_UTILS_CALL_TREE_INTERNAL_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <cstdint>
#include <functional>

namespace dftracer::utils::call_tree {
namespace internal {

/**
 * CallTreeNode - Represents a single function call in the trace
 * Follows initialization pattern:
 * 1. Constructor: Initialize internal variables, pointers to defaults (no allocation)
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
    CallTreeNode(std::uint64_t id, const std::string& name, const std::string& category);
    
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
    void initialize(std::uint64_t id, const std::string& name, const std::string& category,
                   std::uint64_t start_time, std::uint64_t duration, int level);
    
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
    const std::unordered_map<std::string, std::string>& get_args() const { return args_; }
    const std::vector<std::uint64_t>& get_children() const { return children_; }
    
    // Setters
    void set_parent_id(std::uint64_t parent_id) { parent_id_ = parent_id; }
    void add_child(std::uint64_t child_id) { children_.push_back(child_id); }
    void add_arg(const std::string& key, const std::string& value) { args_[key] = value; }
    void set_args(const std::unordered_map<std::string, std::string>& args) { args_ = args; }
    
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

/**
 * Composite key for identifying process/thread/node combination
 */
struct ProcessKey {
    std::uint32_t pid;        // Process ID
    std::uint32_t tid;        // Thread ID  
    std::uint32_t node_id;    // Node ID (or hash of node name)
    
    ProcessKey(std::uint32_t p = 0, std::uint32_t t = 0, std::uint32_t n = 0)
        : pid(p), tid(t), node_id(n) {}
    
    bool operator==(const ProcessKey& other) const {
        return pid == other.pid && tid == other.tid && node_id == other.node_id;
    }
    
    bool operator!=(const ProcessKey& other) const {
        return !(*this == other);
    }
};

// call graph for one process/thread/node combination
struct ProcessCallTree {
    ProcessKey key;
    std::unordered_map<std::uint64_t, std::shared_ptr<CallTreeNode>> calls;
    std::vector<std::uint64_t> root_calls; // top level calls
    std::vector<std::uint64_t> call_sequence; // order they appear in trace
    
    ProcessCallTree() = default;
    ~ProcessCallTree() = default;
};

} // namespace internal
} // namespace dftracer::utils::call_tree

// Hash function for ProcessKey to use in unordered_map
namespace std {
    template<>
    struct hash<dftracer::utils::call_tree::internal::ProcessKey> {
        size_t operator()(const dftracer::utils::call_tree::internal::ProcessKey& k) const {
            return ((hash<uint32_t>()(k.pid)
                    ^ (hash<uint32_t>()(k.tid) << 1)) >> 1)
                    ^ (hash<uint32_t>()(k.node_id) << 1);
        }
    };
}

namespace dftracer::utils::call_tree {
namespace internal {

/**
 * Callback function type for processing traces
 * Returns true to continue processing, false to stop
 */
using TraceCallback = std::function<bool(const std::string& json_line)>;

// Forward declaration
class CallTree;

/**
 * CallTreeFactory - Factory for creating and managing CallTreeNode objects
 * Follows initialization pattern:
 * 1. Constructor: Initialize internal variables (no allocation)
 * 2. initialize(): Initialize state and prepare for node creation
 * 3. cleanup(): Deallocate all nodes and clean up state
 * 4. Destructor: Clear all state
 */
class CallTreeFactory {
public:
    /**
     * Constructor - initializes internal variables to defaults
     */
    CallTreeFactory();
    
    /**
     * Destructor - clears all state
     */
    ~CallTreeFactory();
    
    /**
     * Initialize the factory state
     */
    void initialize();
    
    /**
     * Cleanup - deallocates all managed nodes
     */
    void cleanup();
    
    /**
     * Create a new CallTreeNode from trace event data
     * The factory manages the lifecycle of created nodes
     */
    std::shared_ptr<CallTreeNode> create_node(
        std::uint64_t id,
        const std::string& name, 
        const std::string& category,
        std::uint64_t start_time,
        std::uint64_t duration,
        int level,
        const std::unordered_map<std::string, std::string>& args = {});
    
    /**
     * Get total number of nodes created by this factory
     */
    size_t get_node_count() const { return node_count_; }
    
private:
    size_t node_count_;
    bool initialized_;
    bool cleaned_up_;
    
    // Track all nodes for proper cleanup
    std::vector<std::shared_ptr<CallTreeNode>> managed_nodes_;
};

/**
 * TraceReader - Handles reading and parsing trace files
 * Separates I/O concerns from the CallTree data structure
 * Supports reading from single files, multiple files, or directories
 */
class TraceReader {
public:
    TraceReader() = default;
    ~TraceReader() = default;
    
    /**
     * Read trace file and populate call graph
     * @param trace_file Path to trace log file
     * @param graph CallTree to populate
     * @return true if successful, false otherwise
     */
    bool read(const std::string& trace_file, CallTree& graph);
    
    /**
     * Read multiple trace files and populate call graph
     * Each file may contain traces from different nodes/processes
     * @param trace_files Vector of paths to trace files
     * @param graph CallTree to populate
     * @return true if all files read successfully, false otherwise
     */
    bool read_multiple(const std::vector<std::string>& trace_files, CallTree& graph);
    
    /**
     * Read all trace files matching pattern from a directory
     * @param directory Path to directory containing trace files
     * @param pattern Glob pattern for trace files (e.g., "*.pfw")
     * @param graph CallTree to populate
     * @return true if successful, false otherwise
     */
    bool read_directory(const std::string& directory, const std::string& pattern, CallTree& graph);
    
    /**
     * Process a single JSON trace line
     * Made public for MPI-based filtered readers
     * @param line JSON line from trace file
     * @param graph CallTree to add data to
     * @return true if successful, false otherwise
     */
    bool process_trace_line(const std::string& line, CallTree& graph);

private:
    /**
     * Detect file format and use appropriate reader
     * Returns true if read with Reader API, false if need fallback
     */
    bool read_with_reader(const std::string& trace_file, CallTree& graph);
    
    /**
     * Fallback to direct file reading for plain text files
     */
    bool read_direct(const std::string& trace_file, CallTree& graph);
};

/**
 * Main call graph - Container for all process call graphs
 * Acts as a map-like structure that returns ProcessCallTree nodes by ProcessKey
 * Modern C++ API design:
 * - Constructor takes log file (no separate load method)
 * - Simplified method names based on return types
 * - Support for composite keys (PID, TID, NodeID)
 * 
 * Follows initialization pattern:
 * 1. Constructor: Initialize internal variables (no allocation, no file loading)
 * 2. initialize(): Initialize state and prepare for data
 * 3. cleanup(): Deallocate memory and clean up state
 * 4. Destructor: Clear all state
 */
class CallTree {
public:
    /**
     * Default constructor for empty call graph
     * Only initializes internal variables to defaults
     */
    CallTree();
    
    /**
     * Construct call graph (note: does not load data, call initialize/load separately)
     * @param log_file Path to trace log file (stored for later use)
     */
    explicit CallTree(const std::string& log_file);
    
    /**
     * Destructor - clears all state
     */
    ~CallTree();
    
    /**
     * Initialize the call graph state and factory
     * Must be called before adding data
     */
    void initialize();
    
    /**
     * Cleanup - deallocates all memory and cleans up state
     * Call at the end to ensure no memory leaks
     */
    void cleanup();
    
    /**
     * Get call graph for specific process/thread/node
     * Simplified name - return type tells the story
     */
    ProcessCallTree* get(const ProcessKey& key);
    
    /**
     * Convenience overload for get with separate parameters
     */
    ProcessCallTree* get(std::uint32_t pid, std::uint32_t tid = 0, std::uint32_t node_id = 0);
    
    /**
     * Operator overload for natural C++ access
     */
    ProcessCallTree& operator[](const ProcessKey& key);
    
    /**
     * Get all process keys in the call graph
     * Renamed from get_process_ids to reflect composite key
     */
    std::vector<ProcessKey> keys() const;
    
    /**
     * Print call graph for specific process/thread/node
     * Simplified name
     */
    void print(const ProcessKey& key) const;
    
    /**
     * Convenience overload for print with separate parameters
     */
    void print(std::uint32_t pid, std::uint32_t tid = 0, std::uint32_t node_id = 0) const;
    
    /**
     * Check if call graph is empty
     */
    bool empty() const { return process_graphs_.empty(); }
    
    /**
     * Get number of process/thread/node combinations
     */
    size_t size() const { return process_graphs_.size(); }
    
    /**
     * Add a function call to the appropriate process graph
     * Used by TraceReader to populate the graph
     */
    void add_call(const ProcessKey& key, std::shared_ptr<CallTreeNode> call);
    
    /**
     * Build parent-child relationships after all traces loaded
     * Called by TraceReader after all data is loaded
     */
    void build_hierarchy();
    
    /**
     * Build hierarchy for a specific process (lazy/on-demand)
     * @param key ProcessKey to build hierarchy for
     */
    void build_hierarchy_for_process(const ProcessKey& key);
    
    /**
     * Get the factory for creating nodes
     */
    CallTreeFactory& get_factory() { return factory_; }

private:
    friend class TraceReader;
    std::unordered_map<ProcessKey, std::unique_ptr<ProcessCallTree>> process_graphs_;
    CallTreeFactory factory_;
    std::string log_file_;
    bool initialized_;
    bool cleaned_up_;
    
    /**
     * Load call graph from trace file (moved to private)
     * Delegates to TraceReader for actual I/O
     */
    bool load(const std::string& trace_file);
    
    /**
     * Build hierarchy for a single ProcessCallTree
     */
    void build_hierarchy_internal(ProcessCallTree* graph);
    
    /**
     * Print calls recursively 
     */
    void print_calls_recursive(const ProcessCallTree& graph, std::uint64_t call_id, int indent) const;
};

} // namespace internal
} // namespace dftracer::utils::call_tree

#endif // DFTRACER_UTILS_CALL_TREE_INTERNAL_H