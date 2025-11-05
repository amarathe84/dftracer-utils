#ifndef DFTRACER_UTILS_CALL_GRAPH_H
#define DFTRACER_UTILS_CALL_GRAPH_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <cstdint>
#include <functional>

namespace dftracer::utils::call_graph {

/**
 * CallGraphNode - Represents a single function call in the trace
 * Follows initialization pattern:
 * 1. Constructor: Initialize internal variables, pointers to defaults (no allocation)
 * 2. initialize(): Initialize state and perform allocations
 * 3. cleanup(): Deallocate memory and clean up state
 * 4. Destructor: Clear all state and reset variables
 */
class CallGraphNode {
public:
    /**
     * Constructor - initializes internal variables and pointers to defaults
     * No memory allocation or recursion
     */
    CallGraphNode();
    
    /**
     * Parameterized constructor for setting basic properties
     */
    CallGraphNode(std::uint64_t id, const std::string& name, const std::string& category);
    
    /**
     * Destructor - clears all state of variables and resets them
     */
    ~CallGraphNode();
    
    // Disable copy operations to prevent unintended copies
    CallGraphNode(const CallGraphNode&) = delete;
    CallGraphNode& operator=(const CallGraphNode&) = delete;
    
    // Enable move operations for efficient transfers
    CallGraphNode(CallGraphNode&& other) noexcept;
    CallGraphNode& operator=(CallGraphNode&& other) noexcept;
    
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
using FunctionCall = CallGraphNode;

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
struct ProcessCallGraph {
    ProcessKey key;
    std::unordered_map<std::uint64_t, std::shared_ptr<CallGraphNode>> calls;
    std::vector<std::uint64_t> root_calls; // top level calls
    std::vector<std::uint64_t> call_sequence; // order they appear in trace
    
    ProcessCallGraph() = default;
    ~ProcessCallGraph() = default;
};

} // namespace dftracer::utils::call_graph

// Hash function for ProcessKey to use in unordered_map
namespace std {
    template<>
    struct hash<dftracer::utils::call_graph::ProcessKey> {
        size_t operator()(const dftracer::utils::call_graph::ProcessKey& k) const {
            return ((hash<uint32_t>()(k.pid)
                    ^ (hash<uint32_t>()(k.tid) << 1)) >> 1)
                    ^ (hash<uint32_t>()(k.node_id) << 1);
        }
    };
}

namespace dftracer::utils::call_graph {

/**
 * Callback function type for processing traces
 * Returns true to continue processing, false to stop
 */
using TraceCallback = std::function<bool(const std::string& json_line)>;

// Forward declaration
class CallGraph;

/**
 * CallGraphFactory - Factory for creating and managing CallGraphNode objects
 * Follows initialization pattern:
 * 1. Constructor: Initialize internal variables (no allocation)
 * 2. initialize(): Initialize state and prepare for node creation
 * 3. cleanup(): Deallocate all nodes and clean up state
 * 4. Destructor: Clear all state
 */
class CallGraphFactory {
public:
    /**
     * Constructor - initializes internal variables to defaults
     */
    CallGraphFactory();
    
    /**
     * Destructor - clears all state
     */
    ~CallGraphFactory();
    
    /**
     * Initialize the factory state
     */
    void initialize();
    
    /**
     * Cleanup - deallocates all managed nodes
     */
    void cleanup();
    
    /**
     * Create a new CallGraphNode from trace event data
     * The factory manages the lifecycle of created nodes
     */
    std::shared_ptr<CallGraphNode> create_node(
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
    std::vector<std::shared_ptr<CallGraphNode>> managed_nodes_;
};

/**
 * TraceReader - Handles reading and parsing trace files
 * Separates I/O concerns from the CallGraph data structure
 * Supports reading from single files, multiple files, or directories
 */
class TraceReader {
public:
    TraceReader() = default;
    ~TraceReader() = default;
    
    /**
     * Read trace file and populate call graph
     * @param trace_file Path to trace log file
     * @param graph CallGraph to populate
     * @return true if successful, false otherwise
     */
    bool read(const std::string& trace_file, CallGraph& graph);
    
    /**
     * Read multiple trace files and populate call graph
     * Each file may contain traces from different nodes/processes
     * @param trace_files Vector of paths to trace files
     * @param graph CallGraph to populate
     * @return true if all files read successfully, false otherwise
     */
    bool read_multiple(const std::vector<std::string>& trace_files, CallGraph& graph);
    
    /**
     * Read all trace files matching pattern from a directory
     * @param directory Path to directory containing trace files
     * @param pattern Glob pattern for trace files (e.g., "*.pfw")
     * @param graph CallGraph to populate
     * @return true if successful, false otherwise
     */
    bool read_directory(const std::string& directory, const std::string& pattern, CallGraph& graph);
    
    /**
     * Process a single JSON trace line
     * Made public for MPI-based filtered readers
     * @param line JSON line from trace file
     * @param graph CallGraph to add data to
     * @return true if successful, false otherwise
     */
    bool process_trace_line(const std::string& line, CallGraph& graph);

private:
    /**
     * Detect file format and use appropriate reader
     * Returns true if read with Reader API, false if need fallback
     */
    bool read_with_reader(const std::string& trace_file, CallGraph& graph);
    
    /**
     * Fallback to direct file reading for plain text files
     */
    bool read_direct(const std::string& trace_file, CallGraph& graph);
};

/**
 * Main call graph - Container for all process call graphs
 * Acts as a map-like structure that returns ProcessCallGraph nodes by ProcessKey
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
class CallGraph {
public:
    /**
     * Default constructor for empty call graph
     * Only initializes internal variables to defaults
     */
    CallGraph();
    
    /**
     * Construct call graph (note: does not load data, call initialize/load separately)
     * @param log_file Path to trace log file (stored for later use)
     */
    explicit CallGraph(const std::string& log_file);
    
    /**
     * Destructor - clears all state
     */
    ~CallGraph();
    
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
    ProcessCallGraph* get(const ProcessKey& key);
    
    /**
     * Convenience overload for get with separate parameters
     */
    ProcessCallGraph* get(std::uint32_t pid, std::uint32_t tid = 0, std::uint32_t node_id = 0);
    
    /**
     * Operator overload for natural C++ access
     */
    ProcessCallGraph& operator[](const ProcessKey& key);
    
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
    void add_call(const ProcessKey& key, std::shared_ptr<CallGraphNode> call);
    
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
    CallGraphFactory& get_factory() { return factory_; }

private:
    friend class TraceReader;
    std::unordered_map<ProcessKey, std::unique_ptr<ProcessCallGraph>> process_graphs_;
    CallGraphFactory factory_;
    std::string log_file_;
    bool initialized_;
    bool cleaned_up_;
    
    /**
     * Load call graph from trace file (moved to private)
     * Delegates to TraceReader for actual I/O
     */
    bool load(const std::string& trace_file);
    
    /**
     * Build hierarchy for a single ProcessCallGraph
     */
    void build_hierarchy_internal(ProcessCallGraph* graph);
    
    /**
     * Print calls recursively 
     */
    void print_calls_recursive(const ProcessCallGraph& graph, std::uint64_t call_id, int indent) const;
};

} // namespace dftracer::utils::call_graph

#endif // DFTRACER_UTILS_CALL_GRAPH_H