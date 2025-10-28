#ifndef DFTRACER_UTILS_CALL_GRAPH_H
#define DFTRACER_UTILS_CALL_GRAPH_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <cstdint>
#include <functional>

namespace dftracer::utils::call_graph {

// holds info for each function call
struct FunctionCall {
    std::uint64_t id;
    std::string name;
    std::string category;
    std::uint64_t start_time;
    std::uint64_t duration;
    int level;
    std::uint64_t parent_id;
    std::unordered_map<std::string, std::string> args; // extra stuff from trace
    std::vector<std::uint64_t> children; // child calls
};

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
    std::unordered_map<std::uint64_t, std::shared_ptr<FunctionCall>> calls;
    std::vector<std::uint64_t> root_calls; // top level calls
    std::vector<std::uint64_t> call_sequence; // order they appear in trace
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
    
private:
    /**
     * Process a single JSON trace line
     * @param line JSON line from trace file
     * @param graph CallGraph to add data to
     * @return true if successful, false otherwise
     */
    bool process_trace_line(const std::string& line, CallGraph& graph);
};

/**
 * Main call graph - Container for all process call graphs
 * Acts as a map-like structure that returns ProcessCallGraph nodes by ProcessKey
 * Modern C++ API design:
 * - Constructor takes log file (no separate load method)
 * - Simplified method names based on return types
 * - Support for composite keys (PID, TID, NodeID)
 */
class CallGraph {
public:
    /**
     * Default constructor for empty call graph
     */
    CallGraph() = default;
    
    /**
     * Construct and load call graph from trace file
     * @param log_file Path to trace log file
     */
    explicit CallGraph(const std::string& log_file);
    
    ~CallGraph();
    
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
    void add_call(const ProcessKey& key, std::shared_ptr<FunctionCall> call);
    
    /**
     * Build parent-child relationships after all traces loaded
     * Called by TraceReader after all data is loaded
     */
    void build_hierarchy();

private:
    friend class TraceReader;
    std::unordered_map<ProcessKey, std::unique_ptr<ProcessCallGraph>> process_graphs_;
    
    /**
     * Load call graph from trace file (moved to private)
     * Delegates to TraceReader for actual I/O
     */
    bool load(const std::string& trace_file);
    
    /**
     * Print calls recursively 
     */
    void print_calls_recursive(const ProcessCallGraph& graph, std::uint64_t call_id, int indent) const;
};

} // namespace dftracer::utils::call_graph

#endif // DFTRACER_UTILS_CALL_GRAPH_H