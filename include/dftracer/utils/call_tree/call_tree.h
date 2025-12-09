#ifndef DFTRACER_UTILS_CALL_TREE_H
#define DFTRACER_UTILS_CALL_TREE_H

#include <string>
#include <vector>
#include <cstdint>
#include <memory>

namespace dftracer::utils::call_tree {

/**
 * Simple node information structure for public API
 * Contains basic information about a node without complex internals
 */
struct CallTreeNodeInfo {
    std::uint64_t id;
    std::string name;
    std::string category;
    std::uint64_t start_time_us;
    std::uint64_t duration_us;
    int level;
    std::uint64_t parent_id;
    size_t num_children;
    
    CallTreeNodeInfo() 
        : id(0), name(""), category(""), start_time_us(0), 
          duration_us(0), level(0), parent_id(0), num_children(0) {}
};

/**
 * Aggregate statistics about the call tree
 */
struct CallTreeStats {
    size_t total_nodes;
    size_t num_levels;
    size_t num_leaf_nodes;
    size_t num_processes;
    std::vector<double> avg_time_per_level_us;  // Average time in microseconds per level
    std::vector<size_t> nodes_per_level;        // Number of nodes per level
    
    CallTreeStats()
        : total_nodes(0), num_levels(0), num_leaf_nodes(0), num_processes(0) {}
};

// Forward declarations
namespace internal {
    class CallTreeImpl;
}

/**
 * CallTree - Simple, clean API for working with call trees from DFTracer traces
 * 
 * Usage:
 *   CallTree tree;
 *   tree.load_from_directory("/path/to/traces");
 *   tree.generate();
 *   tree.print_depth_first();
 *   auto nodes = tree.get_nodes_depth_first();
 *   auto stats = tree.get_statistics();
 *   tree.save_to_file("output.calltree");
 */
class CallTree {
public:
    /**
     * Constructor
     */
    CallTree();
    
    /**
     * Destructor
     */
    ~CallTree();
    
    // Disable copy, enable move
    CallTree(const CallTree&) = delete;
    CallTree& operator=(const CallTree&) = delete;
    CallTree(CallTree&&) noexcept;
    CallTree& operator=(CallTree&&) noexcept;
    
    /**
     * Specify trace directory path
     * Automatically finds all .gz compressed trace files in the directory
     * @param trace_dir Path to directory containing trace files
     * @param pattern Optional file pattern (default: "*.pfw.gz")
     * @return true if directory exists and files found, false otherwise
     */
    bool load_from_directory(const std::string& trace_dir, 
                            const std::string& pattern = "*.pfw.gz");
    
    /**
     * Generate call tree from loaded traces
     * Reads all trace files and builds the in-memory call tree structure
     * @return true if successful, false otherwise
     */
    bool generate();
    
    /**
     * Print the call tree in depth-first order to stdout
     * @param max_depth Maximum depth to print (0 = unlimited)
     */
    void print_depth_first(int max_depth = 0) const;
    
    /**
     * Print the call tree to a file in depth-first order
     * @param filename Output file path
     * @param max_depth Maximum depth to print (0 = unlimited)
     * @return true if successful, false otherwise
     */
    bool print_depth_first_to_file(const std::string& filename, int max_depth = 0) const;
    
    /**
     * Get list of nodes in depth-first traversal order
     * Returns simple node info structures (no complex internals)
     * @return Vector of node information structures
     */
    std::vector<CallTreeNodeInfo> get_nodes_depth_first() const;
    
    /**
     * Get the path where serialized tree would be saved
     * @return Default output path based on input directory
     */
    std::string get_output_path() const;
    
    /**
     * Set custom output path for serialization
     * @param path Custom output file path
     */
    void set_output_path(const std::string& path);
    
    /**
     * Serialize and save call tree to file in binary format
     * @param filename Output file path (optional, uses get_output_path() if empty)
     * @return true if successful, false otherwise
     */
    bool save_to_file(const std::string& filename = "") const;
    
    /**
     * Serialize and save call tree to file in JSON format (Chrome Tracing/Perfetto compatible)
     * Follows DFTracer serialization format for compatibility with existing analysis tools
     * @param filename Output file path (optional, uses get_output_path() with .pfw extension if empty)
     * @return true if successful, false otherwise
     */
    bool save_to_json(const std::string& filename = "") const;
    
    /**
     * Load call tree from previously saved file
     * @param filename Input file path
     * @return true if successful, false otherwise
     */
    bool load_from_file(const std::string& filename);
    
    /**
     * Get aggregate statistics about the call tree
     * @return Statistics structure with aggregate information
     */
    CallTreeStats get_statistics() const;
    
    /**
     * Print aggregate statistics to stdout
     */
    void print_statistics() const;
    
    /**
     * Check if call tree has been generated
     * @return true if generate() completed successfully
     */
    bool is_generated() const;
    
    /**
     * Get number of trace files loaded
     * @return Number of trace files
     */
    size_t get_num_trace_files() const;
    
    /**
     * Clear all data and reset to initial state
     */
    void clear();

private:
    std::unique_ptr<internal::CallTreeImpl> impl_;
};

} // namespace dftracer::utils::call_tree

#endif // DFTRACER_UTILS_CALL_TREE_H
