#ifndef DFTRACER_UTILS_CALL_GRAPH_MPI_H
#define DFTRACER_UTILS_CALL_GRAPH_MPI_H

#include <dftracer/utils/call_graph/call_graph.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/indexer/indexer_factory.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/reader/reader_factory.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/executors/executor.h>
#include <dftracer/utils/pipeline/executors/executor_factory.h>

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef DFTRACER_UTILS_MPI_ENABLED
#include <mpi.h>
#endif

namespace dftracer::utils::call_graph {

/**
 * Structure to hold PID index information from gzip indexer
 * Maps each PID to its starting line in the trace file
 */
struct PIDIndexInfo {
    std::uint32_t pid;
    std::uint64_t start_line;
    std::uint64_t end_line;
    std::uint64_t event_count;
    std::string source_file;
    
    PIDIndexInfo() : pid(0), start_line(0), end_line(0), event_count(0) {}
    PIDIndexInfo(std::uint32_t p, std::uint64_t sl, std::uint64_t el, 
                 std::uint64_t ec, const std::string& sf)
        : pid(p), start_line(sl), end_line(el), event_count(ec), source_file(sf) {}
};

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
    ProcessKey key;
    std::vector<SerializableCallNode> nodes;
    std::vector<std::uint64_t> root_calls;
    std::vector<std::uint64_t> call_sequence;
    
    // Serialization to bytes
    std::vector<char> serialize() const;
    static SerializableProcessGraph deserialize(const char* data, size_t& offset);
};

/**
 * Configuration for MPI call graph generation
 */
struct MPICallGraphConfig {
    std::string output_file;              // Output file for call graph
    std::string file_pattern = "*.pfw.gz"; // Pattern for trace files
    bool use_indexer = true;              // Use indexer for gzip files
    bool verbose = false;                 // Verbose logging
    bool summary_only = false;            // Only print summary
    std::size_t num_threads = 0;          // Threads for pipeline (0 = auto)
    std::uint64_t checkpoint_size = 0;    // Indexer checkpoint size (0 = default)
};

/**
 * Result from MPI call graph generation
 */
struct MPICallGraphResult {
    bool success = false;
    std::size_t total_pids = 0;
    std::size_t local_pids = 0;
    std::size_t total_events = 0;
    std::size_t local_events = 0;
    double elapsed_time_s = 0.0;
    std::string error_message;
};

/**
 * File header for persisted call graph
 */
struct CallGraphFileHeader {
    static constexpr char MAGIC[8] = {'D', 'F', 'T', 'C', 'G', 'R', 'P', 'H'};
    static constexpr std::uint32_t VERSION = 1;
    
    char magic[8];
    std::uint32_t version;
    std::uint32_t num_process_graphs;
    std::uint64_t data_offset;
    std::uint64_t total_events;
    
    CallGraphFileHeader() 
        : version(VERSION), num_process_graphs(0), data_offset(0), total_events(0) {
        std::memcpy(magic, MAGIC, sizeof(MAGIC));
    }
    
    bool is_valid() const {
        return std::memcmp(magic, MAGIC, sizeof(MAGIC)) == 0 && version == VERSION;
    }
};

/**
 * MPICallGraphBuilder - Main class for MPI-parallel call graph generation
 * 
 * Usage:
 *   1. Create builder with config
 *   2. Call discover_pids() to find all PIDs in trace files
 *   3. Call build() to generate call graphs in parallel
 *   4. Call gather() to collect all graphs to all ranks (all-to-all)
 *   5. Call save() to write to file
 * 
 * Follows initialization pattern:
 * 1. Constructor: Initialize internal variables (no allocation)
 * 2. initialize(): Set up MPI, index files, discover PIDs
 * 3. build(): Generate call graphs using pipeline
 * 4. gather(): All-to-all MPI communication
 * 5. save()/load(): File I/O
 * 6. cleanup(): Deallocate memory
 */
class MPICallGraphBuilder {
public:
    /**
     * Constructor - initializes with configuration
     */
    explicit MPICallGraphBuilder(const MPICallGraphConfig& config);
    
    /**
     * Destructor
     */
    ~MPICallGraphBuilder();
    
    // Disable copy
    MPICallGraphBuilder(const MPICallGraphBuilder&) = delete;
    MPICallGraphBuilder& operator=(const MPICallGraphBuilder&) = delete;
    
    // Enable move
    MPICallGraphBuilder(MPICallGraphBuilder&&) noexcept;
    MPICallGraphBuilder& operator=(MPICallGraphBuilder&&) noexcept;
    
    /**
     * Initialize MPI and internal structures
     * Must be called after MPI_Init
     */
    void initialize();
    
    /**
     * Cleanup and release resources
     */
    void cleanup();
    
    /**
     * Add trace files to process
     * @param files Vector of file paths
     */
    void add_trace_files(const std::vector<std::string>& files);
    
    /**
     * Add trace files from directory
     * @param directory Path to directory
     * @param pattern File pattern (e.g., "*.pfw.gz")
     */
    void add_trace_directory(const std::string& directory, 
                            const std::string& pattern = "*.pfw.gz");
    
    /**
     * Phase 1: Discover all PIDs and build index
     * Each MPI rank discovers PIDs from the trace files
     * Results are gathered and PIDs are distributed
     * @return Map of PID to index info
     */
    std::map<std::uint32_t, PIDIndexInfo> discover_pids();
    
    /**
     * Phase 2: Build call graphs for assigned PIDs
     * Uses pipeline for parallel processing within rank
     * @return Result containing success status and statistics
     */
    MPICallGraphResult build();
    
    /**
     * Phase 3: All-to-all communication to share graphs
     * After this, all ranks have identical copies of all call graphs
     * @return true if successful
     */
    bool gather();
    
    /**
     * Save the global call graph to file
     * @param filename Output file path
     * @return true if successful
     */
    bool save(const std::string& filename) const;
    
    /**
     * Load call graph from file (static method)
     * @param filename Input file path
     * @return Loaded call graph or nullptr on error
     */
    static std::unique_ptr<CallGraph> load(const std::string& filename);
    
    /**
     * Get the generated call graph
     * @return Reference to the call graph
     */
    CallGraph& get_call_graph() { return *call_graph_; }
    const CallGraph& get_call_graph() const { return *call_graph_; }
    
    /**
     * Get MPI rank
     */
    int get_rank() const { return rank_; }
    
    /**
     * Get MPI world size
     */
    int get_world_size() const { return world_size_; }
    
    /**
     * Get PIDs assigned to this rank
     */
    const std::set<std::uint32_t>& get_assigned_pids() const { return assigned_pids_; }
    
    /**
     * Print summary statistics
     */
    void print_summary() const;

private:
    MPICallGraphConfig config_;
    std::unique_ptr<CallGraph> call_graph_;
    
    // MPI state
    int rank_ = 0;
    int world_size_ = 1;
    bool mpi_initialized_ = false;
    
    // File tracking
    std::vector<std::string> trace_files_;
    std::map<std::string, std::unique_ptr<Indexer>> indexers_;
    
    // PID management
    std::map<std::uint32_t, PIDIndexInfo> pid_index_map_;
    std::set<std::uint32_t> assigned_pids_;
    std::vector<std::uint32_t> all_pids_;
    
    // State flags
    bool initialized_ = false;
    bool pids_discovered_ = false;
    bool graphs_built_ = false;
    bool graphs_gathered_ = false;
    
    // Internal methods
    void create_indexer(const std::string& trace_file);
    std::set<std::uint32_t> scan_file_for_pids(const std::string& trace_file);
    bool read_traces_for_pids(const std::vector<std::string>& files,
                             const std::set<std::uint32_t>& pids);
    SerializableProcessGraph convert_to_serializable(const ProcessCallGraph& graph) const;
    void merge_from_serializable(const SerializableProcessGraph& serializable);
    
    // MPI communication helpers
    void broadcast_string(std::string& str, int root = 0);
    void broadcast_pids(std::vector<std::uint32_t>& pids, int root = 0);
    void distribute_pids();
    bool alltoall_graphs();
};

/**
 * Filtered trace reader that only processes events for specific PIDs
 * Uses the indexer to efficiently skip to relevant sections
 */
class MPIFilteredTraceReader {
public:
    explicit MPIFilteredTraceReader(const std::set<std::uint32_t>& allowed_pids);
    
    /**
     * Read trace file and populate call graph
     * Only processes events for allowed PIDs
     */
    bool read(const std::string& trace_file, CallGraph& graph);
    
    /**
     * Read with indexer for efficient access
     */
    bool read_with_indexer(const std::string& trace_file,
                          const std::string& index_file,
                          CallGraph& graph);
    
    /**
     * Read multiple files
     */
    bool read_multiple(const std::vector<std::string>& trace_files, 
                      CallGraph& graph);
    
    /**
     * Get count of processed events
     */
    std::size_t get_processed_count() const { return processed_count_; }
    
    /**
     * Get count of filtered (skipped) events
     */
    std::size_t get_filtered_count() const { return filtered_count_; }

private:
    std::set<std::uint32_t> allowed_pids_;
    std::size_t processed_count_ = 0;
    std::size_t filtered_count_ = 0;
};

/**
 * Pipeline-based call graph builder task
 * Input: vector of trace files
 * Output: ProcessCallGraph for assigned PIDs
 */
struct CallGraphBuildTask {
    std::set<std::uint32_t> pids;
    std::vector<std::string> trace_files;
    
    ProcessCallGraph execute(CallGraph& graph);
};

// Utility functions for serialization
namespace serialization {

// Write primitives
void write_uint32(std::vector<char>& buffer, std::uint32_t value);
void write_uint64(std::vector<char>& buffer, std::uint64_t value);
void write_int(std::vector<char>& buffer, int value);
void write_string(std::vector<char>& buffer, const std::string& str);

// Read primitives  
std::uint32_t read_uint32(const char* data, size_t& offset);
std::uint64_t read_uint64(const char* data, size_t& offset);
int read_int(const char* data, size_t& offset);
std::string read_string(const char* data, size_t& offset);

} // namespace serialization

} // namespace dftracer::utils::call_graph

#endif // DFTRACER_UTILS_CALL_GRAPH_MPI_H
