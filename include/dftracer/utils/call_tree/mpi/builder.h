#ifndef DFTRACER_UTILS_CALL_TREE_MPI_BUILDER_H
#define DFTRACER_UTILS_CALL_TREE_MPI_BUILDER_H

/**
 * @file builder.h
 * @brief MPICallTreeBuilder - Main class for MPI-parallel call graph generation
 */

#include <dftracer/utils/call_tree/internal/call_tree.h>
#include <dftracer/utils/call_tree/internal/process_call_tree.h>
#include <dftracer/utils/call_tree/mpi/config.h>
#include <dftracer/utils/call_tree/mpi/pid_index_info.h>
#include <dftracer/utils/call_tree/mpi/serializable.h>
#include <dftracer/utils/core/mpi/mpi_utils.h>
#include <dftracer/utils/utilities/indexer/internal/indexer.h>

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace dftracer::utils::call_tree {

// Type alias for indexer shared pointer
using IndexerPtr =
    std::shared_ptr<dftracer::utils::utilities::indexer::internal::Indexer>;

/**
 * MPICallTreeBuilder - Main class for MPI-parallel call graph generation
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
class MPICallTreeBuilder {
   public:
    /**
     * Constructor - initializes with configuration
     */
    explicit MPICallTreeBuilder(const MPICallTreeConfig& config);

    /**
     * Destructor
     */
    ~MPICallTreeBuilder();

    // Disable copy
    MPICallTreeBuilder(const MPICallTreeBuilder&) = delete;
    MPICallTreeBuilder& operator=(const MPICallTreeBuilder&) = delete;

    // Enable move
    MPICallTreeBuilder(MPICallTreeBuilder&&) noexcept;
    MPICallTreeBuilder& operator=(MPICallTreeBuilder&&) noexcept;

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
     * Load call tree from file (static method)
     * @param filename Input file path
     * @return Loaded call tree or nullptr on error
     */
    static std::unique_ptr<internal::CallTree> load(
        const std::string& filename);

    /**
     * Get the generated call tree
     * @return Reference to the call tree
     */
    internal::CallTree& get_call_tree() { return *call_tree_; }
    const internal::CallTree& get_call_tree() const { return *call_tree_; }

    /**
     * Get MPI rank (delegates to MPIUtils singleton)
     */
    int get_rank() const { return mpi::MPIUtils::instance().get_rank(); }

    /**
     * Get MPI world size (delegates to MPIUtils singleton)
     */
    int get_world_size() const {
        return mpi::MPIUtils::instance().get_world_size();
    }

    /**
     * Get PIDs assigned to this rank
     */
    const std::set<std::uint32_t>& get_assigned_pids() const {
        return assigned_pids_;
    }

    /**
     * Print summary statistics
     */
    void print_summary() const;

   private:
    MPICallTreeConfig config_;
    std::unique_ptr<internal::CallTree> call_tree_;

    // File tracking
    std::vector<std::string> trace_files_;
    std::map<std::string, IndexerPtr> indexers_;

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
    SerializableProcessGraph convert_to_serializable(
        const internal::ProcessCallTree& graph) const;
    void merge_from_serializable(const SerializableProcessGraph& serializable);

    // Internal MPI helpers
    void distribute_pids();
    bool alltoall_graphs();
};

}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_BUILDER_H
