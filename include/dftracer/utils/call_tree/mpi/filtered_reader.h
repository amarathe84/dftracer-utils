#ifndef DFTRACER_UTILS_CALL_TREE_MPI_FILTERED_READER_H
#define DFTRACER_UTILS_CALL_TREE_MPI_FILTERED_READER_H

/**
 * @file filtered_reader.h
 * @brief Filtered trace reader that only processes events for specific PIDs
 */

#include <dftracer/utils/call_tree/internal/call_tree.h>

#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace dftracer::utils::call_tree {

/**
 * Filtered trace reader that only processes events for specific PIDs
 * Uses the indexer to efficiently skip to relevant sections
 */
class MPIFilteredTraceReader {
   public:
    explicit MPIFilteredTraceReader(
        const std::set<std::uint32_t>& allowed_pids);

    /**
     * Read trace file and populate call graph
     * Only processes events for allowed PIDs
     */
    bool read(const std::string& trace_file, internal::CallTree& graph);

    /**
     * Read with indexer for efficient access
     */
    bool read_with_indexer(const std::string& trace_file,
                           const std::string& index_file,
                           internal::CallTree& graph);

    /**
     * Read multiple files
     */
    bool read_multiple(const std::vector<std::string>& trace_files,
                       internal::CallTree& graph);

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

}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_FILTERED_READER_H
