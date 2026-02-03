#ifndef DFTRACER_UTILS_CALL_TREE_MPI_PID_INDEX_INFO_H
#define DFTRACER_UTILS_CALL_TREE_MPI_PID_INDEX_INFO_H

/**
 * @file pid_index_info.h
 * @brief Structure to hold PID index information from gzip indexer
 */

#include <cstdint>
#include <string>

namespace dftracer::utils::call_tree {

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
        : pid(p),
          start_line(sl),
          end_line(el),
          event_count(ec),
          source_file(sf) {}
};

}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_PID_INDEX_INFO_H
