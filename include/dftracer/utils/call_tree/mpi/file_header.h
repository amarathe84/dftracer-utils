#ifndef DFTRACER_UTILS_CALL_TREE_MPI_FILE_HEADER_H
#define DFTRACER_UTILS_CALL_TREE_MPI_FILE_HEADER_H

/**
 * @file file_header.h
 * @brief File header structure for persisted call graph files
 */

#include <cstdint>
#include <cstring>

namespace dftracer::utils::call_tree {

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
        : version(VERSION),
          num_process_graphs(0),
          data_offset(0),
          total_events(0) {
        std::memcpy(magic, MAGIC, sizeof(MAGIC));
    }

    bool is_valid() const {
        return std::memcmp(magic, MAGIC, sizeof(MAGIC)) == 0 &&
               version == VERSION;
    }
};

}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_FILE_HEADER_H
