#ifndef DFTRACER_UTILS_CALL_TREE_INTERNAL_TRACE_READER_H
#define DFTRACER_UTILS_CALL_TREE_INTERNAL_TRACE_READER_H

#include <functional>
#include <string>
#include <vector>

namespace dftracer::utils::call_tree {
namespace internal {

// Forward declaration
class CallTree;

/**
 * Callback function type for processing traces
 * Returns true to continue processing, false to stop
 */
using TraceCallback = std::function<bool(const std::string& json_line)>;

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
    bool read_multiple(const std::vector<std::string>& trace_files,
                       CallTree& graph);

    /**
     * Read all trace files matching pattern from a directory
     * @param directory Path to directory containing trace files
     * @param pattern Glob pattern for trace files (e.g., "*.pfw")
     * @param graph CallTree to populate
     * @return true if successful, false otherwise
     */
    bool read_directory(const std::string& directory,
                        const std::string& pattern, CallTree& graph);

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

}  // namespace internal
}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_INTERNAL_TRACE_READER_H
