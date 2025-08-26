#ifndef DFTRACER_UTILS_READER_READER_H
#define DFTRACER_UTILS_READER_READER_H

#include <dftracer/utils/indexer/indexer.h>

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

/**
 * Opaque handle for DFT reader
 */
typedef void *dft_reader_handle_t;
dft_reader_handle_t dft_reader_create(const char *gz_path, const char *idx_path,
                                      size_t index_ckpt_size);
dft_reader_handle_t dft_reader_create_with_indexer(
    dft_indexer_handle_t indexer);
void dft_reader_destroy(dft_reader_handle_t reader);
int dft_reader_get_max_bytes(dft_reader_handle_t reader, size_t *max_bytes);
int dft_reader_get_num_lines(dft_reader_handle_t reader, size_t *num_lines);
int dft_reader_read(dft_reader_handle_t reader, size_t start_bytes,
                    size_t end_bytes, char *buffer, size_t buffer_size);
int dft_reader_read_line_bytes(dft_reader_handle_t reader, size_t start_bytes,
                               size_t end_bytes, char *buffer,
                               size_t buffer_size);
int dft_reader_read_lines(dft_reader_handle_t reader, size_t start_line,
                          size_t end_line, char *buffer, size_t buffer_size,
                          size_t *bytes_written);
void dft_reader_reset(dft_reader_handle_t reader);
#ifdef __cplusplus
}  // extern "C"

#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/utils/json.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Forward declaration for indexer
namespace dftracer::utils {

class Indexer;
struct IndexerCheckpoint;

/**
 * DFT reader
 *
 * Example usage:
 * ```cpp
 * try {
 *     dftracer::utils::reader::Reader reader("trace.gz", "trace.gz.idx");
 *     size_t max_bytes = reader.get_max_bytes();
 *     auto data = reader.read_range_bytes(0, 1024);
 *     // Use data.first.get() for raw pointer, data.second for size
 * } catch (const std::runtime_error& e) {
 *     // Handle error
 * }
 * ```
 */

class Reader {
   public:
    /**
     * Create a new DFT reader instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @param index_ckpt_size Checkpoint size for indexing in bytes
     * @throws std::runtime_error if reader creation fails
     */
    Reader(const std::string &gz_path, const std::string &idx_path,
           size_t index_ckpt_size = Indexer::DEFAULT_CHECKPOINT_SIZE);

    /**
     * Create a new DFT reader instance
     * @param indexer Pointer to the indexer instance
     * @throws std::runtime_error if reader creation fails
     */
    Reader(Indexer *indexer);

    /**
     * Destructor - automatically destroys the reader
     */
    ~Reader();

    // Disable copy constructor and copy assignment
    Reader(const Reader &) = delete;
    Reader &operator=(const Reader &) = delete;

    /**
     * Move constructor
     */
    Reader(Reader &&other) noexcept;

    /**
     * Move assignment operator
     */
    Reader &operator=(Reader &&other) noexcept;

    /**
     * Get the maximum byte position available in the indexed gzip file
     * @return maximum byte position
     * @throws std::runtime_error if operation fails
     */
    size_t get_max_bytes() const;

    /**
     * Get the maximum number of lines available in the indexed gzip file
     * @return maximum number of lines
     * @throws std::runtime_error if operation fails
     */
    size_t get_num_lines() const;

    /**
     * Read raw bytes from the gzip file using the stored gz_path (streaming)
     * Returns data without caring about line boundaries. Call repeatedly until
     * returns 0.
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for raw data
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream
     * @throws std::runtime_error if operation fails
     */
    size_t read(size_t start_bytes, size_t end_bytes, char *buffer,
                size_t buffer_size);

    /**
     * Read a range of bytes from the gzip file using the stored gz_path
     * (streaming) Returns complete lines only. Call repeatedly until returns 0.
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for complete lines
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream
     * @throws std::runtime_error if operation fails
     */
    size_t read_line_bytes(size_t start_bytes, size_t end_bytes, char *buffer,
                           size_t buffer_size);

    /**
     * Read complete lines from the gzip file and return as a string
     * @param start Start line number (0-based)
     * @param end End line number (exclusive, 0-based)
     * @return String containing all lines in the range
     * @throws std::runtime_error if operation fails
     */
    std::string read_lines(size_t start, size_t end);

    /**
     * Read complete lines from gzip file and parse as JSON Lines
     * @param start Start line number (0-based)
     * @param end End line number (exclusive, 0-based)
     * @return Vector of parsed JSON objects
     * @throws std::runtime_error if operation fails
     */
    dftracer::utils::json::JsonDocuments read_json_lines(size_t start,
                                                         size_t end);

    /**
     * Read complete lines from gzip file and parse as JSON Lines (owned)
     * @param start Start line number (0-based)
     * @param end End line number (exclusive, 0-based)
     * @return Vector of parsed JSON objects
     * @throws std::runtime_error if operation fails
     */
    dftracer::utils::json::OwnedJsonDocuments read_json_lines_owned(
        size_t start, size_t end);

    /**
     * Read bytes from gzip file and parse as JSON Lines
     * Returns parsed JSON objects from complete lines only. Call repeatedly
     * until returns empty vector.
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @return Vector of parsed JSON objects from the buffer
     * @throws std::runtime_error if operation fails
     */
    dftracer::utils::json::JsonDocuments read_json_lines_bytes(
        size_t start_bytes, size_t end_bytes);

    /**
     * Read bytes from gzip file and parse as JSON Lines (owned)
     * Returns parsed JSON objects from complete lines only. Call repeatedly
     * until returns empty vector.
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @return Vector of parsed JSON objects from the buffer
     * @throws std::runtime_error if operation fails
     */
    dftracer::utils::json::OwnedJsonDocuments read_json_lines_bytes_owned(
        size_t start_bytes, size_t end_bytes);

    /**
     * Set default buffer size in bytes
     *
     * @param size New buffer size
     */
    void set_buffer_size(size_t size);

    /**
     * Reset the reader to the initial state
     */
    void reset();

    /**
     * Check if the reader is valid
     * @return true if reader is valid, false otherwise
     */
    bool is_valid() const;

    /**
     * Get the gzip file path
     * @return gzip file path
     */
    const std::string &get_gz_path() const;

    /**
     * Get the index file path
     * @return index file path
     */
    const std::string &get_idx_path() const;

   private:
    class Impl;
    std::unique_ptr<Impl> pImpl_;
};

}  // namespace dftracer::utils
#endif

#endif  // DFTRACER_UTILS_READER_READER_H
