#ifndef __DFTRACER_UTILS_READER_READER_H
#define __DFTRACER_UTILS_READER_READER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <dftracer/utils/indexer/indexer.h>
#include <stddef.h>
#include <stdint.h>

/**
 * Opaque handle for DFT reader
 */
typedef void *dft_reader_handle_t;

/**
 * Create a new DFT reader instance
 *
 * The index file will be automatically create if it does not exist
 *
 * @param gz_path Path to the gzipped trace file
 * @param idx_path Path to the index file
 * @param index_ckpt_size Checkpoint size for indexing in bytes
 * @return Opaque handle to the reader instance, or NULL on failure
 */
dft_reader_handle_t dft_reader_create(const char *gz_path, const char *idx_path,
                                      size_t index_ckpt_size);

/**
 * Create a new DFT reader instance with indexer
 * @param indexer Indexer instance
 */
dft_reader_handle_t dft_reader_create_with_indexer(
    dft_indexer_handle_t indexer);

/**
 * Destroy a DFT reader instance and free all associated resources
 * @param reader Opaque handle to the reader instance
 */
void dft_reader_destroy(dft_reader_handle_t reader);

/**
 * Get the maximum byte position available in the indexed gzip file
 * @param reader DFT reader handle
 * @param max_bytes Pointer to store the maximum byte position
 * @return 0 on success, -1 on error
 */
int dft_reader_get_max_bytes(dft_reader_handle_t reader, size_t *max_bytes);

/**
 * Get the maximum number of lines available in the indexed gzip file
 * @param reader DFT reader handle
 * @param num_lines Pointer to store the maximum number of lines
 * @return 0 on success, -1 on error
 */
int dft_reader_get_num_lines(dft_reader_handle_t reader, size_t *num_lines);

/**
 * Read raw bytes from the gzip file using the index database (streaming)
 * Returns data without caring about JSON line boundaries. Call repeatedly until
 * returns 0.
 * @param reader DFT reader handle
 * @param start_bytes Start position in bytes
 * @param end_bytes End position in bytes
 * @param buffer User-provided buffer for raw data
 * @param buffer_size Size of user-provided buffer
 * @return Number of bytes read, 0 indicates end of stream, -1 on error
 */
int dft_reader_read(dft_reader_handle_t reader, size_t start_bytes,
                    size_t end_bytes, char *buffer, size_t buffer_size);

/**
 * Read a range of bytes from the gzip file using the index database (streaming)
 * Returns complete lines only. Call repeatedly until returns 0.
 * @param reader DFT reader handle
 * @param start_bytes Start position in bytes
 * @param end_bytes End position in bytes
 * @param buffer User-provided buffer for complete lines
 * @param buffer_size Size of user-provided buffer
 * @return Number of bytes read, 0 indicates end of stream, -1 on error
 */
int dft_reader_read_line_bytes(dft_reader_handle_t reader, size_t start_bytes,
                               size_t end_bytes, char *buffer,
                               size_t buffer_size);

/**
 * Read complete lines from the gzip file using line numbers
 * @param reader DFT reader handle
 * @param start_line Start line number (1-based)
 * @param end_line End line number (1-based, inclusive)
 * @param buffer User-provided buffer for the lines
 * @param buffer_size Size of user-provided buffer
 * @param bytes_written Pointer to store number of bytes written to buffer
 * @return 0 on success, -1 on error
 */
int dft_reader_read_lines(dft_reader_handle_t reader, size_t start_line,
                          size_t end_line, char *buffer, size_t buffer_size,
                          size_t *bytes_written);

/**
 * Reset the reader to the initial state
 * @param reader DFT reader handle
 */
void dft_reader_reset(dft_reader_handle_t reader);

#ifdef __cplusplus
}  // extern "C"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <dftracer/utils/utils/json.h>

// Forward declaration for indexer
namespace dftracer {
namespace utils {
namespace indexer {
class Indexer;
struct CheckpointInfo;
}  // namespace indexer
}  // namespace utils
}  // namespace dftracer

namespace dftracer {
namespace utils {
namespace reader {

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
         size_t index_ckpt_size = indexer::Indexer::DEFAULT_CHECKPOINT_SIZE);

  /**
   * Create a new DFT reader instance
   * @param indexer Pointer to the indexer instance
   * @throws std::runtime_error if reader creation fails
   */
  Reader(indexer::Indexer *indexer);

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
  std::vector<dftracer::utils::json::AnyMap> read_json_lines(size_t start, size_t end);

  /**
   * Read bytes from gzip file and parse as JSON Lines (streaming)
   * Returns parsed JSON objects from complete lines only. Call repeatedly until returns empty vector.
   * @param start_bytes Start position in bytes
   * @param end_bytes End position in bytes
   * @param buffer User-provided buffer for complete lines
   * @param buffer_size Size of user-provided buffer
   * @return Vector of parsed JSON objects from the buffer
   * @throws std::runtime_error if operation fails
   */
  std::vector<dftracer::utils::json::AnyMap> read_json_lines_bytes(size_t start_bytes, size_t end_bytes, char *buffer, size_t buffer_size);

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

  class Error : public std::runtime_error {
   public:
    enum Type {
      DATABASE_ERROR,
      FILE_IO_ERROR,
      COMPRESSION_ERROR,
      INVALID_ARGUMENT,
      INITIALIZATION_ERROR,
      READ_ERROR,
      UNKNOWN_ERROR,
    };

    Error(Type type, const std::string &message)
        : std::runtime_error(format_message(type, message)), type_(type) {}

    Type get_type() const { return type_; }

   private:
    Type type_;

    static std::string format_message(Type type, const std::string &message);
  };

 private:
  class Impl;
  std::unique_ptr<Impl> pImpl_;
};

}  // namespace reader
}  // namespace utils
}  // namespace dftracer
#endif

#endif  // __DFTRACER_UTILS_READER_READER_H
