#ifndef __DFTRACER_UTILS_READER_READER_H
#define __DFTRACER_UTILS_READER_READER_H

#ifdef __cplusplus
extern "C"
{
#endif

#include <stddef.h>
#include <stdint.h>

    /**
     * Opaque handle for DFT reader
     */
    typedef void *dft_reader_handle_t;

    /**
     * Create a new DFT reader instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @return Opaque handle to the reader instance, or NULL on failure
     */
    dft_reader_handle_t dft_reader_create(const char *gz_path, const char *idx_path);

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
     * Read a range of bytes from a gzip file using the index database (streaming)
     * Returns complete JSON lines only. Call repeatedly until returns 0.
     * @param reader DFT reader handle
     * @param gz_path Path to the gzip file
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for complete JSON lines
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream, -1 on error
     */
    int dft_reader_read(dft_reader_handle_t reader,
                        const char *gz_path,
                        size_t start_bytes,
                        size_t end_bytes,
                        char *buffer,
                        size_t buffer_size);

    /**
     * Read raw bytes from a gzip file using the index database (streaming)
     * Returns data without caring about JSON line boundaries. Call repeatedly until returns 0.
     * @param reader DFT reader handle
     * @param gz_path Path to the gzip file
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for raw data
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream, -1 on error
     */
    int dft_reader_read_raw(dft_reader_handle_t reader,
                            const char *gz_path,
                            size_t start_bytes,
                            size_t end_bytes,
                            char *buffer,
                            size_t buffer_size);

    /**
     * Reset the reader to the initial state
     * @param reader DFT reader handle
     */
    void dft_reader_reset(dft_reader_handle_t reader);

#ifdef __cplusplus
} // extern "C"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

// Forward declaration for indexer
namespace dft { namespace indexer { class Indexer; struct CheckpointInfo; } }

namespace dft
{
namespace reader
{

/**
 * DFT reader
 *
 * Example usage:
 * ```cpp
 * try {
 *     dft::reader::Reader reader("trace.gz", "trace.gz.idx");
 *     size_t max_bytes = reader.get_max_bytes();
 *     auto data = reader.read_range_bytes(0, 1024);
 *     // Use data.first.get() for raw pointer, data.second for size
 * } catch (const std::runtime_error& e) {
 *     // Handle error
 * }
 * ```
 */
class Reader
{
  public:
    /**
     * Create a new DFT reader instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @throws std::runtime_error if reader creation fails
     */
    Reader(const std::string &gz_path, const std::string &idx_path);

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
     * Read a range of bytes from the gzip file using the index database (streaming)
     * Returns complete JSON lines only. Call repeatedly until returns 0.
     * @param gz_path Path to the gzip file (can be different from constructor)
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for complete JSON lines
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream
     * @throws std::runtime_error if operation fails
     */
    size_t read(const std::string &gz_path,
                size_t start_bytes,
                size_t end_bytes,
                char *buffer,
                size_t buffer_size);

    /**
     * Read a range of bytes from the gzip file using the stored gz_path (streaming)
     * Returns complete JSON lines only. Call repeatedly until returns 0.
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for complete JSON lines
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream
     * @throws std::runtime_error if operation fails
     */
    size_t read(size_t start_bytes, size_t end_bytes, char *buffer, size_t buffer_size);

    /**
     * Read raw bytes from the gzip file using the index database (streaming)
     * Returns data without caring about JSON line boundaries. Call repeatedly until returns 0.
     * @param gz_path Path to the gzip file (can be different from constructor)
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for raw data
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream
     * @throws std::runtime_error if operation fails
     */
    size_t read_raw(const std::string &gz_path,
                    size_t start_bytes,
                    size_t end_bytes,
                    char *buffer,
                    size_t buffer_size);

    /**
     * Read raw bytes from the gzip file using the stored gz_path (streaming)
     * Returns data without caring about JSON line boundaries. Call repeatedly until returns 0.
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param buffer User-provided buffer for raw data
     * @param buffer_size Size of user-provided buffer
     * @return Number of bytes read, 0 indicates end of stream
     * @throws std::runtime_error if operation fails
     */
    size_t read_raw(size_t start_bytes, size_t end_bytes, char *buffer, size_t buffer_size);

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

} // namespace reader
} // namespace dft
#endif

#endif // __DFTRACER_UTILS_READER_READER_H
