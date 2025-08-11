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
     * Read a range of bytes from a gzip file using the index database
     * @param reader DFT reader handle
     * @param gz_path Path to the gzip file
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param output Buffer to store the extracted data (caller must free)
     * @param output_size Pointer to store the size of extracted data
     * @return 0 on success, -1 on error
     */
    int dft_reader_read_range_bytes(dft_reader_handle_t reader,
                                    const char *gz_path,
                                    size_t start_bytes,
                                    size_t end_bytes,
                                    char **output,
                                    size_t *output_size);

    /**
     * Read a range of megabytes from a gzip file using the index database
     * @param reader DFT reader handle
     * @param gz_path Path to the gzip file
     * @param start_mb Start position in megabytes
     * @param end_mb End position in megabytes
     * @param output Buffer to store the extracted data (caller must free)
     * @param output_size Pointer to store the size of extracted data
     * @return 0 on success, -1 on error
     */
    int dft_reader_read_range_megabytes(dft_reader_handle_t reader,
                                        const char *gz_path,
                                        double start_mb,
                                        double end_mb,
                                        char **output,
                                        size_t *output_size);

#ifdef __cplusplus
} // extern "C"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

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
     * Smart pointer type for managing allocated memory
     */
    using Buffer = std::unique_ptr<char, void (*)(void *)>;

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
     * Read a range of bytes from the gzip file using the index database
     * @param gz_path Path to the gzip file (can be different from constructor)
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @return pair of (smart pointer to data, size of data)
     * @throws std::runtime_error if operation fails
     */
    std::pair<Buffer, size_t> read_range_bytes(const std::string &gz_path, size_t start_bytes, size_t end_bytes) const;

    /**
     * Read a range of bytes from the gzip file using the stored gz_path
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @return pair of (smart pointer to data, size of data)
     * @throws std::runtime_error if operation fails
     */
    std::pair<Buffer, size_t> read_range_bytes(size_t start_bytes, size_t end_bytes) const;

    /**
     * Read a range of megabytes from the gzip file using the index database
     * @param gz_path Path to the gzip file (can be different from constructor)
     * @param start_mb Start position in megabytes
     * @param end_mb End position in megabytes
     * @return pair of (smart pointer to data, size of data)
     * @throws std::runtime_error if operation fails
     */
    std::pair<Buffer, size_t> read_range_megabytes(const std::string &gz_path, double start_mb, double end_mb) const;

    /**
     * Read a range of megabytes from the gzip file using the stored gz_path
     * @param start_mb Start position in megabytes
     * @param end_mb End position in megabytes
     * @return pair of (smart pointer to data, size of data)
     * @throws std::runtime_error if operation fails
     */
    std::pair<Buffer, size_t> read_range_megabytes(double start_mb, double end_mb) const;

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
