#ifndef __DFTRACER_UTILS_READER_READER_H
#define __DFTRACER_UTILS_READER_READER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct dft_reader dft_reader_t;

    /**
     * Create a new DFT reader instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @return Pointer to the reader instance, or NULL on failure
     */
    dft_reader_t* dft_reader_create(const char* gz_path, const char* idx_path);

    /**
     * Destroy a DFT reader instance and free all associated resources
     * @param reader Pointer to the reader instance
     */
    void dft_reader_destroy(dft_reader_t *reader);

    /**
     * Get the maximum byte position available in the indexed gzip file
     * @param reader DFT reader instance
     * @param max_bytes Pointer to store the maximum byte position
     * @return 0 on success, -1 on error
     */
    int dft_reader_get_max_bytes(dft_reader_t *reader, size_t *max_bytes);

    /**
     * Read a range of bytes from a gzip file using the index database
     * @param reader DFT reader instance
     * @param gz_path Path to the gzip file
     * @param start_bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param output Buffer to store the extracted data (caller must free)
     * @param output_size Pointer to store the size of extracted data
     * @return 0 on success, -1 on error
     */
    int dft_reader_read_range_bytes(
        dft_reader_t *reader, const char *gz_path, size_t start_bytes, size_t end_bytes, char **output, size_t *output_size);

    /**
     * Read a range of megabytes from a gzip file using the index database
     * @param reader DFT reader instance
     * @param gz_path Path to the gzip file
     * @param start_mb Start position in megabytes
     * @param end_mb End position in megabytes
     * @param output Buffer to store the extracted data (caller must free)
     * @param output_size Pointer to store the size of extracted data
     * @return 0 on success, -1 on error
     */
    static inline int dft_reader_read_range_megabytes(
        dft_reader_t *reader, const char *gz_path, double start_mb, double end_mb, char **output, size_t *output_size)
    {
        if (!reader || !gz_path || !output || !output_size)
        {
            return -1;
        }

        // Convert MB to bytes
        size_t start_bytes = static_cast<size_t>(start_mb * 1024 * 1024);
        size_t end_bytes = static_cast<size_t>(end_mb * 1024 * 1024);

        return dft_reader_read_range_bytes(reader, gz_path, start_bytes, end_bytes, output, output_size);
    }

#ifdef __cplusplus
} // extern "C"

#include <string>
#include <memory>
#include <stdexcept>

// C++ namespace wrapper for convenience
namespace dft
{
namespace reader
{
    inline dft_reader_t* create(const std::string& gz_path, const std::string& idx_path)
    {
        return dft_reader_create(gz_path.c_str(), idx_path.c_str());
    }

    inline void destroy(dft_reader_t* reader)
    {
        dft_reader_destroy(reader);
    }

    inline int get_max_bytes(dft_reader_t* reader, size_t* max_bytes)
    {
        return dft_reader_get_max_bytes(reader, max_bytes);
    }

    inline int read_range_bytes(dft_reader_t* reader, const std::string& gz_path, 
                              size_t start_bytes, size_t end_bytes, 
                              char** output, size_t* output_size)
    {
        return dft_reader_read_range_bytes(reader, gz_path.c_str(), start_bytes, end_bytes, output, output_size);
    }

    inline int read_range_megabytes(dft_reader_t* reader, const std::string& gz_path,
                                  double start_mb, double end_mb,
                                  char** output, size_t* output_size)
    {
        return dft_reader_read_range_megabytes(reader, gz_path.c_str(), start_mb, end_mb, output, output_size);
    }

    /**
     * RAII wrapper class for DFT reader
     * 
     * This class provides automatic resource management for the DFT reader using RAII.
     * The reader is automatically destroyed when the object goes out of scope.
     * Memory allocated for reading operations is automatically managed using smart pointers.
     * 
     * Example usage:
     * ```cpp
     * try {
     *     dft::reader::Reader reader("trace.gz", "trace.gz.idx");
     *     size_t max_bytes = reader.get_max_bytes();
     *     auto data = reader.read_range_bytes("trace.gz", 0, 1024);
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
        using Buffer = std::unique_ptr<char, void(*)(void*)>;

        /**
         * Create a new DFT reader instance
         * @param gz_path Path to the gzipped trace file
         * @param idx_path Path to the index file
         * @throws std::runtime_error if reader creation fails
         */
        Reader(const std::string& gz_path, const std::string& idx_path);

        /**
         * Destructor - automatically destroys the reader
         */
        ~Reader();

        // Disable copy constructor and copy assignment
        Reader(const Reader&) = delete;
        Reader& operator=(const Reader&) = delete;

        /**
         * Move constructor
         */
        Reader(Reader&& other) noexcept;

        /**
         * Move assignment operator
         */
        Reader& operator=(Reader&& other) noexcept;

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
        std::pair<Buffer, size_t> read_range_bytes(const std::string& gz_path, size_t start_bytes, size_t end_bytes) const;

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
        std::pair<Buffer, size_t> read_range_megabytes(const std::string& gz_path, double start_mb, double end_mb) const;

        /**
         * Read a range of megabytes from the gzip file using the stored gz_path
         * @param start_mb Start position in megabytes
         * @param end_mb End position in megabytes
         * @return pair of (smart pointer to data, size of data)
         * @throws std::runtime_error if operation fails
         */
        std::pair<Buffer, size_t> read_range_megabytes(double start_mb, double end_mb) const;

        /**
         * Get the raw C reader pointer (for interoperability)
         * @return Raw pointer to the C reader
         */
        dft_reader_t* get() const;

        /**
         * Check if the reader is valid
         * @return true if reader is valid, false otherwise
         */
        bool is_valid() const;

        /**
         * Get the gzip file path
         * @return gzip file path
         */
        const std::string& get_gz_path() const;

    private:
        dft_reader_t* reader_;
        std::string gz_path_;
    };

} // namespace reader
} // namespace dft
#endif

#endif // __DFTRACER_UTILS_READER_READER_H
