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
} // namespace reader
} // namespace dft
#endif

#endif // __DFTRACER_UTILS_READER_READER_H
