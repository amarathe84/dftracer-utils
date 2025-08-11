#ifndef __DFTRACER_UTILS_READER_READER_H
#define __DFTRACER_UTILS_READER_READER_H

#include <sqlite3.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /**
     * Get the maximum byte position available in the indexed gzip file
     * @param db SQLite database containing the gzip index
     * @param max_bytes Pointer to store the maximum byte position
     * @return 0 on success, -1 on error
     */
    int dft_reader_get_max_bytes(sqlite3 *db, size_t *max_bytes);

    /**
     * Read a range of bytes from a gzip file using the index database
     * @param db SQLite database containing the gzip index
     * @param gz_path Path to the gzip file
     * @param bytes Start position in bytes
     * @param end_bytes End position in bytes
     * @param output Buffer to store the extracted data (caller must free)
     * @param output_size Pointer to store the size of extracted data
     * @return 0 on success, -1 on error
     */
    int dft_reader_read_range_bytes(
        sqlite3 *db, const char *gz_path, size_t start_bytes, size_t end_bytes, char **output, size_t *output_size);

    /**
     * Read a range of megabytes from a gzip file using the index database
     * @param db SQLite database containing the gzip index
     * @param gz_path Path to the gzip file
     * @param start_mb Start position in megabytes
     * @param end_mb End position in megabytes
     * @param output Buffer to store the extracted data (caller must free)
     * @param output_size Pointer to store the size of extracted data
     * @return 0 on success, -1 on error
     */
    static inline int dft_reader_read_range_megabytes(
        sqlite3 *db, const char *gz_path, double start_mb, double end_mb, char **output, size_t *output_size)
    {
        if (!db || !gz_path || !output || !output_size)
        {
            return -1;
        }

        // Convert MB to bytes
        size_t start_bytes = static_cast<size_t>(start_mb * 1024 * 1024);
        size_t end_bytes = static_cast<size_t>(end_mb * 1024 * 1024);

        return dft_reader_read_range_bytes(db, gz_path, start_bytes, end_bytes, output, output_size);
    }

#ifdef __cplusplus
} // extern "C"

namespace dft
{
namespace reader
{
int get_max_bytes(sqlite3 *db, size_t *max_bytes);

int read_range_bytes(
    sqlite3 *db, const char *gz_path, size_t start_bytes, size_t end_bytes, char **output, size_t *output_size);

static inline int read_range_megabytes(
    sqlite3 *db, const char *gz_path, double start_mb, double end_mb, char **output, size_t *output_size)
{
    if (!db || !gz_path || !output || !output_size)
    {
        return -1;
    }

    // Convert MB to bytes
    size_t start_bytes = static_cast<size_t>(start_mb * 1024 * 1024);
    size_t end_bytes = static_cast<size_t>(end_mb * 1024 * 1024);

    return read_range_bytes(db, gz_path, start_bytes, end_bytes, output, output_size);
}
} // namespace reader
} // namespace dft
#endif

#endif // __DFTRACER_UTILS_READER_READER_H
