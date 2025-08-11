#ifndef __DFTRACER_UTILS_READER_INDEXER_H
#define __DFTRACER_UTILS_READER_INDEXER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct dft_indexer dft_indexer_t;

    /**
     * Create a new DFT indexer instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @param chunk_size_mb Chunk size for indexing in megabytes
     * @param force_rebuild Force rebuild even if index exists and chunk size matches
     * @return Pointer to the indexer instance, or NULL on failure
     */
    dft_indexer_t* dft_indexer_create(const char* gz_path, const char* idx_path, double chunk_size_mb, bool force_rebuild);

    /**
     * Build or rebuild the index if necessary
     * @param indexer DFT indexer instance
     * @return 0 on success, -1 on error
     */
    int dft_indexer_build(dft_indexer_t* indexer);

    /**
     * Check if a rebuild is needed (index doesn't exist, is invalid, or chunk size differs)
     * @param indexer DFT indexer instance
     * @return 1 if rebuild is needed, 0 if not needed, -1 on error
     */
    int dft_indexer_need_rebuild(dft_indexer_t* indexer);

    /**
     * Destroy a DFT indexer instance and free all associated resources
     * @param indexer Pointer to the indexer instance
     */
    void dft_indexer_destroy(dft_indexer_t* indexer);

#ifdef __cplusplus
} // extern "C"

#include <string>
#include <stdexcept>

namespace dft
{
namespace indexer
{
/**
 * Create a new DFT indexer instance (C++ wrapper)
 * @param gz_path Path to the gzipped trace file
 * @param idx_path Path to the index file
 * @param chunk_size_mb Chunk size for indexing in megabytes
 * @param force_rebuild Force rebuild even if index exists and chunk size matches
 * @return Pointer to the indexer instance, or nullptr on failure
 */
inline dft_indexer_t* create(const std::string& gz_path, const std::string& idx_path, double chunk_size_mb, bool force_rebuild = false)
{
    return dft_indexer_create(gz_path.c_str(), idx_path.c_str(), chunk_size_mb, force_rebuild);
}

/**
 * Build or rebuild the index if necessary (C++ wrapper)
 * @param indexer DFT indexer instance
 * @return 0 on success, -1 on error
 */
inline int build(dft_indexer_t* indexer)
{
    return dft_indexer_build(indexer);
}

/**
 * Check if a rebuild is needed (C++ wrapper)
 * @param indexer DFT indexer instance
 * @return true if rebuild is needed, false if not needed
 * @throws std::runtime_error on error
 */
inline bool need_rebuild(dft_indexer_t* indexer)
{
    int result = dft_indexer_need_rebuild(indexer);
    if (result == -1)
    {
        throw std::runtime_error("Failed to check if rebuild is needed");
    }
    return result == 1;
}

/**
 * Destroy a DFT indexer instance and free all associated resources (C++ wrapper)
 * @param indexer Pointer to the indexer instance
 */
inline void destroy(dft_indexer_t* indexer)
{
    dft_indexer_destroy(indexer);
}
} // namespace indexer
} // namespace dft
#endif

#endif // __DFTRACER_UTILS_READER_INDEXER_H
