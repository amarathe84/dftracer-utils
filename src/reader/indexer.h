#ifndef __DFTRACER_UTILS_READER_INDEXER_H
#define __DFTRACER_UTILS_READER_INDEXER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /**
     * Opaque handle for DFT indexer
     */
    typedef void* dft_indexer_handle_t;

    /**
     * Create a new DFT indexer instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @param chunk_size_mb Chunk size for indexing in megabytes
     * @param force_rebuild Force rebuild even if index exists and chunk size matches
     * @return Opaque handle to the indexer instance, or NULL on failure
     */
    dft_indexer_handle_t dft_indexer_create(const char* gz_path, const char* idx_path, double chunk_size_mb, int force_rebuild);

    /**
     * Build or rebuild the index if necessary
     * @param indexer DFT indexer handle
     * @return 0 on success, -1 on error
     */
    int dft_indexer_build(dft_indexer_handle_t indexer);

    /**
     * Check if a rebuild is needed (index doesn't exist, is invalid, or chunk size differs)
     * @param indexer DFT indexer handle
     * @return 1 if rebuild is needed, 0 if not needed, -1 on error
     */
    int dft_indexer_need_rebuild(dft_indexer_handle_t indexer);

    /**
     * Destroy a DFT indexer instance and free all associated resources
     * @param indexer Opaque handle to the indexer instance
     */
    void dft_indexer_destroy(dft_indexer_handle_t indexer);

#ifdef __cplusplus
} // extern "C"

#include <string>
#include <memory>

namespace dft {
namespace indexer {

/**
 * C++ RAII wrapper for DFT indexer
 *
 * Example usage:
 * ```cpp
 * try {
 *     dft::indexer::Indexer indexer("trace.gz", "trace.gz.idx", 1.0);
 *     if (indexer.need_rebuild()) {
 *         indexer.build();
 *     }
 * } catch (const std::runtime_error& e) {
 *     // Handle error
 * }
 * ```
 */
class Indexer {
public:
    /**
     * Create a new DFT indexer instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @param chunk_size_mb Chunk size for indexing in megabytes
     * @param force_rebuild Force rebuild even if index exists and chunk size matches
     * @throws std::runtime_error if indexer creation fails
     */
    Indexer(const std::string& gz_path, const std::string& idx_path, double chunk_size_mb, bool force_rebuild = false);

    /**
     * Destructor - automatically destroys the indexer
     */
    ~Indexer();

    // Disable copy constructor and copy assignment
    Indexer(const Indexer&) = delete;
    Indexer& operator=(const Indexer&) = delete;

    /**
     * Move constructor
     */
    Indexer(Indexer&& other) noexcept;

    /**
     * Move assignment operator
     */
    Indexer& operator=(Indexer&& other) noexcept;

    /**
     * Build or rebuild the index if necessary
     * @throws std::runtime_error if build fails
     */
    void build();

    /**
     * Check if a rebuild is needed
     * @return true if rebuild is needed, false if not needed
     * @throws std::runtime_error on error
     */
    bool need_rebuild() const;

    /**
     * Check if the indexer is valid
     * @return true if indexer is valid, false otherwise
     */
    bool is_valid() const;

    /**
     * Get the gzip file path
     * @return gzip file path
     */
    const std::string& get_gz_path() const;

    /**
     * Get the index file path
     * @return index file path
     */
    const std::string& get_idx_path() const;

    /**
     * Get the chunk size in megabytes
     * @return chunk size in megabytes
     */
    double get_chunk_size_mb() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl_;
};

} // namespace indexer
} // namespace dft
#endif

#endif // __DFTRACER_UTILS_READER_INDEXER_H
