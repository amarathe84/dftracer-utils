#ifndef DFTRACER_UTILS_INDEXER_INDEXER_H
#define DFTRACER_UTILS_INDEXER_INDEXER_H

#include <dftracer/utils/indexer/checkpoint.h>

#ifdef __cplusplus
extern "C" {
#endif
#include <stddef.h>
#include <stdint.h>

/**
 * Opaque handle for DFT indexer
 */
typedef void *dft_indexer_handle_t;

/**
 * Create a new DFT indexer instance
 * @param gz_path Path to the gzipped trace file
 * @param idx_path Path to the index file
 * @param checkpoint_size Chunk size for indexing in bytes
 * @param force_rebuild Force rebuild index
 * @return Opaque handle to the indexer instance, or NULL on failure
 */
dft_indexer_handle_t dft_indexer_create(const char *gz_path,
                                        const char *idx_path,
                                        size_t checkpoint_size,
                                        int force_rebuild);

/**
 * Build or rebuild the index if necessary
 * @param indexer DFT indexer handle
 * @return 0 on success, -1 on error
 */
int dft_indexer_build(dft_indexer_handle_t indexer);

/**
 * Check if a rebuild is needed (index doesn't exist, is invalid, or chunk size
 * differs)
 * @param indexer DFT indexer handle
 * @return 1 if rebuild is needed, 0 if not needed, -1 on error
 */
int dft_indexer_need_rebuild(dft_indexer_handle_t indexer);

/**
 * Get the maximum uncompressed bytes in the indexed file
 * @param indexer DFT indexer handle
 * @return maximum uncompressed bytes, or 0 if index doesn't exist or on error
 */
uint64_t dft_indexer_get_max_bytes(dft_indexer_handle_t indexer);

/**
 * Get the total number of lines in the indexed file
 * @param indexer DFT indexer handle
 * @return total number of lines, or 0 if index doesn't exist or on error
 */
uint64_t dft_indexer_get_num_lines(dft_indexer_handle_t indexer);

/**
 * Find the database file ID for a given gzip path
 * @param indexer DFT indexer handle
 * @param gz_path Path to the gzipped file
 * @return file ID, or -1 if not found or on error
 */
int dft_indexer_find_file_id(dft_indexer_handle_t indexer, const char *gz_path);

/**
 * Find the best checkpoint for a given uncompressed offset
 * @param indexer DFT indexer handle
 * @param target_offset Target uncompressed offset
 * @param checkpoint DFT indexer checkpoint
 * @return 1 if checkpoint found, 0 if not found, -1 on error
 */
int dft_indexer_find_checkpoint(dft_indexer_handle_t indexer,
                                size_t target_offset,
                                dft_indexer_checkpoint_t *checkpoint);

/**
 * Get all checkpoints for this file as arrays
 * @param indexer DFT indexer handle
 * @param checkpoints Output: array of checkpoint information (caller must free
 * using dft_indexer_free_checkpoints)
 * @param count Output: number of checkpoints
 */
int dft_indexer_get_checkpoints(dft_indexer_handle_t indexer,
                                dft_indexer_checkpoint_t **checkpoints,
                                size_t *count);

/**
 * Destroy a DFT indexer instance and free all associated resources
 * @param indexer Opaque handle to the indexer instance
 */
void dft_indexer_destroy(dft_indexer_handle_t indexer);

#ifdef __cplusplus
}  // extern "C"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace dftracer::utils {

class IndexerImplementor;
class Indexer {
   public:
    static constexpr size_t DEFAULT_CHECKPOINT_SIZE = 32 * 1024 * 1024;

    Indexer(const std::string &gz_path, const std::string &idx_path,
            size_t checkpoint_size = DEFAULT_CHECKPOINT_SIZE,
            bool force_rebuild = false);
    ~Indexer();
    Indexer(const Indexer &) = delete;
    Indexer &operator=(const Indexer &) = delete;
    Indexer(Indexer &&other) noexcept;
    Indexer &operator=(Indexer &&other) noexcept;
    void build();
    bool need_rebuild() const;
    bool is_valid() const;

    // Metadata
    const std::string &get_idx_path() const;
    const std::string &get_gz_path() const;
    size_t get_checkpoint_size() const;
    uint64_t get_max_bytes() const;
    uint64_t get_num_lines() const;
    int get_file_id() const;

    // Lookup
    int find_file_id(const std::string &gz_path) const;
    bool find_checkpoint(size_t target_offset,
                         IndexCheckpoint &checkpoint) const;
    std::vector<IndexCheckpoint> get_checkpoints() const;
    std::vector<IndexCheckpoint> find_checkpoints_by_line_range(
        size_t start_line, size_t end_line) const;

   private:
    std::unique_ptr<IndexerImplementor> p_impl_;
};

}  // namespace dftracer::utils
#endif

#endif  // DFTRACER_UTILS_INDEXER_INDEXER_H
