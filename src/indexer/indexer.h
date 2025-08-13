#ifndef __DFTRACER_UTILS_INDEXER_INDEXER_H
#define __DFTRACER_UTILS_INDEXER_INDEXER_H

#ifdef __cplusplus
extern "C"
{
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
     * @param chunk_size_mb Chunk size for indexing in megabytes
     * @param force_rebuild Force rebuild index
     * @return Opaque handle to the indexer instance, or NULL on failure
     */
    dft_indexer_handle_t
    dft_indexer_create(const char *gz_path, const char *idx_path, double chunk_size_mb, int force_rebuild);

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
     * @param file_id Database file ID (from dft_indexer_find_file_id)
     * @param target_offset Target uncompressed offset
     * @param uc_offset Output: uncompressed offset of checkpoint
     * @param c_offset Output: compressed offset of checkpoint
     * @param bits Output: bit position of checkpoint
     * @param dict_compressed Output: compressed dictionary data (caller must free)
     * @param dict_size Output: size of compressed dictionary
     * @return 1 if checkpoint found, 0 if not found, -1 on error
     */
    int dft_indexer_find_checkpoint(dft_indexer_handle_t indexer,
                                    int file_id,
                                    uint64_t target_offset,
                                    uint64_t *uc_offset,
                                    uint64_t *c_offset,
                                    int *bits,
                                    unsigned char **dict_compressed,
                                    size_t *dict_size);

    /**
     * Free memory allocated by dft_indexer_find_checkpoint
     * @param dict_compressed Dictionary data to free
     */
    void dft_indexer_free_checkpoint_dict(unsigned char *dict_compressed);

    /**
     * Get all checkpoints for a given file as arrays
     * @param indexer DFT indexer handle
     * @param file_id Database file ID (from dft_indexer_find_file_id)
     * @param count Output: number of checkpoints
     * @param checkpoint_indices Output: array of checkpoint indices (caller must free)
     * @param uc_offsets Output: array of uncompressed offsets (caller must free)
     * @param uc_sizes Output: array of uncompressed sizes (caller must free)
     * @param c_offsets Output: array of compressed offsets (caller must free)
     * @param c_sizes Output: array of compressed sizes (caller must free)
     * @param bits_array Output: array of bit positions (caller must free)
     * @param dict_sizes Output: array of dictionary sizes (caller must free)
     * @param dict_data Output: array of dictionary data pointers (caller must free each and the array)
     * @param num_lines_array Output: array of line counts (caller must free)
     * @return 0 on success, -1 on error
     */
    int dft_indexer_get_checkpoints(dft_indexer_handle_t indexer,
                                    int file_id,
                                    size_t *count,
                                    uint64_t **checkpoint_indices,
                                    uint64_t **uc_offsets,
                                    uint64_t **uc_sizes,
                                    uint64_t **c_offsets,
                                    uint64_t **c_sizes,
                                    int **bits_array,
                                    size_t **dict_sizes,
                                    unsigned char ***dict_data,
                                    uint64_t **num_lines_array);

    /**
     * Free memory allocated by dft_indexer_get_checkpoints
     * @param count Number of checkpoints
     * @param checkpoint_indices Checkpoint indices array to free
     * @param uc_offsets Uncompressed offsets array to free
     * @param uc_sizes Uncompressed sizes array to free
     * @param c_offsets Compressed offsets array to free
     * @param c_sizes Compressed sizes array to free
     * @param bits_array Bits array to free
     * @param dict_sizes Dictionary sizes array to free
     * @param dict_data Dictionary data array to free (frees each dictionary and the array)
     * @param num_lines_array Line counts array to free
     */
    void dft_indexer_free_checkpoints(size_t count,
                                      uint64_t *checkpoint_indices,
                                      uint64_t *uc_offsets,
                                      uint64_t *uc_sizes,
                                      uint64_t *c_offsets,
                                      uint64_t *c_sizes,
                                      int *bits_array,
                                      size_t *dict_sizes,
                                      unsigned char **dict_data,
                                      uint64_t *num_lines_array);

    /**
     * Destroy a DFT indexer instance and free all associated resources
     * @param indexer Opaque handle to the indexer instance
     */
    void dft_indexer_destroy(dft_indexer_handle_t indexer);

#ifdef __cplusplus
} // extern "C"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace dft
{
namespace indexer
{

/**
 * Constants for zlib decompression and checkpoint management
 */
static constexpr size_t ZLIB_WINDOW_SIZE = 32768; // 32KB - Standard zlib window size

/**
 * Information about a checkpoint in the compressed file
 * Now includes chunk information for unified access
 */
struct CheckpointInfo
{
    size_t checkpoint_idx;                      // Checkpoint index
    size_t uc_offset;                           // Uncompressed offset
    size_t uc_size;                             // Uncompressed size (from chunk)
    size_t c_offset;                            // Compressed offset
    size_t c_size;                              // Compressed size (from chunk)
    int bits;                                   // Bit position
    std::vector<unsigned char> dict_compressed; // Compressed dictionary
    size_t num_lines;                           // Number of lines in this chunk

    CheckpointInfo() = default;
    CheckpointInfo(const CheckpointInfo &) = default;
    CheckpointInfo(CheckpointInfo &&) = default;
    CheckpointInfo &operator=(const CheckpointInfo &) = default;
    CheckpointInfo &operator=(CheckpointInfo &&) = default;
};

/**
 * DFT indexer
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
class Indexer
{
  public:
    /**
     * Create a new DFT indexer instance
     * @param gz_path Path to the gzipped trace file
     * @param idx_path Path to the index file
     * @param chunk_size_mb Chunk size for indexing in megabytes (also used for checkpoint interval)
     * @param force_rebuild Force rebuild even if index exists and chunk size matches
     * @throws std::runtime_error if indexer creation fails
     */
    Indexer(const std::string &gz_path, const std::string &idx_path, double chunk_size_mb, bool force_rebuild = false);

    /**
     * Destructor - automatically destroys the indexer
     */
    ~Indexer();

    // Disable copy constructor and copy assignment
    Indexer(const Indexer &) = delete;
    Indexer &operator=(const Indexer &) = delete;

    /**
     * Move constructor
     */
    Indexer(Indexer &&other) noexcept;

    /**
     * Move assignment operator
     */
    Indexer &operator=(Indexer &&other) noexcept;

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
    const std::string &get_gz_path() const;

    /**
     * Get the index file path
     * @return index file path
     */
    const std::string &get_idx_path() const;

    /**
     * Get the chunk size in megabytes (also used for checkpoint interval)
     * @return chunk size in megabytes
     */
    double get_chunk_size_mb() const;

    /**
     * Get the maximum uncompressed bytes in the indexed file
     * @return maximum uncompressed bytes, or 0 if index doesn't exist
     * @throws std::runtime_error on database error
     */
    uint64_t get_max_bytes() const;

    /**
     * Get the total number of lines in the indexed file
     * @return total number of lines, or 0 if index doesn't exist
     * @throws std::runtime_error on database error
     */
    uint64_t get_num_lines() const;

    /**
     * Find the database file ID for a given gzip path
     * @param gz_path Path to the gzipped file
     * @return file ID, or -1 if not found
     * @throws std::runtime_error on database error
     */
    int find_file_id(const std::string &gz_path) const;

    /**
     * Find the best checkpoint for a given uncompressed offset
     * @param file_id Database file ID (from find_file_id)
     * @param target_offset Target uncompressed offset
     * @param checkpoint Output parameter for checkpoint information
     * @return true if checkpoint found, false otherwise
     * @throws std::runtime_error on database error
     */
    bool find_checkpoint(int file_id, size_t target_offset, CheckpointInfo &checkpoint) const;

    /**
     * Get all checkpoints for a given file as a list
     * @param file_id Database file ID (from find_file_id)
     * @return vector of all checkpoints ordered by uncompressed offset
     * @throws std::runtime_error on database error
     */
    std::vector<CheckpointInfo> get_checkpoints(int file_id) const;

  private:
    class Impl;
    std::unique_ptr<Impl> pImpl_;
};

} // namespace indexer
} // namespace dft
#endif

#endif // __DFTRACER_UTILS_READER_INDEXER_H
