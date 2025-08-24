#ifndef DFTRACER_UTILS_INDEXER_INDEXER_H
#define DFTRACER_UTILS_INDEXER_INDEXER_H

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

typedef struct dft_indexer_checkpoint_t {
  uint64_t checkpoint_idx;
  uint64_t uc_offset;
  uint64_t uc_size;
  uint64_t c_offset;
  uint64_t c_size;
  int bits;
  unsigned char *dict_compressed;  // Owned by this struct
  size_t dict_size;
  uint64_t num_lines;
} dft_indexer_checkpoint_info_t;

/**
 * Find the best checkpoint for a given uncompressed offset
 * @param indexer DFT indexer handle
 * @param target_offset Target uncompressed offset
 * @param checkpoint DFT indexer checkpoint
 * @return 1 if checkpoint found, 0 if not found, -1 on error
 */
int dft_indexer_find_checkpoint(dft_indexer_handle_t indexer,
                                size_t target_offset,
                                dft_indexer_checkpoint_info_t *checkpoint);

/**
 * Get all checkpoints for this file as arrays
 * @param indexer DFT indexer handle
 * @param checkpoints Output: array of checkpoint information (caller must free
 * using dft_indexer_free_checkpoints)
 * @param count Output: number of checkpoints
 */
int dft_indexer_get_checkpoints(dft_indexer_handle_t indexer,
                                dft_indexer_checkpoint_info_t **checkpoints,
                                size_t *count);

/*
 * Free memory allocated by dft_indexer_get_checkpoints
 * @param checkpoint Checkpoint information to free
 */
void dft_indexer_free_checkpoint(dft_indexer_checkpoint_info_t *checkpoint);

/*
 * Free memory allocated by dft_indexer_get_checkpoints
 * @param checkpoints Array of checkpoint information to free
 * @param count Number of checkpoints
 */
void dft_indexer_free_checkpoints(dft_indexer_checkpoint_info_t *checkpoints,
                                  size_t count);

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

namespace dftracer::utils::indexer {

/**
 * Constants for zlib decompression and checkpoint management
 */
static constexpr size_t ZLIB_WINDOW_SIZE =
    32768;  // 32KB - Standard zlib window size

/**
 * Information about a checkpoint in the compressed file
 * Now includes chunk information for unified access
 */
struct CheckpointInfo {
  size_t checkpoint_idx;                       // Checkpoint index
  size_t uc_offset;                            // Uncompressed offset
  size_t uc_size;                              // Uncompressed size (from chunk)
  size_t c_offset;                             // Compressed offset
  size_t c_size;                               // Compressed size (from chunk)
  int bits;                                    // Bit position
  std::vector<unsigned char> dict_compressed;  // Compressed dictionary
  size_t num_lines;                            // Number of lines in this chunk

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
 *     dftracer::utils::indexer::Indexer indexer("trace.gz",
 * "trace.gz.idx", 1.0); if (indexer.need_rebuild()) { indexer.build();
 *     }
 * } catch (const std::runtime_error& e) {
 *     // Handle error
 * }
 * ```
 */
class Indexer {
 public:
  static constexpr size_t DEFAULT_CHECKPOINT_SIZE = 32 * 1024 * 1024;

  /**
   * Create a new DFT indexer instance
   * @param gz_path Path to the gzipped trace file
   * @param idx_path Path to the index file
   * @param checkpoint_size Checkpoint size for indexing in bytes
   * @param force_rebuild Force rebuild even if index exists and chunk size
   * matches
   * @throws std::runtime_error if indexer creation fails
   */
  Indexer(const std::string &gz_path, const std::string &idx_path,
          size_t checkpoint_size = DEFAULT_CHECKPOINT_SIZE,
          bool force_rebuild = false);

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
   * Get the checkpoint size in bytes
   * @return checkpoint size in bytes
   */
  size_t get_checkpoint_size() const;

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
   * @param target_offset Target uncompressed offset
   * @param checkpoint Output parameter for checkpoint information
   * @return true if checkpoint found, false otherwise
   * @throws std::runtime_error on database error
   */
  bool find_checkpoint(size_t target_offset, CheckpointInfo &checkpoint) const;

  /**
   * Get all checkpoints for this file as a list
   * @return vector of all checkpoints ordered by uncompressed offset
   * @throws std::runtime_error on database error
   */
  std::vector<CheckpointInfo> get_checkpoints() const;

  /**
   * Find checkpoints that contain data for a specific line range
   * Uses the num_lines field in checkpoints to efficiently locate relevant
   * checkpoints
   * @param start_line Starting line number (1-based)
   * @param end_line Ending line number (1-based, inclusive)
   * @return vector of checkpoints that cover the specified line range
   * @throws std::runtime_error on database error
   */
  std::vector<CheckpointInfo> find_checkpoints_by_line_range(
      size_t start_line, size_t end_line) const;

  /**
   * Get the cached file ID for this indexer instance
   * @return file ID, or -1 if not found
   * @throws std::runtime_error on database error
   */
  int get_file_id() const;

  // Error class for exception handling
  class Error : public std::runtime_error {
   public:
    enum Type {
      DATABASE_ERROR,
      FILE_ERROR,
      COMPRESSION_ERROR,
      INVALID_ARGUMENT,
      BUILD_ERROR,
      UNKNOWN_ERROR
    };

    Error(Type type, const std::string &message)
        : std::runtime_error(format_message(type, message)), type_(type) {}

    Type type() const { return type_; }

   private:
    Type type_;
    static std::string format_message(Type type, const std::string &message);
  };

 private:
  class Impl;
  std::unique_ptr<Impl> pImpl_;
};

}  // namespace dftracer::utils::indexer
#endif

#endif  // DFTRACER_UTILS_INDEXER_INDEXER_H
