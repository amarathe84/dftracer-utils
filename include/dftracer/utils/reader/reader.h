#ifndef DFTRACER_UTILS_READER_READER_H
#define DFTRACER_UTILS_READER_READER_H

#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/line_processor.h>

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

/**
 * Opaque handle for DFT reader
 */
typedef void *dft_reader_handle_t;
dft_reader_handle_t dft_reader_create(const char *gz_path, const char *idx_path,
                                      size_t index_ckpt_size);
dft_reader_handle_t dft_reader_create_with_indexer(
    dft_indexer_handle_t indexer);
void dft_reader_destroy(dft_reader_handle_t reader);
int dft_reader_get_max_bytes(dft_reader_handle_t reader, size_t *max_bytes);
int dft_reader_get_num_lines(dft_reader_handle_t reader, size_t *num_lines);
int dft_reader_read(dft_reader_handle_t reader, size_t start_bytes,
                    size_t end_bytes, char *buffer, size_t buffer_size);
int dft_reader_read_line_bytes(dft_reader_handle_t reader, size_t start_bytes,
                               size_t end_bytes, char *buffer,
                               size_t buffer_size);
int dft_reader_read_lines(dft_reader_handle_t reader, size_t start_line,
                          size_t end_line, char *buffer, size_t buffer_size,
                          size_t *bytes_written);
int dft_reader_read_lines_with_processor(dft_reader_handle_t reader, 
                                        size_t start_line, size_t end_line,
                                        dft_line_processor_callback_t callback,
                                        void *user_data);
void dft_reader_reset(dft_reader_handle_t reader);
#ifdef __cplusplus
}  // extern "C"

#include <dftracer/utils/common/constants.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace dftracer::utils {

class Indexer;
struct ReaderImplementor;

class Reader {
   public:
    Reader(const std::string &gz_path, const std::string &idx_path,
           size_t index_ckpt_size = Indexer::DEFAULT_CHECKPOINT_SIZE);

    Reader(Indexer *indexer);
    ~Reader();

    // Disable copy constructor and copy assignment
    Reader(const Reader &) = delete;
    Reader &operator=(const Reader &) = delete;
    Reader(Reader &&other) noexcept;
    Reader &operator=(Reader &&other) noexcept;

    /**
     * Get maximum bytes available in the gzip file
     */
    std::size_t get_max_bytes() const;

    /**
     * Get number of lines in the gzip file
     */
    std::size_t get_num_lines() const;

    /**
     * Read raw bytes from the gzip file using the stored gz_path (streaming)
     * Returns data without caring about line boundaries. Call repeatedly until
     * returns 0.
     */
    std::size_t read(std::size_t start_bytes, std::size_t end_bytes,
                     char *buffer, std::size_t buffer_size);

    /**
     * Read a range of bytes from the gzip file using the stored gz_path
     * (streaming) Returns complete lines only. Call repeatedly until returns 0.
     */
    std::size_t read_line_bytes(std::size_t start_bytes, std::size_t end_bytes,
                                char *buffer, std::size_t buffer_size);

    /**
     * Read complete lines from the gzip file and return as a string
     */
    std::string read_lines(std::size_t start, std::size_t end);

    /**
     * Read complete lines from the gzip file using callback processor (zero-copy)
     * @param start Starting line number (1-based)
     * @param end Ending line number (1-based) 
     * @param processor LineProcessor implementation for handling lines
     */
    void read_lines_with_processor(std::size_t start, std::size_t end, 
                                  LineProcessor& processor);

    /**
     * Read complete lines from byte range using callback processor (zero-copy)
     * @param start_bytes Starting byte position
     * @param end_bytes Ending byte position
     * @param processor LineProcessor implementation for handling lines
     */
    void read_line_bytes_with_processor(std::size_t start_bytes, std::size_t end_bytes,
                                       LineProcessor& processor);

    /**
     * Set default reader buffer size in bytes
     */
    void set_buffer_size(std::size_t size);

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
    std::unique_ptr<ReaderImplementor> p_impl_;
};

}  // namespace dftracer::utils
#endif

#endif  // DFTRACER_UTILS_READER_READER_H
