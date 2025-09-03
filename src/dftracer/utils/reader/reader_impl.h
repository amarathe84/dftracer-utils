#ifndef DFTRACER_UTILS_READER_READER_IMPL_H
#define DFTRACER_UTILS_READER_READER_IMPL_H

#include <dftracer/utils/reader/line_processor.h>
#include <dftracer/utils/reader/streams/byte_stream.h>
#include <dftracer/utils/reader/streams/factory.h>
#include <dftracer/utils/reader/streams/line_byte_stream.h>

#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>

namespace dftracer::utils {
class Indexer;

struct ReaderImplementor {
    std::string gz_path;
    std::string idx_path;
    bool is_open;
    std::size_t default_buffer_size;
    Indexer *indexer;

    ReaderImplementor(const std::string &gz_path, const std::string &idx_path,
                      std::size_t index_ckpt_size);
    ReaderImplementor(Indexer *indexer);
    ~ReaderImplementor();

    std::size_t get_max_bytes() const;
    std::size_t get_num_lines() const;

    // Reader
    std::size_t read(std::size_t start_bytes, std::size_t end_bytes,
                     char *buffer, std::size_t buffer_size);
    std::size_t read_line_bytes(std::size_t start_bytes, std::size_t end_bytes,
                                char *buffer, std::size_t buffer_size);

    std::string read_lines(std::size_t start_line, std::size_t end_line);
    void read_lines_with_processor(std::size_t start_line, std::size_t end_line,
                                   LineProcessor &processor);
    void read_line_bytes_with_processor(std::size_t start_bytes,
                                        std::size_t end_bytes,
                                        LineProcessor &processor);

    void reset();
    inline bool is_valid() const { return is_open && indexer != nullptr; }
    inline const std::string &get_gz_path() const { return gz_path; }
    inline const std::string &get_idx_path() const { return idx_path; }
    inline void set_buffer_size(std::size_t size) {
        default_buffer_size = size;
    }

   private:
    std::size_t process_lines(const char *buffer_data, std::size_t buffer_size,
                              std::size_t &current_line, std::size_t start_line,
                              std::size_t end_line,
                              std::string &line_accumulator,
                              LineProcessor &processor);

   private:
    bool is_indexer_initialized_internally;
    std::unique_ptr<StreamFactory> stream_factory;
    std::unique_ptr<LineByteStream> line_byte_stream;
    std::unique_ptr<ByteStream> byte_stream;
};
}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_READER_READER_IMPL_H
