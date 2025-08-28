#ifndef DFTRACER_UTILS_READER_READER_IMPL_H
#define DFTRACER_UTILS_READER_READER_IMPL_H

#include <dftracer/utils/reader/streams/byte_stream.h>
#include <dftracer/utils/reader/streams/factory.h>
#include <dftracer/utils/reader/streams/line_byte_stream.h>
#include <dftracer/utils/utils/json.h>

#include <cstdint>
#include <string>

namespace dftracer::utils {
class Indexer;

struct ReaderImplementor {
    std::string gz_path;
    std::string idx_path;
    bool is_open;
    size_t default_buffer_size;
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

    std::string read_lines(size_t start_line, size_t end_line);

    // JSON reader
    inline json::JsonDocuments read_json_lines(std::size_t start,
                                               std::size_t end) {
        std::string lines_data = read_lines(start, end);
        return json::parse_json_lines(lines_data.data(), lines_data.size());
    }
    inline json::OwnedJsonDocuments read_json_lines_owned(std::size_t start,
                                                          std::size_t end) {
        std::string lines_data = read_lines(start, end);
        return json::parse_json_lines_owned(lines_data.data(),
                                            lines_data.size());
    }

    json::JsonDocuments read_json_lines_bytes(std::size_t start_bytes,
                                              std::size_t end_bytes);
    json::OwnedJsonDocuments read_json_lines_bytes_owned(size_t start_bytes,
                                                         size_t end_bytes);

    void reset();
    inline bool is_valid() const { return is_open && indexer != nullptr; }
    inline const std::string &get_gz_path() const { return gz_path; }
    inline const std::string &get_idx_path() const { return idx_path; }
    inline void set_buffer_size(size_t size) { default_buffer_size = size; }

   private:
    std::string read_lines_from_beginning(std::size_t start_line,
                                          std::size_t end_line);

   private:
    bool is_indexer_initialized_internally;
    std::unique_ptr<StreamFactory> stream_factory;
    std::unique_ptr<LineByteStream> line_byte_stream;
    std::unique_ptr<ByteStream> byte_stream;
};
} // namespace dftracer::utils

#endif  // DFTRACER_UTILS_READER_READER_IMPL_H
