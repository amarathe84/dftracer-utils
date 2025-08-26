#ifndef DFTRACER_UTILS_READER_STREAMS_FACTORY_H
#define DFTRACER_UTILS_READER_STREAMS_FACTORY_H

#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/streams/byte_stream.h>
#include <dftracer/utils/reader/streams/line_byte_stream.h>

#include <memory>
#include <string>

class StreamFactory {
   private:
    Indexer &indexer_;

   public:
    explicit StreamFactory(Indexer &indexer) : indexer_(indexer) {}

    std::unique_ptr<LineByteStream> create_line_stream(
        const std::string &gz_path, size_t start_bytes, size_t end_bytes) {
        auto session = std::make_unique<LineByteStream>();
        session->initialize(gz_path, start_bytes, end_bytes, indexer_);
        return session;
    }

    std::unique_ptr<ByteStream> create_byte_stream(const std::string &gz_path,
                                                   size_t start_bytes,
                                                   size_t end_bytes) {
        auto session = std::make_unique<ByteStream>();
        session->initialize(gz_path, start_bytes, end_bytes, indexer_);
        return session;
    }

    bool needs_new_line_stream(const LineByteStream *current,
                               const std::string &gz_path, size_t start_bytes,
                               size_t end_bytes) const {
        return !current || !current->matches(gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }

    bool needs_new_byte_stream(const ByteStream *current,
                               const std::string &gz_path, size_t start_bytes,
                               size_t end_bytes) const {
        return !current || !current->matches(gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }
};

#endif  // DFTRACER_UTILS_READER_STREAMS_FACTORY_H
