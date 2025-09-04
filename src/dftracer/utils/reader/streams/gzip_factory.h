#ifndef DFTRACER_UTILS_READER_STREAMS_GZIP_FACTORY_H
#define DFTRACER_UTILS_READER_STREAMS_GZIP_FACTORY_H

#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/streams/gzip_byte_stream.h>
#include <dftracer/utils/reader/streams/gzip_line_byte_stream.h>

#include <memory>
#include <string>

class GzipStreamFactory {
   private:
    dftracer::utils::Indexer &indexer_;

   public:
    explicit GzipStreamFactory(dftracer::utils::Indexer &indexer) : indexer_(indexer) {}

    std::unique_ptr<GzipLineByteStream> create_line_stream(
        const std::string &gz_path, size_t start_bytes, size_t end_bytes) {
        auto session = std::make_unique<GzipLineByteStream>();
        session->initialize(gz_path, start_bytes, end_bytes, indexer_);
        return session;
    }

    std::unique_ptr<GzipByteStream> create_byte_stream(const std::string &gz_path,
                                                   size_t start_bytes,
                                                   size_t end_bytes) {
        auto session = std::make_unique<GzipByteStream>();
        session->initialize(gz_path, start_bytes, end_bytes, indexer_);
        return session;
    }

    bool needs_new_line_stream(const GzipLineByteStream *current,
                               const std::string &gz_path, size_t start_bytes,
                               size_t end_bytes) const {
        return !current || !current->matches(gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }

    bool needs_new_byte_stream(const GzipByteStream *current,
                               const std::string &gz_path, size_t start_bytes,
                               size_t end_bytes) const {
        return !current || !current->matches(gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }
};

#endif  // DFTRACER_UTILS_READER_STREAMS_FACTORY_H
