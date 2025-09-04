#ifndef DFTRACER_UTILS_READER_STREAMS_STREAM_H
#define DFTRACER_UTILS_READER_STREAMS_STREAM_H

#include <dftracer/utils/indexer/indexer.h>
class Stream {
   public:
    virtual ~Stream() { reset(); }
    virtual std::size_t stream(char *buffer, std::size_t buffer_size) = 0;
    virtual void reset() {}

   protected:
    virtual void initialize(const std::string &gz_path, std::size_t start_bytes,
                            std::size_t end_bytes, dftracer::utils::Indexer &indexer) = 0;

};

#endif  // DFTRACER_UTILS_READER_STREAMS_STREAM_H
