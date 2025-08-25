#ifndef DFTRACER_UTILS_INDEXER_INFLATE_H
#define DFTRACER_UTILS_INDEXER_INFLATE_H

#include <dftracer/utils/indexer/constants.h>
#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>

namespace dftracer::utils::indexer {
class Inflater {
   public:
    FILE *file;
    z_stream stream;

   public:
    Inflater(FILE *file_) : file(file_) { reset(); }

    ~Inflater() { inflateEnd(&stream); }

    void reset() {
        inflateEnd(&stream);
        memset(&stream, 0, sizeof(stream));
        if (inflateInit2(&stream, constants::ZLIB_GZIP_WINDOW_BITS) != Z_OK) {
            throw std::runtime_error("Failed to reinitialize inflater");
        }
    }

    /*
     * Process the input buffer and inflate it into the output buffer.
     * Returns the number of bytes written to the output buffer.
     *
     * @param buf The input buffer to process.
     * @param buf_size The size of the input buffer.
     * @param c_off The offset to start writing to the output buffer.
     * @return The number of bytes written to the output buffer.
     */
    bool process(unsigned char *buf, size_t buf_size, size_t *bytes_out,
                 size_t *c_off) {
        stream.next_out = buf;
        stream.avail_out = static_cast<uInt>(buf_size);
        c_off = 0;

        while (stream.avail_out > 0) {
            if (stream.avail_in == 0) {
                size_t n =
                    fread(internal_buffer_, 1, sizeof(internal_buffer_), file);
                if (n == 0) {
                    break;
                }
                stream.next_in = internal_buffer_;
                stream.avail_in = static_cast<uInt>(n);
            }
            size_t c_pos_before =
                static_cast<size_t>(ftello(file)) - stream.avail_in;
            int ret = inflate(&stream, Z_BLOCK);

            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                // error
                return false;
            }

            *c_off = c_pos_before;

            // Break early if we've processed at least some data and hit a block
            // boundary This allows us to check for checkpoint opportunities
            // after each block
            if (*bytes_out > 0 && (stream.data_type & 0xc0) == 0x80) {
                break;
            }
        }

        *bytes_out = buf_size - stream.avail_out;
        return true;
    }

   private:
    unsigned char internal_buffer_[constants::INFLATE_BUFFER_SIZE];
};

}  // namespace dftracer::utils::indexer

#endif  // DFTRACER_UTILS_INDEXER_INFLATE_H
