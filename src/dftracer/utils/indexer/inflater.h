#ifndef DFTRACER_UTILS_INDEXER_INFLATER_H
#define DFTRACER_UTILS_INDEXER_INFLATER_H

#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/logging.h>
#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

class Inflater {
   public:
    FILE *file;
    std::size_t bytes_read;
    z_stream stream;
    unsigned char buffer[constants::indexer::PROCESS_BUFFER_SIZE];

   public:
    Inflater(FILE *file_) : file(file_), bytes_read(0) {
        std::memset(&stream, 0, sizeof(stream));
        if (inflateInit2(&stream, constants::indexer::ZLIB_GZIP_WINDOW_BITS) !=
            Z_OK) {
            throw std::runtime_error("Failed to initialize inflater");
        }
        std::memset(buffer, 0, sizeof(buffer));
        std::memset(in_buffer, 0, sizeof(in_buffer));
    }

    ~Inflater() { inflateEnd(&stream); }

    void reset() {
        bytes_read = 0;
        inflateEnd(&stream);
        std::memset(&stream, 0, sizeof(stream));
        if (inflateInit2(&stream, constants::indexer::ZLIB_GZIP_WINDOW_BITS) !=
            Z_OK) {
            throw std::runtime_error("Failed to reinitialize inflater");
        }
        std::memset(buffer, 0, sizeof(buffer));
        std::memset(in_buffer, 0, sizeof(in_buffer));
    }

    /*
     * Process the input buffer and inflate it into the output buffer.
     * Returns the number of bytes written to the output buffer.
     *
     * @param buf The input buffer to process.
     * @return false if the process failed, true otherwise.
     */
    bool process() {
        stream.next_out = buffer;
        stream.avail_out = static_cast<uInt>(sizeof(buffer));
        bytes_read = 0;

        DFTRACER_UTILS_LOG_DEBUG(
            "Starting inflation process with buffer size %zu", sizeof(buffer));

        while (stream.avail_out > 0) {
            if (stream.avail_in == 0) {
                std::size_t n = fread(in_buffer, 1, sizeof(in_buffer), file);
                if (n == 0) {
                    break;
                }
                stream.next_in = in_buffer;
                stream.avail_in = static_cast<uInt>(n);
            }
            (void)ftello(file);
            int ret = inflate(&stream, Z_BLOCK);

            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                // error
                return false;
            }

            // Break early if we've processed at least some data and hit a block
            // boundary This allows us to check for checkpoint opportunities
            // after each block
            if (bytes_read > 0 && (stream.data_type & 0xc0) == 0x80) {
                DFTRACER_UTILS_LOG_DEBUG(
                    "Hit block boundary after processing %zu bytes",
                    bytes_read);
                break;
            }
        }

        bytes_read = constants::indexer::PROCESS_BUFFER_SIZE - stream.avail_out;
        return true;
    }

   private:
    unsigned char in_buffer[constants::indexer::INFLATE_BUFFER_SIZE];
};

#endif  // DFTRACER_UTILS_INDEXER_INFLATER_H
