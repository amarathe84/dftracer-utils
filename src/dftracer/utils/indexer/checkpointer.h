#ifndef DFTRACER_UTILS_INDEXER_CHECKPOINTER_H
#define DFTRACER_UTILS_INDEXER_CHECKPOINTER_H

#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/checkpoint.h>
#include <dftracer/utils/indexer/inflater.h>

#include <cstddef>
#include <cstdint>

using namespace dftracer::utils;

struct Checkpointer {
    std::size_t uc_offset;
    std::size_t c_offset;
    int bits;
    Inflater &inflater;
    unsigned char window[constants::indexer::ZLIB_WINDOW_SIZE];

    Checkpointer(Inflater &in, std::size_t uc_offset_ = 0)
        : uc_offset(uc_offset_), c_offset(0), bits(0), inflater(in) {
        std::memset(window, 0, sizeof(window));
    }

    bool create() {
        // Get precise compressed position: file position minus unprocessed
        // input
        size_t file_pos = static_cast<size_t>(ftello(inflater.file));
        size_t absolute_c_offset = file_pos - inflater.stream.avail_in;

        // Store absolute file position (as in original zran)
        c_offset = absolute_c_offset;

        // Get bit offset from zlib state (following zran approach)
        bits = inflater.stream.data_type & 7;

        // Try to get the sliding window dictionary from zlib
        // This contains the last 32KB of uncompressed data
        // Only attempt this when the zlib state is stable
        unsigned have = 0;
        if ((inflater.stream.data_type & 0xc0) == 0x80 &&
            inflater.stream.avail_out == 0 &&
            inflateGetDictionary(&inflater.stream, window, &have) == Z_OK &&
            have > 0) {
            // Got dictionary successfully
            if (have < constants::indexer::ZLIB_WINDOW_SIZE) {
                // If less than 32KB available, right-align and pad with zeros
                std::memmove(
                    window + (constants::indexer::ZLIB_WINDOW_SIZE - have),
                    window, have);
                std::memset(window, 0,
                            constants::indexer::ZLIB_WINDOW_SIZE - have);
            }

            DFTRACER_UTILS_LOG_DEBUG(
                "Created checkpoint: uc_offset=%zu, c_offset=%zu, bits=%d, "
                "dict_size=%u",
                uc_offset, c_offset, bits, have);
            return true;
        }

        // If we can't get dictionary from zlib, this checkpoint won't work
        DFTRACER_UTILS_LOG_DEBUG(
            "Could not get dictionary for checkpoint at offset %zu", uc_offset);
        return false;
    }

    bool compress(unsigned char **compressed,
                  std::size_t *compressed_size) const {
        z_stream zs;
        memset(&zs, 0, sizeof(zs));

        if (deflateInit(&zs, Z_BEST_COMPRESSION) != Z_OK) {
            DFTRACER_UTILS_LOG_DEBUG("Failed to initialize zlib");
            return false;
        }

        size_t max_compressed =
            deflateBound(&zs, constants::indexer::ZLIB_WINDOW_SIZE);
        *compressed = static_cast<unsigned char *>(malloc(max_compressed));
        if (!*compressed) {
            DFTRACER_UTILS_LOG_DEBUG(
                "Failed to allocate memory for compressed data");
            deflateEnd(&zs);
            return false;
        }

        zs.next_in = const_cast<unsigned char *>(window);
        zs.avail_in = static_cast<uInt>(constants::indexer::ZLIB_WINDOW_SIZE);
        zs.next_out = *compressed;
        zs.avail_out = static_cast<uInt>(max_compressed);

        int ret = deflate(&zs, Z_FINISH);
        if (ret != Z_STREAM_END) {
            free(*compressed);
            deflateEnd(&zs);
            DFTRACER_UTILS_LOG_DEBUG("Failed to compress dictionary data");
            return false;
        }

        *compressed_size = max_compressed - zs.avail_out;
        deflateEnd(&zs);
        return true;
    }
};

#endif  // DFTRACER_UTILS_INDEXER_CHECKPOINTER_H
