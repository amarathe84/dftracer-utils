#ifndef DFTRACER_UTILS_COMMON_INFLATER_H
#define DFTRACER_UTILS_COMMON_INFLATER_H

#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/logging.h>
#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace dftracer::utils {
class Inflater {
   public:
    static constexpr std::size_t BUFFER_SIZE = 65536;
    int bits;
    std::uint64_t c_off;
    z_stream stream;
    alignas(64) unsigned char buffer[BUFFER_SIZE];
    alignas(64) unsigned char in_buffer[BUFFER_SIZE];

   public:
    Inflater() : bits(constants::indexer::ZLIB_GZIP_WINDOW_BITS), c_off(0) {
        std::memset(&stream, 0, sizeof(stream));
        std::memset(buffer, 0, sizeof(buffer));
        std::memset(in_buffer, 0, sizeof(in_buffer));
    }

    ~Inflater() { inflateEnd(&stream); }

    bool prime(int bits_, int value) {
        return inflatePrime(&stream, bits_, value) == Z_OK;
    }

    bool set_dictionary(const unsigned char *dict, std::size_t dict_size) {
        return inflateSetDictionary(&stream, dict,
                                    static_cast<uInt>(dict_size)) == Z_OK;
    }

    bool fread(FILE *file) {
        std::size_t n = ::fread(in_buffer, 1, sizeof(in_buffer), file);
        if (n > 0) {
            stream.next_in = in_buffer;
            stream.avail_in = static_cast<uInt>(n);
        } else if (std::ferror(file)) {
            DFTRACER_UTILS_LOG_DEBUG(
                "Error reading from file during inflation with "
                "error: %s",
                std::strerror(errno));
            return false;
        }
        return true;
    }

    bool initialize(FILE *file, std::uint64_t c_off_ = 0,
                    int bits_ = constants::indexer::ZLIB_GZIP_WINDOW_BITS) {
        bits = bits_;
        c_off = c_off_;
        std::memset(&stream, 0, sizeof(stream));

        // Stream type detection following zran.c approach
        int mode = bits_;
        if (mode == 0) {
            // Auto-detect stream type: RAW (-15), ZLIB (15), or GZIP (15+16)
            // Read first byte to determine format
            if (fseeko(file, static_cast<off_t>(c_off_), SEEK_SET) == 0) {
                int first_byte = fgetc(file);
                if (first_byte != EOF) {
                    if (first_byte == 0x1f) {
                        // Likely GZIP magic number
                        mode = constants::indexer::ZLIB_GZIP_WINDOW_BITS; // 15+16
                    } else if ((first_byte & 0xf) == 8) {
                        // Likely ZLIB format
                        mode = 15;
                    } else {
                        // Default to RAW deflate
                        mode = -15;
                    }
                    // Seek back to start
                    fseeko(file, static_cast<off_t>(c_off_), SEEK_SET);
                } else {
                    mode = constants::indexer::ZLIB_GZIP_WINDOW_BITS; // Default to GZIP
                }
            } else {
                mode = constants::indexer::ZLIB_GZIP_WINDOW_BITS; // Default to GZIP
            }
            bits = mode;
        }

        if (inflateInit2(&stream, bits) != Z_OK) {
            throw std::runtime_error("Failed to initialize inflater");
        }

        if (fseeko(file, static_cast<off_t>(c_off), SEEK_SET) != 0) {
            throw std::runtime_error("Failed to seek to initial offset");
        }
        std::memset(buffer, 0, sizeof(buffer));
        std::memset(in_buffer, 0, sizeof(in_buffer));

        // Reset stream input state
        stream.avail_in = 0;
        stream.next_in = nullptr;

        return true;
    }

    void reset() {
        inflateEnd(&stream);
        std::memset(&stream, 0, sizeof(stream));
    }

    bool read(FILE *file, unsigned char *buf, std::size_t len,
              std::size_t &bytes_out) {
        stream.next_out = buf;
        stream.avail_out = static_cast<uInt>(len);
        bytes_out = 0;

        while (stream.avail_out > 0) {
            if (stream.avail_in == 0) {
                std::size_t n = ::fread(in_buffer, 1, sizeof(in_buffer), file);
                if (n == 0) {
                    if (std::ferror(file)) {
                        DFTRACER_UTILS_LOG_DEBUG(
                            "Error reading from file during inflation with "
                            "error: %s",
                            std::strerror(errno));
                        return false;
                    }
                    break;
                }
                stream.next_in = in_buffer;
                stream.avail_in = static_cast<uInt>(n);
            }
            int ret = inflate(&stream, Z_NO_FLUSH);

            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                DFTRACER_UTILS_LOG_DEBUG(
                    "inflate() failed with error: %d (%s)", ret,
                    stream.msg ? stream.msg : "no message");
                return false;
            }
        }

        bytes_out = len - stream.avail_out;
        return true;
    }

    bool read_and_count_lines(FILE *file, std::size_t &bytes_read,
                              std::uint64_t &lines_found) {
        bool result = read(file, buffer, sizeof(buffer), bytes_read);
        if (!result) {
            return false;
        }
        lines_found = 0;
        for (std::size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                lines_found++;
            }
        }
        return true;
    }

    // New method that uses Z_BLOCK for proper block boundary detection
    bool read_and_count_lines_with_blocks(FILE *file, std::size_t &bytes_read,
                                         std::uint64_t &lines_found) {
        stream.next_out = buffer;
        stream.avail_out = static_cast<uInt>(sizeof(buffer));
        bytes_read = 0;
        lines_found = 0;

        while (stream.avail_out > 0) {
            if (stream.avail_in == 0) {
                std::size_t n = ::fread(in_buffer, 1, sizeof(in_buffer), file);
                if (n == 0) {
                    if (std::ferror(file)) {
                        DFTRACER_UTILS_LOG_DEBUG(
                            "Error reading from file during inflation with "
                            "error: %s",
                            std::strerror(errno));
                        return false;
                    }
                    break;
                }
                stream.next_in = in_buffer;
                stream.avail_in = static_cast<uInt>(n);
            }
            
            int ret = inflate(&stream, Z_BLOCK);  // Use Z_BLOCK instead of Z_NO_FLUSH

            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                DFTRACER_UTILS_LOG_DEBUG(
                    "inflate() failed with error: %d (%s)", ret,
                    stream.msg ? stream.msg : "no message");
                return false;
            }
            
            // Check if we're at a proper block boundary (end of header or non-last deflate block)
            // Following zran.c: don't break immediately, continue until proper conditions are met
            if ((stream.data_type & 0xc0) == 0x80) {
                // At proper block boundary, but continue decompression for indexing
                // The indexer will handle checkpoint creation timing
                continue;
            }
        }

        bytes_read = sizeof(buffer) - stream.avail_out;
        
        // Count lines in decompressed data
        for (std::size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                lines_found++;
            }
        }
        
        return true;
    }

    // Method for indexer that tracks input bytes consumed
    bool read_and_count_lines_with_blocks_track_input(FILE *file, std::size_t &bytes_read,
                                                      std::uint64_t &lines_found,
                                                      std::size_t &total_input_bytes) {
        stream.next_out = buffer;
        stream.avail_out = static_cast<uInt>(sizeof(buffer));
        bytes_read = 0;
        lines_found = 0;

        while (stream.avail_out > 0) {
            if (stream.avail_in == 0) {
                std::size_t n = ::fread(in_buffer, 1, sizeof(in_buffer), file);
                if (n == 0) {
                    if (std::ferror(file)) {
                        DFTRACER_UTILS_LOG_DEBUG(
                            "Error reading from file during inflation with "
                            "error: %s",
                            std::strerror(errno));
                        return false;
                    }
                    break;
                }
                stream.next_in = in_buffer;
                stream.avail_in = static_cast<uInt>(n);
                total_input_bytes += n;  // Track total input bytes consumed
            }
            
            int ret = inflate(&stream, Z_BLOCK);  // Use Z_BLOCK for boundary detection

            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                DFTRACER_UTILS_LOG_DEBUG(
                    "inflate() failed with error: %d (%s)", ret,
                    stream.msg ? stream.msg : "no message");
                return false;
            }
            
            // Continue processing - don't break on block boundaries for indexer
            // The main indexer loop will handle checkpoint creation timing
        }

        bytes_read = sizeof(buffer) - stream.avail_out;
        
        // Count lines in decompressed data
        for (std::size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                lines_found++;
            }
        }
        
        return true;
    }

    // Variant for reader streams - uses Z_NO_FLUSH without early breaking
    bool read_continuous(FILE *file, unsigned char *buf, std::size_t len,
                         std::size_t &bytes_out) {
        stream.next_out = buf;
        stream.avail_out = static_cast<uInt>(len);
        bytes_out = 0;

        while (stream.avail_out > 0) {
            if (stream.avail_in == 0) {
                std::size_t n = ::fread(in_buffer, 1, sizeof(in_buffer), file);
                if (n == 0) {
                    if (std::ferror(file)) {
                        return false;
                    }
                    break;
                }
                stream.next_in = in_buffer;
                stream.avail_in = static_cast<uInt>(n);
            }
            int ret = inflate(&stream, Z_NO_FLUSH);

            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                return false;
            }
        }

        bytes_out = len - stream.avail_out;
        return true;
    }

    // Method for indexer that doesn't read new input (input already loaded)
    bool read_and_count_lines_with_blocks_no_input(std::size_t &bytes_read,
                                                   std::uint64_t &lines_found) {
        stream.next_out = buffer;
        stream.avail_out = static_cast<uInt>(sizeof(buffer));
        bytes_read = 0;
        lines_found = 0;

        while (stream.avail_out > 0 && stream.avail_in > 0) {
            int ret = inflate(&stream, Z_BLOCK);  // Use Z_BLOCK for boundary detection

            if (ret == Z_STREAM_END) {
                break;
            }
            if (ret != Z_OK) {
                DFTRACER_UTILS_LOG_DEBUG(
                    "inflate() failed with error: %d (%s)", ret,
                    stream.msg ? stream.msg : "no message");
                return false;
            }
            
            // Check if we're at a proper block boundary (not last deflate block)
            if ((stream.data_type & 0xc0) == 0x80) {
                // At proper block boundary for checkpoint creation
                break;
            }
        }

        bytes_read = sizeof(buffer) - stream.avail_out;
        
        // Count lines in decompressed data
        for (std::size_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                lines_found++;
            }
        }
        
        return true;
    }

    bool skip(FILE *file, std::size_t bytes_to_skip, unsigned char *buf,
              std::size_t buf_size) {
        DFTRACER_UTILS_LOG_DEBUG("Inflater::skip - bytes_to_skip=%zu", bytes_to_skip);
        if (bytes_to_skip == 0) return true;
        size_t remaining_skip = bytes_to_skip;
        size_t total_skipped = 0;
        while (remaining_skip > 0) {
            size_t to_skip = std::min(remaining_skip, buf_size);
            size_t skipped;
            bool result = read_continuous(file, buf, to_skip, skipped);
            if (!result || skipped == 0) {
                DFTRACER_UTILS_LOG_DEBUG("Inflater::skip - failed at total_skipped=%zu, remaining=%zu, result=%d, skipped=%zu", 
                                         total_skipped, remaining_skip, result, skipped);
                break;
            }
            remaining_skip -= skipped;
            total_skipped += skipped;
        }
        DFTRACER_UTILS_LOG_DEBUG("Inflater::skip - completed: total_skipped=%zu, success=%s", 
                                 total_skipped, remaining_skip == 0 ? "true" : "false");
        return remaining_skip == 0;
    }

    bool skip(FILE *file, std::size_t bytes_to_skip) {
        alignas(64) unsigned char skip_buffer[BUFFER_SIZE];
        return skip(file, bytes_to_skip, skip_buffer, sizeof(skip_buffer));
    }

   private:
    // in_buffer moved to public section for indexer access
};
}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_COMMON_INFLATER_H
