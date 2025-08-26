#ifndef DFTRACER_UTILS_COMMON_INFLATER_H
#define DFTRACER_UTILS_COMMON_INFLATER_H

#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/logging.h>
#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

class Inflater {
   public:
    static constexpr std::size_t BUFFER_SIZE = 65536;
    int bits;
    std::uint64_t c_off;
    z_stream stream;
    alignas(64) unsigned char buffer[BUFFER_SIZE];

   public:
    Inflater()
        : bits(constants::indexer::ZLIB_GZIP_WINDOW_BITS),
          c_off(0) {
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

    bool read(FILE *file, unsigned char *buf, std::size_t len, std::size_t &bytes_out) {
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

    // Variant for reader streams - uses Z_NO_FLUSH without early breaking
    bool read_continuous(FILE *file, unsigned char *buf, std::size_t len, std::size_t &bytes_out) {
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

    bool skip(FILE *file, std::size_t bytes_to_skip, unsigned char *buf,
              std::size_t buf_size) {
        if (bytes_to_skip == 0) return true;
        size_t remaining_skip = bytes_to_skip;
        while (remaining_skip > 0) {
            size_t to_skip = std::min(remaining_skip, buf_size);
            size_t skipped;
            bool result = read_continuous(file, buf, to_skip, skipped);
            if (!result || skipped == 0) {
                break;
            }
            remaining_skip -= skipped;
        }
        return remaining_skip == 0;
    }

    bool skip(FILE *file, std::size_t bytes_to_skip) {
        alignas(64) unsigned char skip_buffer[BUFFER_SIZE];
        return skip(file, bytes_to_skip, skip_buffer, sizeof(skip_buffer));
    }

   private:
    alignas(64) unsigned char in_buffer[BUFFER_SIZE];
};

#endif  // DFTRACER_UTILS_COMMON_INFLATER_H
