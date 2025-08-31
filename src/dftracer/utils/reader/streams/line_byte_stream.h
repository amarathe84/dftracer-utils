#ifndef DFTRACER_UTILS_READER_STREAMS_LINE_BYTE_STREAM_H
#define DFTRACER_UTILS_READER_STREAMS_LINE_BYTE_STREAM_H

#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/common/platform_compat.h>
#include <dftracer/utils/reader/streams/stream.h>

#include <cstdint>
#include <cstring>
#include <vector>

class LineByteStream : public Stream {
   private:
    static constexpr std::size_t SEARCH_BUFFER_SIZE = 2048;
    static constexpr std::size_t LINE_SEARCH_LOOKBACK = 512;

    std::vector<char> partial_line_buffer_;
    std::vector<char> temp_buffer_;  // Reusable temp buffer
    std::size_t actual_start_bytes_;

   public:
    LineByteStream() : Stream(), actual_start_bytes_(0) {
        partial_line_buffer_.reserve(1 * 1024 * 1024);
        temp_buffer_.reserve(1 * 1024 * 1024);
    }

    virtual void initialize(const std::string &gz_path, std::size_t start_bytes,
                            std::size_t end_bytes, Indexer &indexer) override {
        Stream::initialize(gz_path, start_bytes, end_bytes, indexer);
        actual_start_bytes_ = find_line_start(start_bytes);
        current_position_ = actual_start_bytes_;
    }

    std::size_t find_line_start(std::size_t target_start) {
        std::size_t current_pos = checkpoint_.uc_offset;
        std::size_t actual_start = target_start;

        if (target_start <= current_pos) {
            return target_start;
        }

        std::size_t search_start = (target_start >= LINE_SEARCH_LOOKBACK)
                                       ? target_start - LINE_SEARCH_LOOKBACK
                                       : current_pos;

        if (search_start > current_pos) {
            skip(search_start);
            current_pos = search_start;
        }

        // Use stack allocation for small search buffer
        unsigned char search_buffer[SEARCH_BUFFER_SIZE];
        std::size_t search_bytes;
        if (inflater_.read(file_handle_, search_buffer,
                           sizeof(search_buffer) - 1, search_bytes)) {
            std::size_t relative_target = target_start - current_pos;
            if (relative_target < search_bytes) {
                for (int64_t i = static_cast<int64_t>(relative_target); i >= 0;
                     i--) {
                    if (i == 0 || search_buffer[i - 1] == '\n') {
                        actual_start =
                            current_pos + static_cast<std::size_t>(i);
                        DFTRACER_UTILS_LOG_DEBUG(
                            "Found JSON line start at position %zu (requested "
                            "%zu)",
                            actual_start, target_start);
                        break;
                    }
                }
            }
        }

        restart_compression();
        if (actual_start > checkpoint_.uc_offset) {
            skip(actual_start);
        }

        return actual_start;
    }

    std::size_t stream(char *buffer, std::size_t buffer_size) override {
// Prefetch buffer for writing
#ifdef __GNUC__
        __builtin_prefetch(buffer, 1, 3);
#endif

        if (!decompression_initialized_) {
            throw ReaderError(ReaderError::INITIALIZATION_ERROR,
                              "Streaming session not properly initialized");
        }

        if (is_at_target_end()) {
            is_finished_ = true;
            return 0;
        }

        // Prepare temp buffer
        ensure_temp_buffer_size(buffer_size);

        std::size_t available_buffer_space = buffer_size;
        if (!partial_line_buffer_.empty()) {
            if (partial_line_buffer_.size() > buffer_size) {
                throw ReaderError(
                    ReaderError::READ_ERROR,
                    "Partial line buffer exceeds available buffer space");
            }
            std::memcpy(temp_buffer_.data(), partial_line_buffer_.data(),
                        partial_line_buffer_.size());
            available_buffer_space -= partial_line_buffer_.size();
        }

        // Read data
        std::size_t max_bytes_to_read = target_end_bytes_ - current_position_;
        std::size_t bytes_to_read =
            std::min(max_bytes_to_read, available_buffer_space);

        std::size_t bytes_read = 0;
        if (bytes_to_read > 0) {
            bool status = inflater_.read(
                file_handle_,
                reinterpret_cast<unsigned char *>(temp_buffer_.data() +
                                                  partial_line_buffer_.size()),
                bytes_to_read, bytes_read);

            if (!status || bytes_read == 0) {
                is_finished_ = true;
                return 0;
            }
        }

        DFTRACER_UTILS_LOG_DEBUG(
            "Read %zu bytes from compressed stream, partial_buffer_size=%zu, "
            "current_position=%zu, target_end=%zu",
            bytes_read, partial_line_buffer_.size(), current_position_,
            target_end_bytes_);

        std::size_t total_data_size = partial_line_buffer_.size() + bytes_read;
        std::size_t adjusted_size = apply_range_and_boundary_limits(
            temp_buffer_.data(), total_data_size);

        current_position_ += bytes_read;

        if (adjusted_size == 0) {
            DFTRACER_UTILS_LOG_ERROR(
                "No complete line found, need to read more data, try "
                "increasing the "
                "end bytes",
                "");
            is_finished_ = true;
            return 0;
        }

        std::memcpy(buffer, temp_buffer_.data(), adjusted_size);

        update_partial_buffer(adjusted_size, total_data_size);

        return adjusted_size;
    }

    void reset() override {
        Stream::reset();
        partial_line_buffer_.clear();
        partial_line_buffer_.shrink_to_fit();
        temp_buffer_.clear();
        temp_buffer_.shrink_to_fit();
        actual_start_bytes_ = 0;
    }

   private:
    void ensure_temp_buffer_size(std::size_t required_size) {
        if (temp_buffer_.size() < required_size) {
            temp_buffer_.resize(required_size);
        }
    }

    void update_partial_buffer(std::size_t adjusted_size,
                               std::size_t total_data_size) {
        if (adjusted_size < total_data_size) {
            std::size_t remaining_size = total_data_size - adjusted_size;
            partial_line_buffer_.resize(remaining_size);
            std::memcpy(partial_line_buffer_.data(),
                        temp_buffer_.data() + adjusted_size, remaining_size);
        } else {
            partial_line_buffer_.clear();
        }
    }

    std::size_t adjust_to_boundary(char *buffer, std::size_t buffer_size) {
        for (int64_t i = static_cast<int64_t>(buffer_size) - 1; i >= 0; i--) {
            if (buffer[i] == '\n') {
                return static_cast<std::size_t>(i) + 1;
            }
        }

        if (!is_finished_) {
            return 0;
        }
        return buffer_size;
    }

    std::size_t apply_range_and_boundary_limits(char *buffer,
                                                std::size_t total_data_size) {
        std::size_t adjusted_size;
        std::size_t original_range_size = target_end_bytes_ - start_bytes_;

        if (current_position_ < actual_start_bytes_) {
            DFTRACER_UTILS_LOG_ERROR(
                "Invalid state: current_position_ %zu < "
                "actual_start_bytes_ %zu",
                current_position_, actual_start_bytes_);
            throw ReaderError(ReaderError::READ_ERROR,
                              "Invalid internal position state detected");
        }
        std::size_t bytes_already_returned =
            current_position_ - actual_start_bytes_;
        std::size_t max_allowed_return =
            (bytes_already_returned < original_range_size)
                ? (original_range_size - bytes_already_returned)
                : 0;

        std::size_t limited_data_size =
            std::min(total_data_size, max_allowed_return);
        adjusted_size = adjust_to_boundary(buffer, limited_data_size);
        return adjusted_size;
    }
};

#endif  // DFTRACER_UTILS_READER_STREAMS_LINE_BYTE_STREAM_H
