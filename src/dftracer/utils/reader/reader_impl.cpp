#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/reader_impl.h>
#include <dftracer/utils/reader/string_line_processor.h>
#include <dftracer/utils/utils/timer.h>

#include <cstdio>
#include <cstring>
#include <limits>
#include <string_view>

static void validate_parameters(
    const char *buffer, std::size_t buffer_size, std::size_t start_bytes,
    std::size_t end_bytes,
    std::size_t max_bytes = std::numeric_limits<std::size_t>::max()) {
    if (!buffer || buffer_size == 0) {
        throw ReaderError(ReaderError::INVALID_ARGUMENT,
                          "Invalid buffer parameters");
    }
    if (start_bytes >= end_bytes) {
        throw ReaderError(ReaderError::INVALID_ARGUMENT,
                          "start_bytes must be less than end_bytes");
    }
    if (max_bytes != SIZE_MAX) {
        if (end_bytes > max_bytes) {
            throw ReaderError(ReaderError::INVALID_ARGUMENT,
                              "end_bytes exceeds maximum available bytes");
        }
        if (start_bytes > max_bytes) {
            throw ReaderError(ReaderError::INVALID_ARGUMENT,
                              "start_bytes exceeds maximum available bytes");
        }
    }
}

static void check_reader_state(bool is_open, const void *indexer) {
    if (!is_open || !indexer) {
        throw std::runtime_error("Reader is not open");
    }
}

static constexpr std::size_t DEFAULT_READER_BUFFER_SIZE = 1 * 1024 * 1024;

namespace dftracer::utils {

ReaderImplementor::ReaderImplementor(const std::string &gz_path_,
                                     const std::string &idx_path_,
                                     std::size_t index_ckpt_size)
    : gz_path(gz_path_),
      idx_path(idx_path_),
      is_open(false),
      default_buffer_size(DEFAULT_READER_BUFFER_SIZE) {
    try {
        indexer = new Indexer(gz_path, idx_path, index_ckpt_size);
        is_open = true;
        is_indexer_initialized_internally = true;

        stream_factory = std::make_unique<StreamFactory>(*indexer);

        DFTRACER_UTILS_LOG_DEBUG(
            "Successfully created DFT reader for gz: %s and index: %s",
            gz_path.c_str(), idx_path.c_str());
    } catch (const std::exception &e) {
        throw ReaderError(ReaderError::INITIALIZATION_ERROR,
                          "Failed to initialize reader with indexer: " +
                              std::string(e.what()));
    }
}

ReaderImplementor::ReaderImplementor(Indexer *indexer_)
    : default_buffer_size(DEFAULT_READER_BUFFER_SIZE),
      indexer(indexer_),
      is_indexer_initialized_internally(false) {
    if (indexer == nullptr) {
        throw ReaderError(ReaderError::INITIALIZATION_ERROR,
                          "Invalid indexer provided");
    }
    stream_factory = std::make_unique<StreamFactory>(*indexer);
    is_open = true;
    gz_path = indexer_->get_gz_path();
    idx_path = indexer_->get_idx_path();
}

ReaderImplementor::~ReaderImplementor() {
    if (is_indexer_initialized_internally) {
        delete indexer;
    }
}

std::size_t ReaderImplementor::get_max_bytes() const {
    check_reader_state(is_open, indexer);
    std::size_t max_bytes = static_cast<std::size_t>(indexer->get_max_bytes());
    DFTRACER_UTILS_LOG_DEBUG("Maximum bytes available: %zu", max_bytes);
    return max_bytes;
}

std::size_t ReaderImplementor::get_num_lines() const {
    check_reader_state(is_open, indexer);
    std::size_t num_lines = static_cast<std::size_t>(indexer->get_num_lines());
    DFTRACER_UTILS_LOG_DEBUG("Total lines available: %zu", num_lines);
    return num_lines;
}

void ReaderImplementor::reset() {
    check_reader_state(is_open, indexer);
    if (line_byte_stream) {
        line_byte_stream->reset();
    }
    if (byte_stream) {
        byte_stream->reset();
    }
}

std::size_t ReaderImplementor::read(std::size_t start_bytes,
                                    std::size_t end_bytes, char *buffer,
                                    std::size_t buffer_size) {
    check_reader_state(is_open, indexer);
    validate_parameters(buffer, buffer_size, start_bytes, end_bytes,
                        indexer->get_max_bytes());

    DFTRACER_UTILS_LOG_DEBUG(
        "ReaderImplementor::read - request: start_bytes=%zu, end_bytes=%zu, "
        "buffer_size=%zu",
        start_bytes, end_bytes, buffer_size);

    if (stream_factory->needs_new_byte_stream(byte_stream.get(), gz_path,
                                              start_bytes, end_bytes)) {
        DFTRACER_UTILS_LOG_DEBUG(
            "ReaderImplementor::read - creating new byte stream", "");
        byte_stream =
            stream_factory->create_byte_stream(gz_path, start_bytes, end_bytes);
    } else {
        DFTRACER_UTILS_LOG_DEBUG(
            "ReaderImplementor::read - reusing existing byte stream", "");
    }

    if (byte_stream->is_finished()) {
        DFTRACER_UTILS_LOG_DEBUG("ReaderImplementor::read - stream is finished",
                                 "");
        return 0;
    }

    std::size_t result = byte_stream->stream(buffer, buffer_size);
    DFTRACER_UTILS_LOG_DEBUG("ReaderImplementor::read - returned %zu bytes",
                             result);
    return result;
}

std::size_t ReaderImplementor::read_line_bytes(std::size_t start_bytes,
                                               std::size_t end_bytes,
                                               char *buffer,
                                               std::size_t buffer_size) {
    check_reader_state(is_open, indexer);

    if (end_bytes > indexer->get_max_bytes()) {
        end_bytes = indexer->get_max_bytes();
    }

    validate_parameters(buffer, buffer_size, start_bytes, end_bytes,
                        indexer->get_max_bytes());

    if (stream_factory->needs_new_line_stream(line_byte_stream.get(), gz_path,
                                              start_bytes, end_bytes)) {
        line_byte_stream =
            stream_factory->create_line_stream(gz_path, start_bytes, end_bytes);
    }

    if (line_byte_stream->is_finished()) {
        return 0;
    }

    return line_byte_stream->stream(buffer, buffer_size);
}

std::string ReaderImplementor::read_lines(size_t start_line, size_t end_line) {
    check_reader_state(is_open, indexer);

    if (start_line == 0 || end_line == 0) {
        throw std::runtime_error("Line numbers must be 1-based (start from 1)");
    }

    if (start_line > end_line) {
        throw std::runtime_error("Start line must be <= end line");
    }

    size_t total_lines = indexer->get_num_lines();
    if (start_line > total_lines || end_line > total_lines) {
        throw std::runtime_error("Line numbers exceed total lines in file (" +
                                 std::to_string(total_lines) + ")");
    }

    std::string result;
    StringLineProcessor processor(result);
    read_lines_with_processor(start_line, end_line, processor);
    return result;
}

void ReaderImplementor::read_lines_with_processor(std::size_t start_line,
                                                  std::size_t end_line,
                                                  LineProcessor &processor) {
    check_reader_state(is_open, indexer);

    if (start_line == 0 || end_line == 0) {
        throw std::runtime_error("Line numbers must be 1-based (start from 1)");
    }

    if (start_line > end_line) {
        throw std::runtime_error("Start line must be <= end line");
    }

    std::size_t total_lines = indexer->get_num_lines();
    if (start_line > total_lines || end_line > total_lines) {
        throw std::runtime_error("Line numbers exceed total lines in file (" +
                                 std::to_string(total_lines) + ")");
    }

    processor.begin(start_line, end_line);

    std::vector<char> process_buffer(default_buffer_size);
    std::size_t buffer_usage = 0;

    std::vector<IndexCheckpoint> checkpoints =
        indexer->get_checkpoints_for_line_range(start_line, end_line);

    if (checkpoints.empty()) {
        std::size_t max_bytes = indexer->get_max_bytes();
        line_byte_stream =
            stream_factory->create_line_stream(gz_path, 0, max_bytes);

        std::size_t current_line = 1;
        std::string line_accumulator;

        while (!line_byte_stream->is_finished() && current_line <= end_line) {
            std::size_t bytes_read =
                line_byte_stream->stream(process_buffer.data() + buffer_usage,
                                         default_buffer_size - buffer_usage);
            if (bytes_read == 0) break;

            buffer_usage += bytes_read;

            process_lines(process_buffer.data(), buffer_usage, current_line,
                          start_line, end_line, line_accumulator, processor);

            buffer_usage = 0;
        }
    } else {
        std::uint64_t total_start_offset = 0;
        std::uint64_t total_end_offset = 0;
        std::uint64_t first_line_in_data = 1;

        if (checkpoints[0].checkpoint_idx == 0) {
            total_start_offset = 0;
            first_line_in_data = 1;
        } else {
            auto all_checkpoints = indexer->get_checkpoints();
            for (const auto &prev_ckpt : all_checkpoints) {
                if (prev_ckpt.checkpoint_idx ==
                    checkpoints[0].checkpoint_idx - 1) {
                    total_start_offset = prev_ckpt.uc_offset;
                    first_line_in_data = prev_ckpt.last_line_num + 1;
                    break;
                }
            }
        }

        const auto &last_checkpoint = checkpoints.back();
        total_end_offset = last_checkpoint.uc_offset + last_checkpoint.uc_size;

        std::size_t total_bytes =
            static_cast<std::size_t>(total_end_offset - total_start_offset);
        std::vector<char> read_buffer(total_bytes);

        std::size_t bytes_read = read(total_start_offset, total_end_offset,
                                      read_buffer.data(), total_bytes);

        if (bytes_read > 0) {
            std::string line_accumulator;
            std::size_t current_line = first_line_in_data;

            process_lines(read_buffer.data(), bytes_read, current_line,
                          start_line, end_line, line_accumulator, processor);
        }
    }

    processor.end();
}

void ReaderImplementor::read_line_bytes_with_processor(
    std::size_t start_bytes, std::size_t end_bytes, LineProcessor &processor) {
    check_reader_state(is_open, indexer);

    if (end_bytes > indexer->get_max_bytes()) {
        end_bytes = indexer->get_max_bytes();
    }

    if (start_bytes >= end_bytes) {
        return;
    }

    processor.begin(start_bytes, end_bytes);

    std::vector<char> process_buffer(default_buffer_size);
    std::size_t buffer_usage = 0;
    std::string line_accumulator;

    if (stream_factory->needs_new_line_stream(line_byte_stream.get(), gz_path,
                                              start_bytes, end_bytes)) {
        line_byte_stream =
            stream_factory->create_line_stream(gz_path, start_bytes, end_bytes);
    }

    while (!line_byte_stream->is_finished()) {
        std::size_t bytes_read =
            line_byte_stream->stream(process_buffer.data() + buffer_usage,
                                     default_buffer_size - buffer_usage);
        if (bytes_read == 0) break;

        buffer_usage += bytes_read;

        // Reused the optimized process_lines function
        // dummy current line and start line should always be 1 here since
        // we are processing the entire buffer as a single logical line
        std::size_t dummy_current_line = 1;
        static constexpr std::size_t dummy_start_line = 1;
        static constexpr std::size_t dummy_end_line =
            std::numeric_limits<std::size_t>::max();

        std::size_t processed = process_lines(
            process_buffer.data(), buffer_usage, dummy_current_line,
            dummy_start_line, dummy_end_line, line_accumulator, processor);

        // Move unprocessed data to beginning of buffer
        if (processed < buffer_usage) {
            std::memmove(process_buffer.data(),
                         process_buffer.data() + processed,
                         buffer_usage - processed);
            buffer_usage -= processed;
        } else {
            buffer_usage = 0;
        }
    }

    // Process any remaining data in line_accumulator
    if (!line_accumulator.empty()) {
        processor.process(line_accumulator.c_str(), line_accumulator.length());
    }

    processor.end();
}

std::size_t ReaderImplementor::process_lines(
    const char *buffer_data, std::size_t buffer_size, std::size_t &current_line,
    std::size_t start_line, std::size_t end_line, std::string &line_accumulator,
    LineProcessor &processor) {
    std::size_t pos = 0;

    while (pos < buffer_size && current_line <= end_line) {
        const char *newline_ptr = static_cast<const char *>(
            std::memchr(buffer_data + pos, '\n', buffer_size - pos));

        if (newline_ptr != nullptr) {
            std::size_t newline_pos = newline_ptr - buffer_data;

            if (current_line >= start_line) {
                if (!line_accumulator.empty()) {
                    line_accumulator.append(buffer_data + pos,
                                            newline_pos - pos);
                    if (!processor.process(line_accumulator.c_str(),
                                           line_accumulator.length())) {
                        return pos;
                    }
                    line_accumulator.clear();
                } else {
                    if (!processor.process(buffer_data + pos,
                                           newline_pos - pos)) {
                        return pos;
                    }
                }
            } else {
                line_accumulator.clear();
            }

            current_line++;
            pos = newline_pos + 1;
        } else {
            line_accumulator.append(buffer_data + pos, buffer_size - pos);
            break;
        }
    }

    return pos;
}

}  // namespace dftracer::utils
