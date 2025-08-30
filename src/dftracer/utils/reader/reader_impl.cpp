#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/reader_impl.h>
#include <dftracer/utils/utils/timer.h>

#include <cstring>
#include <string_view>

static void validate_parameters(const char *buffer, size_t buffer_size,
                                size_t start_bytes, size_t end_bytes,
                                size_t max_bytes = SIZE_MAX) {
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

std::size_t ReaderImplementor::extract_lines_from_chunk(
    std::string_view chunk_data, std::size_t target_start_line,
    std::size_t target_end_line, std::size_t chunk_first_line,
    std::size_t &start_offset) {
    const char *data = chunk_data.data();
    std::size_t data_size = chunk_data.size();

    if (target_start_line > target_end_line || chunk_data.empty()) {
        start_offset = 0;
        return 0;
    }

    std::size_t current_line = chunk_first_line;
    std::size_t pos = 0;
    start_offset = 0;
    std::size_t end_pos = 0;
    bool found_start = false;

    // Single pass to find both start and end positions
    while (pos < data_size && current_line <= target_end_line) {
        const char *newline_ptr = static_cast<const char *>(
            std::memchr(data + pos, '\n', data_size - pos));

        if (current_line == target_start_line && !found_start) {
            start_offset = pos;
            found_start = true;
        }

        if (newline_ptr != nullptr) {
            std::size_t newline_pos = newline_ptr - data;
            if (current_line == target_end_line) {
                end_pos = newline_pos + 1;
                break;
            }
            pos = newline_pos + 1;
            current_line++;
        } else {
            // Handle final line without newline
            if (current_line == target_end_line) {
                end_pos = data_size;
            }
            break;
        }
    }

    if (!found_start || end_pos <= start_offset) {
        start_offset = 0;
        return 0;
    }

    return end_pos - start_offset;
}

std::string ReaderImplementor::read_lines_optimized(std::size_t start_line,
                                                    std::size_t end_line) {
    check_reader_state(is_open, indexer);

    std::vector<char> read_buffer;

    std::vector<IndexCheckpoint> checkpoints;

    {
        Timer timer("Finding checkpoints within line range", true, true);
        checkpoints =
            indexer->get_checkpoints_for_line_range(start_line, end_line);
    }

    std::string result;
    {
        Timer timer("Preparing string result", true, true);
        // Calculate more accurate size estimate using checkpoint metadata
        std::size_t estimated_bytes = 0;
        if (!checkpoints.empty()) {
            for (const auto &checkpoint : checkpoints) {
                estimated_bytes += checkpoint.uc_size;
            }
            // Reserve space for the entire result upfront
            result.reserve(estimated_bytes);
        } else {
            // Fallback: estimate based on line count with conservative average
            result.reserve((end_line - start_line + 1) * 100);
        }
    }

    if (checkpoints.empty()) {
        DFTRACER_UTILS_LOG_INFO(
            "No checkpoints found for line range [%zu, %zu], falling back to "
            "reading from beginning",
            start_line, end_line);

        // Fallback: read from the beginning of the file for early lines
        std::size_t max_bytes = indexer->get_max_bytes();
        line_byte_stream =
            stream_factory->create_line_stream(gz_path, 0, max_bytes);

        std::size_t current_line = 1;
        const std::size_t buffer_size = default_buffer_size;
        std::vector<char> buffer(buffer_size);

        static constexpr std::size_t typical_line_length = 16 * 1024;  // 16 KB
        std::string current_line_content;
        current_line_content.reserve(
            typical_line_length);  // Reserve for typical line length

        while (!line_byte_stream->is_finished() && current_line <= end_line) {
            std::size_t bytes_read =
                line_byte_stream->stream(buffer.data(), buffer_size);
            if (bytes_read == 0) break;

            // Use faster memchr-based newline detection
            const char *data = buffer.data();
            std::size_t pos = 0;

            while (pos < bytes_read && current_line <= end_line) {
                // Use memchr for faster newline detection
                const char *newline_ptr = static_cast<const char *>(
                    std::memchr(data + pos, '\n', bytes_read - pos));

                if (newline_ptr != nullptr) {
                    std::size_t newline_pos = newline_ptr - data;
                    // Found complete line
                    if (current_line >= start_line) {
                        // Append previous partial line + current segment
                        // directly to result
                        if (!current_line_content.empty()) {
                            result.append(current_line_content);
                            current_line_content.clear();
                        }
                        result.append(data + pos, newline_pos - pos + 1);
                    } else {
                        current_line_content.clear();
                    }

                    current_line++;
                    pos = newline_pos + 1;
                } else {
                    // Store partial line
                    current_line_content.append(data + pos, bytes_read - pos);
                    break;
                }
            }
        }

        // case where last line doesn't end with newline
        if (!current_line_content.empty() && current_line >= start_line &&
            current_line <= end_line) {
            result.append(current_line_content);
        }

        return result;
    }

    std::uint64_t total_start_offset = 0;
    std::uint64_t total_end_offset = 0;
    std::uint64_t first_line_in_data = 1;

    if (checkpoints[0].checkpoint_idx == 0) {
        // first checkpoint: read from beginning
        total_start_offset = 0;
        first_line_in_data = 1;
    } else {
        // Find the previous checkpoint for the starting position
        Timer timer("Finding previous checkpoint for start position", true,
                    true);
        auto all_checkpoints = indexer->get_checkpoints();
        for (const auto &prev_ckpt : all_checkpoints) {
            if (prev_ckpt.checkpoint_idx == checkpoints[0].checkpoint_idx - 1) {
                total_start_offset = prev_ckpt.uc_offset;
                first_line_in_data = prev_ckpt.last_line_num + 1;
                break;
            }
        }
    }

    // End offset is the end of the last checkpoint
    const auto &last_checkpoint = checkpoints.back();
    total_end_offset = last_checkpoint.uc_offset + last_checkpoint.uc_size;

    // Single large read operation
    std::size_t total_bytes =
        static_cast<std::size_t>(total_end_offset - total_start_offset);
    read_buffer.resize(total_bytes);

    std::size_t bytes_read = 0;
    {
        Timer timer("Reading data", true, true);
        bytes_read = read(total_start_offset, total_end_offset,
                          read_buffer.data(), total_bytes);
    }

    if (bytes_read > 0) {
        Timer timer("Extracting lines", true, true);
        std::string_view all_data(read_buffer.data(), bytes_read);

        // Single line extraction pass
        std::size_t chunk_start_offset;
        std::size_t size = extract_lines_from_chunk(
            all_data, start_line, end_line,
            static_cast<std::size_t>(first_line_in_data), chunk_start_offset);

        if (size > 0) {
            result.append(all_data.data() + chunk_start_offset, size);
        }
    }

    return result;
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

    return read_lines_optimized(start_line, end_line);
}
}  // namespace dftracer::utils
