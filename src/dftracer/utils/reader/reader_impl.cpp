#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/reader_impl.h>

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

std::string ReaderImplementor::extract_lines_from_chunk(
    const std::string& chunk_data, 
    size_t target_start_line, 
    size_t target_end_line, 
    size_t chunk_first_line) {
    
    std::string result;
    size_t current_line = chunk_first_line;
    size_t line_start = 0;
    
    for (size_t i = 0; i <= chunk_data.size(); i++) {
        // Check if we've reached end of line or end of data
        if (i == chunk_data.size() || chunk_data[i] == '\n') {
            // Include the newline character if present
            size_t line_end = (i == chunk_data.size()) ? i : i + 1;
            
            if (current_line >= target_start_line && current_line <= target_end_line) {
                result += chunk_data.substr(line_start, line_end - line_start);
            }
            
            current_line++;
            line_start = line_end;
            
            // Early termination if we've passed our target range
            if (current_line > target_end_line) {
                break;
            }
        }
    }
    
    return result;
}

std::string ReaderImplementor::read_lines_optimized(std::size_t start_line,
                                                    std::size_t end_line) {
    check_reader_state(is_open, indexer);
    
    auto checkpoints = indexer->get_checkpoints_for_line_range(start_line, end_line);
    
    if (checkpoints.empty()) {
        DFTRACER_UTILS_LOG_DEBUG("No checkpoints found for line range [%zu, %zu]", start_line, end_line);
        return "";
    }
    
    std::string result;
    
    for (const auto& checkpoint : checkpoints) {
        // Calculate which lines we need from this checkpoint
        size_t chunk_start_line = std::max(start_line, static_cast<std::size_t>(checkpoint.first_line_num));
        size_t chunk_end_line = std::min(end_line, static_cast<std::size_t>(checkpoint.last_line_num));
        
        DFTRACER_UTILS_LOG_DEBUG(
            "Processing checkpoint %zu: lines [%zu, %zu], extracting lines [%zu, %zu]",
            checkpoint.checkpoint_idx, checkpoint.first_line_num, checkpoint.last_line_num,
            chunk_start_line, chunk_end_line);
        
        // Read the entire checkpoint's byte range
        size_t bytes_to_read = checkpoint.uc_size > 0 ? checkpoint.uc_size : (1024 * 1024); // Default to 1MB if uc_size not set
        std::vector<char> chunk_buffer(bytes_to_read);
        
        size_t bytes_read = read_line_bytes(checkpoint.uc_offset, 
                                           checkpoint.uc_offset + bytes_to_read,
                                           chunk_buffer.data(), 
                                           bytes_to_read);
        
        if (bytes_read > 0) {
            std::string chunk_data(chunk_buffer.data(), bytes_read);
            
            // Extract only the lines we need from this chunk
            std::string chunk_lines = extract_lines_from_chunk(
                chunk_data, chunk_start_line, chunk_end_line, static_cast<std::size_t>(checkpoint.first_line_num));
            
            result += chunk_lines;
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
        DFTRACER_UTILS_LOG_DEBUG(
            "ReaderImplementor::read - stream is finished", "");
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

json::JsonDocuments ReaderImplementor::read_json_lines_bytes(
    std::size_t start_bytes, std::size_t end_bytes) {
    check_reader_state(is_open, indexer);

    auto buffer = std::make_unique<char[]>(default_buffer_size);

    validate_parameters(buffer.get(), default_buffer_size, start_bytes,
                        end_bytes, indexer->get_max_bytes());

    std::string buffer_content;
    size_t bytes_read = 0;
    size_t total_bytes = 0;
    while ((bytes_read = read_line_bytes(start_bytes, end_bytes, buffer.get(),
                                         default_buffer_size)) > 0) {
        buffer_content.append(buffer.get(), bytes_read);
        total_bytes += bytes_read;
    }

    if (total_bytes == 0) {
        return {};
    }

    return json::parse_json_lines(buffer_content.data(), buffer_content.size());
}

json::OwnedJsonDocuments ReaderImplementor::read_json_lines_bytes_owned(
    std::size_t start_bytes, std::size_t end_bytes) {
    check_reader_state(is_open, indexer);

    auto buffer = std::make_unique<char[]>(default_buffer_size);

    validate_parameters(buffer.get(), default_buffer_size, start_bytes,
                        end_bytes, indexer->get_max_bytes());

    std::string buffer_content;
    size_t bytes_read = 0;
    size_t total_bytes = 0;
    while ((bytes_read = read_line_bytes(start_bytes, end_bytes, buffer.get(),
                                         default_buffer_size)) > 0) {
        buffer_content.append(buffer.get(), bytes_read);
        total_bytes += bytes_read;
    }

    if (total_bytes == 0) {
        return {};
    }

    return json::parse_json_lines_owned(buffer_content.data(),
                                        buffer_content.size());
}
}  // namespace dftracer::utils
