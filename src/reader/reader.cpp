#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/platform_compat.h>
#include <spdlog/spdlog.h>
#include <zlib.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

// Forward declarations
struct InflateState {
  z_stream zs;
  FILE *file;
  unsigned char in[16384];  // 16 KB
  int bits;
  size_t c_off;
};

namespace dftracer {
namespace utils {
namespace reader {

class CompressionManager {
 public:
  class InflationSession {
   public:
    explicit InflationSession(InflateState *state, bool owns = false)
        : state_(state), owns_state_(owns) {}

    ~InflationSession() {
      if (owns_state_ && state_) {
        inflateEnd(&state_->zs);
      }
    }

    int read(unsigned char *output, size_t output_size, size_t *bytes_read) {
      return inflate_read(state_, output, output_size, bytes_read);
    }

    void skip(size_t bytes_to_skip, unsigned char *skip_buffer,
              size_t skip_buffer_size) {
      if (bytes_to_skip == 0) return;

      size_t remaining_skip = bytes_to_skip;
      while (remaining_skip > 0) {
        size_t to_skip = (remaining_skip > skip_buffer_size) ? skip_buffer_size
                                                             : remaining_skip;
        size_t skipped;
        int result = read(skip_buffer, to_skip, &skipped);
        if (result != 0) {
          throw std::runtime_error("Failed during skip phase");
        }
        if (skipped == 0) {
          break;
        }
        remaining_skip -= skipped;
      }
    }

   private:
    InflateState *state_;
    bool owns_state_;

    InflationSession(const InflationSession &);             // deleted
    InflationSession &operator=(const InflationSession &);  // deleted
  };

  static int inflate_init(InflateState *state, FILE *f, size_t c_off,
                          int bits) {
    memset(state, 0, sizeof(*state));
    state->file = f;
    state->c_off = c_off;
    state->bits = bits;

    if (inflateInit2(&state->zs, 15 + 16) != Z_OK) {
      return -1;
    }

    if (fseeko(f, static_cast<off_t>(c_off), SEEK_SET) != 0) {
      spdlog::error("Failed to seek to compressed offset: {}", c_off);
      inflateEnd(&state->zs);
      return -1;
    }

    return 0;
  }

  static void inflate_cleanup(InflateState *state) { inflateEnd(&state->zs); }

  static int inflate_read(InflateState *state, unsigned char *out,
                          size_t out_size, size_t *bytes_read) {
    state->zs.next_out = out;
    state->zs.avail_out = static_cast<uInt>(out_size);
    *bytes_read = 0;

    while (state->zs.avail_out > 0) {
      if (state->zs.avail_in == 0) {
        size_t n = fread(state->in, 1, sizeof(state->in), state->file);
        if (n == 0) {
          if (ferror(state->file)) {
            spdlog::error("Error reading from file during inflate_read");
            return -1;
          }
          break;  // EOF
        }
        state->zs.next_in = state->in;
        state->zs.avail_in = static_cast<uInt>(n);
      }

      int ret = inflate(&state->zs, Z_NO_FLUSH);
      if (ret == Z_STREAM_END) {
        break;
      }
      if (ret != Z_OK) {
        spdlog::debug("inflate() failed with error: {} ({})", ret,
                      state->zs.msg ? state->zs.msg : "no message");
        return -1;
      }
    }

    *bytes_read = out_size - state->zs.avail_out;
    return 0;
  }
};

static void validate_read_parameters(const char *buffer, size_t buffer_size,
                                     size_t start_bytes, size_t end_bytes,
                                     size_t max_bytes = SIZE_MAX) {
  if (!buffer || buffer_size == 0) {
    throw Reader::Error(Reader::Error::INVALID_ARGUMENT,
                        "Invalid buffer parameters");
  }
  if (start_bytes >= end_bytes) {
    throw Reader::Error(Reader::Error::INVALID_ARGUMENT,
                        "start_bytes must be less than end_bytes");
  }
  if (max_bytes != SIZE_MAX && end_bytes > max_bytes) {
    throw Reader::Error(Reader::Error::INVALID_ARGUMENT,
                        "end_bytes exceeds maximum available bytes");
  }
  if (max_bytes != SIZE_MAX && start_bytes > max_bytes) {
    throw Reader::Error(Reader::Error::INVALID_ARGUMENT,
                        "start_bytes exceeds maximum available bytes");
  }
}

class BaseStreamingSession {
 protected:
  std::string current_gz_path_;
  size_t start_bytes_;
  size_t current_position_;
  size_t target_end_bytes_;
  bool is_active_;
  bool is_finished_;

  std::unique_ptr<InflateState> inflate_state_;
  std::unique_ptr<dftracer::utils::indexer::CheckpointInfo> checkpoint_;
  FILE *file_handle_;
  bool decompression_initialized_;

  static const size_t SKIP_BUFFER_SIZE = 65536;
  unsigned char skip_buffer_[SKIP_BUFFER_SIZE];

 public:
  BaseStreamingSession()
      : start_bytes_(0),
        current_position_(0),
        target_end_bytes_(0),
        is_active_(false),
        is_finished_(false),
        file_handle_(nullptr),
        decompression_initialized_(false) {}

  virtual ~BaseStreamingSession() { reset(); }

  bool matches(const std::string &gz_path, size_t start_bytes,
               size_t end_bytes) const {
    return current_gz_path_ == gz_path && start_bytes_ == start_bytes &&
           target_end_bytes_ == end_bytes;
  }

  bool is_finished() const { return is_finished_; }

  virtual void initialize(const std::string &gz_path, size_t start_bytes,
                          size_t end_bytes,
                          dftracer::utils::indexer::Indexer &indexer) = 0;
  virtual size_t stream_chunk(char *buffer, size_t buffer_size) = 0;

  virtual void reset() {
    current_gz_path_.clear();
    start_bytes_ = 0;
    current_position_ = 0;
    target_end_bytes_ = 0;
    is_active_ = false;
    is_finished_ = false;
    if (file_handle_) {
      fclose(file_handle_);
      file_handle_ = nullptr;
    }
    if (decompression_initialized_ && inflate_state_) {
      CompressionManager::inflate_cleanup(inflate_state_.get());
    }
    inflate_state_.reset();
    checkpoint_.reset();
    decompression_initialized_ = false;
  }

 protected:
  FILE *open_file(const std::string &path) {
    FILE *file = fopen(path.c_str(), "rb");
    if (!file) {
      throw Reader::Error(Reader::Error::FILE_IO_ERROR,
                          "Failed to open file: " + path);
    }
    return file;
  }

  void initialize_compression(const std::string &gz_path, size_t start_bytes,
                              dftracer::utils::indexer::Indexer &indexer) {
    file_handle_ = open_file(gz_path);
    inflate_state_.reset(new InflateState());
    bool use_checkpoint = false;

    // Try to find checkpoint
    checkpoint_.reset(new dftracer::utils::indexer::CheckpointInfo());

    // Use first checkpoint for positions within the first checkpoint range to
    // avoid optimized checkpoint issues The first checkpoint covers bytes 0 to
    // ~33MB, so use sequential decompression for this range
    bool should_use_first_checkpoint =
        start_bytes < 33554401;  // First checkpoint range

    if (should_use_first_checkpoint) {
      // Get the first checkpoint (at file beginning)
      if (indexer.find_checkpoint(0, *checkpoint_)) {
        if (inflate_init_from_checkpoint(inflate_state_.get(), file_handle_,
                                         checkpoint_.get()) == 0) {
          use_checkpoint = true;
          spdlog::debug(
              "Using first checkpoint at uncompressed offset {} for early "
              "target {}",
              checkpoint_->uc_offset, start_bytes);
        }
      }
    } else {
      // For later positions, find the optimal checkpoint
      if (indexer.find_checkpoint(start_bytes, *checkpoint_)) {
        if (inflate_init_from_checkpoint(inflate_state_.get(), file_handle_,
                                         checkpoint_.get()) == 0) {
          use_checkpoint = true;
          spdlog::debug(
              "Using checkpoint at uncompressed offset {} for target {}",
              checkpoint_->uc_offset, start_bytes);
        }
      }
    }

    // Fallback to sequential read
    if (!use_checkpoint) {
      checkpoint_.reset();
      if (CompressionManager::inflate_init(inflate_state_.get(), file_handle_,
                                           0, 0) != 0) {
        throw Reader::Error(Reader::Error::COMPRESSION_ERROR,
                            "Failed to initialize inflation");
      }
    }

    decompression_initialized_ = true;
  }

  void skip_to_position(size_t target_position) {
    size_t current_pos = checkpoint_ ? checkpoint_->uc_offset : 0;
    if (target_position > current_pos) {
      CompressionManager::InflationSession session(inflate_state_.get(), false);
      session.skip(target_position - current_pos, skip_buffer_,
                   SKIP_BUFFER_SIZE);
    }
  }

  int inflate_init_from_checkpoint(
      InflateState *state, FILE *f,
      const dftracer::utils::indexer::CheckpointInfo *checkpoint) const {
    memset(state, 0, sizeof(*state));
    state->file = f;
    state->c_off = checkpoint->c_offset;
    state->bits = checkpoint->bits;

    spdlog::debug("Checkpoint c_offset: {}, bits: {}", checkpoint->c_offset,
                  checkpoint->bits);

    // Position file exactly like zran: seek to point->in - (point->bits ? 1 :
    // 0)
    off_t seek_pos =
        static_cast<off_t>(checkpoint->c_offset) - (checkpoint->bits ? 1 : 0);
    if (fseeko(f, seek_pos, SEEK_SET) != 0) {
      spdlog::error("Failed to seek to checkpoint position: {}", seek_pos);
      return -1;
    }

    // If we have partial bits, read the extra byte (following zran approach)
    int ch = 0;
    if (checkpoint->bits != 0) {
      ch = fgetc(f);
      if (ch == EOF) {
        spdlog::error("Failed to read byte at checkpoint position");
        return -1;
      }
    }

    // Initialize raw deflate stream (not gzip wrapper) following zran approach
    if (inflateInit2(&state->zs, -15) != Z_OK) {
      return -1;
    }

    // Reset to raw mode (following zran: inflateReset2(&index->strm, RAW))
    state->zs.avail_in = 0;
    if (inflateReset2(&state->zs, -15) != Z_OK) {  // RAW mode
      inflateEnd(&state->zs);
      return -1;
    }

    // Prime with partial bits if needed (following zran: INFLATEPRIME)
    if (checkpoint->bits != 0) {
      int prime_value = ch >> (8 - checkpoint->bits);
      spdlog::debug("Applying inflatePrime with {} bits, value: {}",
                    checkpoint->bits, prime_value);
      if (inflatePrime(&state->zs, checkpoint->bits, prime_value) != Z_OK) {
        spdlog::error("inflatePrime failed with {} bits, value: {}",
                      checkpoint->bits, prime_value);
        inflateEnd(&state->zs);
        return -1;
      }
    }

    // Decompress the saved dictionary
    unsigned char window[dftracer::utils::indexer::ZLIB_WINDOW_SIZE];
    size_t window_size = dftracer::utils::indexer::ZLIB_WINDOW_SIZE;
    if (decompress_window(checkpoint->dict_compressed.data(),
                          checkpoint->dict_compressed.size(), window,
                          &window_size) != 0) {
      inflateEnd(&state->zs);
      return -1;
    }

    // Set dictionary (following zran: inflateSetDictionary)
    if (inflateSetDictionary(&state->zs, window,
                             static_cast<uInt>(window_size)) != Z_OK) {
      spdlog::error("inflateSetDictionary failed");
      inflateEnd(&state->zs);
      return -1;
    }

    // Prime the input buffer for subsequent reads
    size_t n = fread(state->in, 1, sizeof(state->in), state->file);
    if (n > 0) {
      state->zs.next_in = state->in;
      state->zs.avail_in = static_cast<uInt>(n);
    } else if (ferror(state->file)) {
      spdlog::error("Error reading from file during checkpoint initialization");
      return -1;
    }

    return 0;
  }

  int decompress_window(const unsigned char *compressed, size_t compressed_size,
                        unsigned char *window, size_t *window_size) const {
    z_stream zs;
    memset(&zs, 0, sizeof(zs));

    if (inflateInit(&zs) != Z_OK) {
      spdlog::error("Failed to initialize inflate for window decompression");
      return -1;
    }

    zs.next_in = const_cast<unsigned char *>(compressed);
    zs.avail_in = static_cast<uInt>(compressed_size);
    zs.next_out = window;
    zs.avail_out = static_cast<uInt>(*window_size);

    int ret = inflate(&zs, Z_FINISH);
    if (ret != Z_STREAM_END) {
      spdlog::error(
          "inflate failed during window decompression with error: {} ({})", ret,
          zs.msg ? zs.msg : "no message");
      inflateEnd(&zs);
      return -1;
    }

    *window_size = *window_size - zs.avail_out;
    inflateEnd(&zs);
    return 0;
  }
};

class LineByteStreamingSession : public BaseStreamingSession {
 private:
  std::vector<char> partial_line_buffer_;  // Partial line from previous read
  size_t actual_start_bytes_;

 public:
  LineByteStreamingSession() : BaseStreamingSession(), actual_start_bytes_(0) {}

  void initialize(const std::string &gz_path, size_t start_bytes,
                  size_t end_bytes,
                  dftracer::utils::indexer::Indexer &indexer) override {
    spdlog::debug(
        "Initializing JSON streaming session for range [{}, {}] from {}",
        start_bytes, end_bytes, gz_path);

    if (is_active_) {
      reset();
    }

    current_gz_path_ = gz_path;
    start_bytes_ = start_bytes;
    target_end_bytes_ = end_bytes;
    is_active_ = true;
    is_finished_ = false;

    initialize_compression(gz_path, start_bytes, indexer);

    // Find the actual start position (beginning of a complete line)
    actual_start_bytes_ = find_line_start(start_bytes);
    current_position_ = actual_start_bytes_;

    spdlog::debug(
        "JSON streaming session initialized: actual_start={}, target_end={}",
        actual_start_bytes_, end_bytes);
  }

  // size_t read_until_first_line

  size_t stream_chunk(char *buffer, size_t buffer_size) override {
    if (!decompression_initialized_) {
      throw Reader::Error(Reader::Error::INITIALIZATION_ERROR,
                          "Streaming session not properly initialized");
    }

    // Check if we've reached the target end
    if (current_position_ >= target_end_bytes_) {
      is_finished_ = true;
      spdlog::trace(
          "LineByteStreamingSession finished: current_position_={}, "
          "target_end_bytes_={}",
          current_position_, target_end_bytes_);
      return 0;
    }

    // if we have partial lines then add it to bytes_to_read
    // CRITICAL: Ensure partial buffer doesn't exceed available space
    std::vector<char> temp_buffer(buffer_size);
    size_t available_buffer_space = buffer_size;
    if (!partial_line_buffer_.empty()) {
      // SECURITY: Check for buffer overflow before copying
      if (partial_line_buffer_.size() > buffer_size) {
        throw Reader::Error(
            Reader::Error::READ_ERROR,
            "Partial line buffer exceeds available buffer space");
      }
      // prepend partial buffer
      std::memcpy(temp_buffer.data(), partial_line_buffer_.data(),
                  partial_line_buffer_.size());
      available_buffer_space -= partial_line_buffer_.size();
    }

    // Calculate how much we can read without exceeding the target
    size_t max_bytes_to_read = target_end_bytes_ - current_position_;
    size_t bytes_to_read = std::min(max_bytes_to_read, available_buffer_space);

    size_t bytes_read = 0;
    if (bytes_to_read > 0) {
      int result = CompressionManager::inflate_read(
          inflate_state_.get(),
          reinterpret_cast<unsigned char *>(temp_buffer.data() +
                                            partial_line_buffer_.size()),
          bytes_to_read, &bytes_read);

      if (result != 0 || bytes_read == 0) {
        is_finished_ = true;
        return 0;
      }
    }

    spdlog::trace(
        "Read {} bytes from compressed stream, partial_buffer_size={}, "
        "current_position={}, target_end={}",
        bytes_read, partial_line_buffer_.size(), current_position_,
        target_end_bytes_);

    // Total data in temp_buffer is partial_line_buffer + new bytes
    size_t total_data_size = partial_line_buffer_.size() + bytes_read;

    // Apply range limiting only for small partial reads, not for full file
    // reads
    size_t adjusted_size;
    size_t original_range_size = target_end_bytes_ - start_bytes_;

    // If the requested range is small (< 1MB), apply strict range limiting
    // For larger ranges (including full file reads), let it read naturally
    if (original_range_size < 1024 * 1024) {
      // Small range read - apply cumulative range limiting
      // SECURITY: Prevent integer underflow by validating positions
      if (current_position_ < actual_start_bytes_) {
        spdlog::error(
            "Invalid state: current_position_ {} < actual_start_bytes_ {}",
            current_position_, actual_start_bytes_);
        throw Reader::Error(Reader::Error::READ_ERROR,
                            "Invalid internal position state detected");
      }
      size_t bytes_already_returned = current_position_ - actual_start_bytes_;
      size_t max_allowed_return =
          (bytes_already_returned < original_range_size)
              ? (original_range_size - bytes_already_returned)
              : 0;

      size_t limited_data_size = std::min(total_data_size, max_allowed_return);
      adjusted_size = adjust_to_boundary(temp_buffer.data(), limited_data_size);
    } else {
      // Large range or full file read - no artificial limiting
      adjusted_size = adjust_to_boundary(temp_buffer.data(), total_data_size);
    }

    spdlog::trace(
        "After boundary adjustment: total_data_size={}, "
        "original_range_size={}, final_adjusted_size={}",
        total_data_size, original_range_size, adjusted_size);

    current_position_ += bytes_read;

    if (adjusted_size == 0) {
      // No complete line found, we need to read more data
      spdlog::error(
          "No complete line found, need to read more data, try increasing the "
          "end bytes");
      is_finished_ = true;
      return 0;
    }

    // copy the adjusted buffer to the output
    std::memcpy(buffer, temp_buffer.data(), adjusted_size);

    // Update partial buffer with remaining data
    if (adjusted_size < total_data_size) {
      size_t remaining_size = total_data_size - adjusted_size;
      partial_line_buffer_ = std::vector<char>(remaining_size);
      std::memcpy(partial_line_buffer_.data(),
                  temp_buffer.data() + adjusted_size, remaining_size);
    } else {
      // All data was used, clear partial buffer
      partial_line_buffer_.clear();
    }

    // Debug for large range reads
    if ((target_end_bytes_ - start_bytes_) > 40000) {
      spdlog::trace(
          "Large range read: returning {} bytes, current_pos={}, "
          "target_end={}, range_size={}",
          adjusted_size, current_position_, target_end_bytes_,
          target_end_bytes_ - start_bytes_);
    }

    return adjusted_size;
  }

  void reset() override {
    BaseStreamingSession::reset();
    partial_line_buffer_ = std::vector<char>();
    actual_start_bytes_ = 0;
  }

 private:
  size_t find_line_start(size_t target_start) {
    size_t current_pos = checkpoint_ ? checkpoint_->uc_offset : 0;
    size_t actual_start = target_start;

    if (target_start <= current_pos) {
      return target_start;
    }

    // Search for line start
    size_t search_start =
        (target_start >= 512) ? target_start - 512 : current_pos;

    if (search_start > current_pos) {
      skip_to_position(search_start);
      current_pos = search_start;
    }

    // Read data to find start of complete JSON line
    unsigned char search_buffer[2048];
    size_t search_bytes;
    if (CompressionManager::inflate_read(inflate_state_.get(), search_buffer,
                                         sizeof(search_buffer) - 1,
                                         &search_bytes) == 0) {
      size_t relative_target = target_start - current_pos;
      if (relative_target < search_bytes) {
        for (int64_t i = static_cast<int64_t>(relative_target); i >= 0; i--) {
          if (i == 0 || search_buffer[i - 1] == '\n') {
            actual_start = current_pos + static_cast<size_t>(i);
            spdlog::debug("Found JSON line start at position {} (requested {})",
                          actual_start, target_start);
            break;
          }
        }
      }
    }

    // Restart compression and skip to actual start
    restart_compression();
    if (actual_start > (checkpoint_ ? checkpoint_->uc_offset : 0)) {
      skip_to_position(actual_start);
    }

    return actual_start;
  }

  size_t adjust_to_boundary(char *buffer, size_t buffer_size) {
    // Find the last complete boundary in the buffer
    for (int64_t i = static_cast<int64_t>(buffer_size) - 1; i > 0; i--) {
      if (buffer[i] == '\n') {
        return static_cast<size_t>(i) + 1;
      }
    }
    // If no JSON boundary found and we're not finished, return 0 to buffer more
    // data
    if (!is_finished_) {
      return 0;
    }
    // Only return partial data if we're at the end of stream
    return buffer_size;
  }

  void restart_compression() {
    CompressionManager::inflate_cleanup(inflate_state_.get());

    bool use_checkpoint = (checkpoint_ != nullptr);
    if (use_checkpoint) {
      if (inflate_init_from_checkpoint(inflate_state_.get(), file_handle_,
                                       checkpoint_.get()) != 0) {
        throw Reader::Error(Reader::Error::COMPRESSION_ERROR,
                            "Failed to reinitialize from checkpoint");
      }
    } else {
      if (CompressionManager::inflate_init(inflate_state_.get(), file_handle_,
                                           0, 0) != 0) {
        throw Reader::Error(Reader::Error::COMPRESSION_ERROR,
                            "Failed to reinitialize inflation");
      }
    }
  }
};

class ByteStreamingSession : public BaseStreamingSession {
 public:
  ByteStreamingSession() : BaseStreamingSession() {}

  void initialize(const std::string &gz_path, size_t start_bytes,
                  size_t end_bytes,
                  dftracer::utils::indexer::Indexer &indexer) override {
    spdlog::debug(
        "Initializing raw streaming session for range [{}, {}] from {}",
        start_bytes, end_bytes, gz_path);

    if (is_active_) {
      reset();
    }

    current_gz_path_ = gz_path;
    start_bytes_ = start_bytes;
    target_end_bytes_ = end_bytes;
    current_position_ = start_bytes;
    is_active_ = true;
    is_finished_ = false;

    initialize_compression(gz_path, start_bytes, indexer);

    // Skip to the start position
    size_t current_pos = checkpoint_ ? checkpoint_->uc_offset : 0;
    if (start_bytes > current_pos) {
      skip_to_position(start_bytes);
    }

    spdlog::debug("Raw streaming session initialized: start={}, target_end={}",
                  start_bytes, end_bytes);
  }

  size_t stream_chunk(char *buffer, size_t buffer_size) override {
    if (!decompression_initialized_) {
      throw Reader::Error(Reader::Error::INITIALIZATION_ERROR,
                          "Raw streaming session not properly initialized");
    }

    // Check if we've reached the target end
    if (current_position_ >= target_end_bytes_) {
      is_finished_ = true;
      return 0;
    }

    // Calculate how much we can read without exceeding the target
    size_t max_read = target_end_bytes_ - current_position_;
    size_t read_size = std::min(buffer_size, max_read);

    size_t bytes_read;
    int result = CompressionManager::inflate_read(
        inflate_state_.get(), reinterpret_cast<unsigned char *>(buffer),
        read_size, &bytes_read);

    if (result != 0 || bytes_read == 0) {
      is_finished_ = true;
      return 0;
    }

    current_position_ += bytes_read;

    spdlog::debug("Raw streamed {} bytes (position: {} / {})", bytes_read,
                  current_position_, target_end_bytes_);

    return bytes_read;
  }
};

class StreamingSessionFactory {
 private:
  dftracer::utils::indexer::Indexer &indexer_;

 public:
  explicit StreamingSessionFactory(dftracer::utils::indexer::Indexer &indexer)
      : indexer_(indexer) {}

  std::unique_ptr<LineByteStreamingSession> create_line_session(
      const std::string &gz_path, size_t start_bytes, size_t end_bytes) {
    std::unique_ptr<LineByteStreamingSession> session(
        new LineByteStreamingSession());
    session->initialize(gz_path, start_bytes, end_bytes, indexer_);
    return session;
  }

  std::unique_ptr<ByteStreamingSession> create_raw_session(
      const std::string &gz_path, size_t start_bytes, size_t end_bytes) {
    std::unique_ptr<ByteStreamingSession> session(new ByteStreamingSession());
    session->initialize(gz_path, start_bytes, end_bytes, indexer_);
    return session;
  }

  bool needs_new_line_session(const LineByteStreamingSession *current,
                              const std::string &gz_path, size_t start_bytes,
                              size_t end_bytes) const {
    return !current || !current->matches(gz_path, start_bytes, end_bytes) ||
           current->is_finished();
  }

  bool needs_new_raw_session(const ByteStreamingSession *current,
                             const std::string &gz_path, size_t start_bytes,
                             size_t end_bytes) const {
    return !current || !current->matches(gz_path, start_bytes, end_bytes) ||
           current->is_finished();
  }
};

class Reader::Impl {
 public:
  Impl(const std::string &gz_path, const std::string &idx_path)
      : gz_path_(gz_path), idx_path_(idx_path), is_open_(false) {
    try {
      // Create indexer instance - will auto-build index if needed
      indexer_.reset(
          new dftracer::utils::indexer::Indexer(gz_path, idx_path, 1.0));
      if (indexer_->need_rebuild()) {
        indexer_->build();
      }
      is_open_ = true;

      // Initialize session factory using indexer directly
      session_factory_.reset(new StreamingSessionFactory(*indexer_));

      spdlog::debug("Successfully created DFT reader for gz: {} and index: {}",
                    gz_path, idx_path);
    } catch (const std::exception &e) {
      throw Reader::Error(
          Reader::Error::INITIALIZATION_ERROR,
          "Failed to initialize reader with indexer: " + std::string(e.what()));
    }
  }

  ~Impl() = default;

  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;

  Impl(Impl &&other) noexcept
      : gz_path_(std::move(other.gz_path_)),
        idx_path_(std::move(other.idx_path_)),
        is_open_(other.is_open_),
        indexer_(std::move(other.indexer_)),
        session_factory_(std::move(other.session_factory_)),
        line_byte_session_(std::move(other.line_byte_session_)),
        byte_session_(std::move(other.byte_session_)) {
    other.is_open_ = false;
  }

  Impl &operator=(Impl &&other) noexcept {
    if (this != &other) {
      gz_path_ = std::move(other.gz_path_);
      idx_path_ = std::move(other.idx_path_);
      is_open_ = other.is_open_;
      indexer_ = std::move(other.indexer_);
      session_factory_ = std::move(other.session_factory_);
      line_byte_session_ = std::move(other.line_byte_session_);
      byte_session_ = std::move(other.byte_session_);
      other.is_open_ = false;
    }
    return *this;
  }

  size_t get_max_bytes() const {
    if (!is_open_ || !indexer_) {
      throw std::runtime_error("Reader is not open");
    }

    size_t max_bytes = static_cast<size_t>(indexer_->get_max_bytes());

    if (max_bytes > 0) {
      spdlog::debug("Maximum bytes available: {}", max_bytes);
    } else {
      spdlog::debug("No chunks found, maximum bytes: 0");
    }

    return max_bytes;
  }

  size_t get_num_lines() const {
    if (!is_open_ || !indexer_) {
      throw std::runtime_error("Reader is not open");
    }

    size_t num_lines = static_cast<size_t>(indexer_->get_num_lines());

    spdlog::debug("Total lines available: {}", num_lines);

    return num_lines;
  }

  size_t read(size_t start_bytes, size_t end_bytes, char *buffer,
              size_t buffer_size) {
    if (!is_open_ || !indexer_) {
      throw Reader::Error(Reader::Error::INITIALIZATION_ERROR,
                          "Reader is not open");
    }

    validate_read_parameters(buffer, buffer_size, start_bytes, end_bytes,
                             indexer_->get_max_bytes());

    // Create or reuse raw streaming session
    if (session_factory_->needs_new_raw_session(byte_session_.get(), gz_path_,
                                                start_bytes, end_bytes)) {
      byte_session_ = session_factory_->create_raw_session(
          gz_path_, start_bytes, end_bytes);
    }

    if (byte_session_->is_finished()) {
      return 0;
    }

    return byte_session_->stream_chunk(buffer, buffer_size);
  }

  size_t read_line_bytes(size_t start_bytes, size_t end_bytes, char *buffer,
                         size_t buffer_size) {
    if (!is_open_ || !indexer_) {
      throw Reader::Error(Reader::Error::INITIALIZATION_ERROR,
                          "Reader is not open");
    }

    if (end_bytes > indexer_->get_max_bytes()) {
      end_bytes = indexer_->get_max_bytes();
    }

    validate_read_parameters(buffer, buffer_size, start_bytes, end_bytes,
                             indexer_->get_max_bytes());

    // Create or reuse line streaming session
    if (session_factory_->needs_new_line_session(
            line_byte_session_.get(), gz_path_, start_bytes, end_bytes)) {
      line_byte_session_ = session_factory_->create_line_session(
          gz_path_, start_bytes, end_bytes);
    }

    if (line_byte_session_->is_finished()) {
      return 0;
    }

    return line_byte_session_->stream_chunk(buffer, buffer_size);
  }

  std::string read_lines(size_t start_line, size_t end_line) {
    if (!is_open_ || !indexer_) {
      throw std::runtime_error("Reader is not open");
    }

    if (start_line == 0 || end_line == 0) {
      throw std::runtime_error("Line numbers must be 1-based (start from 1)");
    }

    if (start_line > end_line) {
      throw std::runtime_error("Start line must be <= end line");
    }

    size_t total_lines = indexer_->get_num_lines();
    if (start_line > total_lines || end_line > total_lines) {
      throw std::runtime_error("Line numbers exceed total lines in file (" +
                               std::to_string(total_lines) + ")");
    }

    // For now, always read from beginning since we don't have precise line
    // tracking
    return read_lines_from_beginning(start_line, end_line);
  }

  void reset() {
    if (!is_open_ || !indexer_) {
      throw std::runtime_error("Reader is not open");
    }
    if (line_byte_session_) {
      line_byte_session_->reset();
    }
    if (byte_session_) {
      byte_session_->reset();
    }
  }

 private:
  std::string read_lines_from_beginning(size_t start_line, size_t end_line) {
    size_t max_bytes = indexer_->get_max_bytes();
    spdlog::debug("Reading lines [{}, {}] from file beginning (max bytes: {})",
                  start_line, end_line, max_bytes);

    // CRITICAL: Always create a fresh session to avoid state corruption
    // Session reuse was causing wrong line positions to be returned
    line_byte_session_ =
        session_factory_->create_line_session(gz_path_, 0, max_bytes);

    // Read through the file and extract only the requested lines
    std::string result;
    size_t current_line = 1;
    std::string current_line_content;
    const size_t buffer_size = 64 * 1024;
    std::vector<char> buffer(buffer_size);

    while (!line_byte_session_->is_finished() && current_line <= end_line) {
      size_t bytes_read =
          line_byte_session_->stream_chunk(buffer.data(), buffer_size);
      if (bytes_read == 0) break;

      for (size_t i = 0; i < bytes_read && current_line <= end_line; i++) {
        current_line_content += buffer[i];

        if (buffer[i] == '\n') {
          // We found a complete line
          if (current_line >= start_line) {
            result += current_line_content;
          }
          current_line_content.clear();
          current_line++;
        }
      }
    }

    // If we have a partial line at the end and we're still within range, add it
    if (!current_line_content.empty() && current_line >= start_line &&
        current_line <= end_line) {
      result += current_line_content;
    }

    return result;
  }

  size_t find_line_in_checkpoint(
      const dftracer::utils::indexer::CheckpointInfo &checkpoint,
      size_t checkpoint_start_line, size_t target_line) {
    spdlog::debug(
        "Finding line {} starting from checkpoint at uc_offset {} (checkpoint "
        "starts at line {})",
        target_line, checkpoint.uc_offset, checkpoint_start_line);

    // Calculate how many lines we need to skip from the checkpoint beginning
    size_t lines_to_skip = target_line - checkpoint_start_line;

    // Create a session from this checkpoint
    auto temp_session = session_factory_->create_line_session(
        gz_path_, checkpoint.uc_offset,
        checkpoint.uc_offset + checkpoint.uc_size);

    const size_t buffer_size = 64 * 1024;
    std::vector<char> buffer(buffer_size);
    size_t lines_skipped = 0;
    size_t total_bytes_read = 0;

    while (!temp_session->is_finished()) {
      size_t bytes_read =
          temp_session->stream_chunk(buffer.data(), buffer_size);
      if (bytes_read == 0) break;

      // Count newlines and find the position of the target line
      for (size_t i = 0; i < bytes_read; i++) {
        if (buffer[i] == '\n') {
          if (lines_skipped == lines_to_skip) {
            // Found the target line! Return the byte position after this
            // newline
            size_t result = checkpoint.uc_offset + total_bytes_read + i + 1;
            spdlog::debug(
                "Found line {} at byte offset {} (skipped {} lines from "
                "checkpoint)",
                target_line, result, lines_skipped);
            return result;
          }
          lines_skipped++;
        }
      }

      total_bytes_read += bytes_read;
    }

    // If we couldn't find the exact line, return the checkpoint start
    spdlog::warn(
        "Could not find line {} in checkpoint, using checkpoint boundary",
        target_line);
    return checkpoint.uc_offset;
  }

 public:
  bool is_valid() const { return is_open_ && indexer_ != nullptr; }
  const std::string &get_gz_path() const { return gz_path_; }
  const std::string &get_idx_path() const { return idx_path_; }

 private:
  static constexpr size_t INFLATE_CHUNK_SIZE = 16 * 1024;  // 16 KB

  std::string gz_path_;
  std::string idx_path_;
  bool is_open_;

  std::unique_ptr<dftracer::utils::indexer::Indexer> indexer_;
  std::unique_ptr<StreamingSessionFactory> session_factory_;
  std::unique_ptr<LineByteStreamingSession> line_byte_session_;
  std::unique_ptr<ByteStreamingSession> byte_session_;
};

// ==============================================================================
// C++ Public Interface Implementation
// ==============================================================================

Reader::Reader(const std::string &gz_path, const std::string &idx_path)
    : pImpl_(new Impl(gz_path, idx_path)) {}

Reader::~Reader() = default;

Reader::Reader(Reader &&other) noexcept : pImpl_(other.pImpl_.release()) {}

Reader &Reader::operator=(Reader &&other) noexcept {
  if (this != &other) {
    pImpl_.reset(other.pImpl_.release());
  }
  return *this;
}

size_t Reader::get_max_bytes() const { return pImpl_->get_max_bytes(); }

size_t Reader::get_num_lines() const { return pImpl_->get_num_lines(); }

size_t Reader::read(size_t start_bytes, size_t end_bytes, char *buffer,
                    size_t buffer_size) {
  return pImpl_->read(start_bytes, end_bytes, buffer, buffer_size);
}

size_t Reader::read_line_bytes(size_t start_bytes, size_t end_bytes,
                               char *buffer, size_t buffer_size) {
  return pImpl_->read_line_bytes(start_bytes, end_bytes, buffer, buffer_size);
}

std::string Reader::read_lines(size_t start, size_t end) {
  return pImpl_->read_lines(start, end);
}

void Reader::reset() { pImpl_->reset(); }

bool Reader::is_valid() const { return pImpl_ && pImpl_->is_valid(); }

const std::string &Reader::get_gz_path() const { return pImpl_->get_gz_path(); }

const std::string &Reader::get_idx_path() const {
  return pImpl_->get_idx_path();
}

std::string Reader::Error::format_message(Type type,
                                          const std::string &message) {
  const char *prefix = "";
  switch (type) {
    case Reader::Error::DATABASE_ERROR:
      prefix = "Database error";
      break;
    case Reader::Error::FILE_IO_ERROR:
      prefix = "File I/O error";
      break;
    case Reader::Error::COMPRESSION_ERROR:
      prefix = "Compression error";
      break;
    case Reader::Error::INVALID_ARGUMENT:
      prefix = "Invalid argument";
      break;
    case Reader::Error::INITIALIZATION_ERROR:
      prefix = "Initialization error";
      break;
    case Reader::Error::READ_ERROR:
      prefix = "Read error";
      break;
    case Reader::Error::UNKNOWN_ERROR:
      prefix = "Unknown error";
      break;
  }
  return std::string(prefix) + ": " + message;
}

}  // namespace reader
}  // namespace utils
}  // namespace dftracer

// ==============================================================================
// C API Implementation (wraps C++ implementation)
// ==============================================================================

extern "C" {

dft_reader_handle_t dft_reader_create(const char *gz_path,
                                      const char *idx_path) {
  if (!gz_path || !idx_path) {
    spdlog::error("Both gz_path and idx_path cannot be null");
    return nullptr;
  }

  try {
    auto *reader = new dftracer::utils::reader::Reader(gz_path, idx_path);
    return static_cast<dft_reader_handle_t>(reader);
  } catch (const std::exception &e) {
    spdlog::error("Failed to create DFT reader: {}", e.what());
    return nullptr;
  }
}

void dft_reader_destroy(dft_reader_handle_t reader) {
  if (reader) {
    delete static_cast<dftracer::utils::reader::Reader *>(reader);
  }
}

int dft_reader_get_max_bytes(dft_reader_handle_t reader, size_t *max_bytes) {
  if (!reader || !max_bytes) {
    return -1;
  }

  try {
    auto *cpp_reader = static_cast<dftracer::utils::reader::Reader *>(reader);
    *max_bytes = cpp_reader->get_max_bytes();
    return 0;
  } catch (const std::exception &e) {
    spdlog::error("Failed to get max bytes: {}", e.what());
    return -1;
  }
}

int dft_reader_get_num_lines(dft_reader_handle_t reader, size_t *num_lines) {
  if (!reader || !num_lines) {
    return -1;
  }

  try {
    auto *cpp_reader = static_cast<dftracer::utils::reader::Reader *>(reader);
    *num_lines = cpp_reader->get_num_lines();
    return 0;
  } catch (const std::exception &e) {
    spdlog::error("Failed to get number of lines: {}", e.what());
    return -1;
  }
}

int dft_reader_read(dft_reader_handle_t reader, size_t start_bytes,
                    size_t end_bytes, char *buffer, size_t buffer_size) {
  if (!reader || !buffer || buffer_size == 0) {
    return -1;
  }

  try {
    auto *cpp_reader = static_cast<dftracer::utils::reader::Reader *>(reader);
    size_t bytes_read =
        cpp_reader->read(start_bytes, end_bytes, buffer, buffer_size);
    return static_cast<int>(bytes_read);
  } catch (const std::exception &e) {
    spdlog::error("Failed to read: {}", e.what());
    return -1;
  }
}

int dft_reader_read_line_bytes(dft_reader_handle_t reader, size_t start_bytes,
                               size_t end_bytes, char *buffer,
                               size_t buffer_size) {
  if (!reader || !buffer || buffer_size == 0) {
    return -1;
  }

  try {
    auto *cpp_reader = static_cast<dftracer::utils::reader::Reader *>(reader);
    size_t bytes_read = cpp_reader->read_line_bytes(start_bytes, end_bytes,
                                                    buffer, buffer_size);
    return static_cast<int>(bytes_read);
  } catch (const std::exception &e) {
    spdlog::error("Failed to read line bytes: {}", e.what());
    return -1;
  }
}

int dft_reader_read_lines(dft_reader_handle_t reader, size_t start_line,
                          size_t end_line, char *buffer, size_t buffer_size,
                          size_t *bytes_written) {
  if (!reader || !buffer || buffer_size == 0 || !bytes_written) {
    return -1;
  }

  try {
    auto *cpp_reader = static_cast<dftracer::utils::reader::Reader *>(reader);
    std::string result = cpp_reader->read_lines(start_line, end_line);

    size_t result_size = result.size();
    if (result_size >= buffer_size) {
      // Buffer too small - return error and required size
      *bytes_written = result_size;
      return -1;
    }

    // Copy result to buffer
    std::memcpy(buffer, result.c_str(), result_size);
    buffer[result_size] = '\0';  // Null terminate
    *bytes_written = result_size;

    return 0;
  } catch (const std::exception &e) {
    spdlog::error("Failed to read lines: {}", e.what());
    *bytes_written = 0;
    return -1;
  }
}

void dft_reader_reset(dft_reader_handle_t reader) {
  if (reader) {
    static_cast<dftracer::utils::reader::Reader *>(reader)->reset();
  }
}

}  // extern "C"
