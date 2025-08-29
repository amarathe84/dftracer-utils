#ifndef DFTRACER_UTILS_READER_STREAMS_STREAM_H
#define DFTRACER_UTILS_READER_STREAMS_STREAM_H

#include <dftracer/utils/common/checkpointer.h>
#include <dftracer/utils/reader/inflater.h>
#include <dftracer/utils/indexer/checkpoint.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/error.h>

#ifdef __linux__
#include <fcntl.h>
#endif

using namespace dftracer::utils;

class Stream {
   protected:
    FILE *file_handle_;
    mutable ReaderInflater inflater_;
    size_t current_position_;
    size_t target_end_bytes_;
    bool is_active_;
    bool is_finished_;
    bool decompression_initialized_;
    bool use_checkpoint_;

    // Less frequently accessed members
    std::string current_gz_path_;
    size_t start_bytes_;
    IndexCheckpoint checkpoint_;

   public:
    Stream()
        : file_handle_(nullptr),
          current_position_(0),
          target_end_bytes_(0),
          is_active_(false),
          is_finished_(false),
          decompression_initialized_(false),
          use_checkpoint_(false),
          start_bytes_(0) {}

    virtual ~Stream() { reset(); }

    bool matches(const std::string &gz_path, size_t start_bytes,
                 size_t end_bytes) const {
        return current_gz_path_ == gz_path && start_bytes_ == start_bytes &&
               target_end_bytes_ == end_bytes;
    }

    bool is_finished() const { return is_finished_; }

    virtual std::size_t stream(char *buffer, std::size_t buffer_size) = 0;

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
        inflater_.reset();
        checkpoint_ = IndexCheckpoint();
        decompression_initialized_ = false;
    }

   protected:
    FILE *open_file(const std::string &path) {
        FILE *file = fopen(path.c_str(), "rb");
        if (!file) {
            throw ReaderError(ReaderError::FILE_IO_ERROR,
                              "Failed to open file: " + path);
        }

        // Optimize file I/O with larger buffer
        setvbuf(file, nullptr, _IOFBF, constants::reader::FILE_IO_BUFFER_SIZE);

#ifdef __linux__
        // Hint to kernel about sequential access
        int fd = fileno(file);
        posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif

        return file;
    }

    virtual void initialize(const std::string &gz_path, std::size_t start_bytes,
                            std::size_t end_bytes, Indexer &indexer) {
        if (is_active_) {
            reset();
        }
        current_gz_path_ = gz_path;
        start_bytes_ = start_bytes;
        target_end_bytes_ = end_bytes;
        is_active_ = true;
        is_finished_ = false;

        file_handle_ = open_file(gz_path);

        use_checkpoint_ = try_initialize_with_checkpoint(start_bytes, indexer);

        if (!use_checkpoint_) {
            checkpoint_ = IndexCheckpoint();
            if (!inflater_.initialize(
                    file_handle_, 0,
                    constants::indexer::ZLIB_GZIP_WINDOW_BITS)) {
                throw ReaderError(ReaderError::COMPRESSION_ERROR,
                                  "Failed to initialize inflater");
            }
        }

        decompression_initialized_ = true;
    }

    bool try_initialize_with_checkpoint(std::size_t start_bytes,
                                        Indexer &indexer) {
        bool should_use_first_checkpoint =
            start_bytes < indexer.get_checkpoint_size();

        if (should_use_first_checkpoint) {
            if (indexer.find_checkpoint(0, checkpoint_)) {
                if (inflate_init_from_checkpoint()) {
                    DFTRACER_UTILS_LOG_DEBUG(
                        "Using first checkpoint at uncompressed offset %zu for "
                        "early "
                        "target %zu",
                        checkpoint_.uc_offset, start_bytes);
                    return true;
                }
            }
        } else {
            if (indexer.find_checkpoint(start_bytes, checkpoint_)) {
                if (inflate_init_from_checkpoint()) {
                    DFTRACER_UTILS_LOG_DEBUG(
                        "Using checkpoint at uncompressed offset %llu for "
                        "target %zu",
                        checkpoint_.uc_offset, start_bytes);
                    return true;
                }
            }
        }
        return false;
    }

    void skip(std::size_t target_position) {
        std::size_t current_pos = checkpoint_.uc_offset;
        if (target_position > current_pos) {
            inflater_.skip_bytes(file_handle_, target_position - current_pos);
        }
    }

    bool is_at_target_end() const {
        return current_position_ >= target_end_bytes_;
    }

    void restart_compression() {
        inflater_.reset();
        if (use_checkpoint_) {
            if (!inflate_init_from_checkpoint()) {
                throw ReaderError(ReaderError::COMPRESSION_ERROR,
                                  "Failed to reinitialize from checkpoint");
            }
        } else {
            if (!inflater_.initialize(
                    file_handle_, 0,
                    constants::indexer::ZLIB_GZIP_WINDOW_BITS)) {
                throw ReaderError(ReaderError::COMPRESSION_ERROR,
                                  "Failed to initialize inflater");
            }
        }
    }

   private:
    bool inflate_init_from_checkpoint() const {
        return inflater_.restore_from_checkpoint(file_handle_, checkpoint_);
    }
};

#endif  // DFTRACER_UTILS_READER_STREAMS_STREAM_H
