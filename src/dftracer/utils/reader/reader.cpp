#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/error.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/reader/reader_impl.h>

using namespace constants;

namespace dftracer::utils {
Reader::Reader(const std::string &gz_path, const std::string &idx_path,
               std::size_t index_ckpt_size)
    : p_impl_(new ReaderImplementor(gz_path, idx_path, index_ckpt_size)) {}

Reader::Reader(Indexer *indexer) : p_impl_(new ReaderImplementor(indexer)) {}

Reader::~Reader() = default;

Reader::Reader(Reader &&other) noexcept : p_impl_(other.p_impl_.release()) {}

Reader &Reader::operator=(Reader &&other) noexcept {
    if (this != &other) {
        p_impl_.reset(other.p_impl_.release());
    }
    return *this;
}

std::size_t Reader::get_max_bytes() const { return p_impl_->get_max_bytes(); }

std::size_t Reader::get_num_lines() const { return p_impl_->get_num_lines(); }

std::size_t Reader::read(std::size_t start_bytes, std::size_t end_bytes,
                         char *buffer, std::size_t buffer_size) {
    return p_impl_->read(start_bytes, end_bytes, buffer, buffer_size);
}

std::size_t Reader::read_line_bytes(std::size_t start_bytes,
                                    std::size_t end_bytes, char *buffer,
                                    std::size_t buffer_size) {
    return p_impl_->read_line_bytes(start_bytes, end_bytes, buffer,
                                    buffer_size);
}

std::string Reader::read_lines(std::size_t start, std::size_t end) {
    return p_impl_->read_lines(start, end);
}

void Reader::read_lines_with_processor(std::size_t start, std::size_t end,
                                       LineProcessor &processor) {
    p_impl_->read_lines_with_processor(start, end, processor);
}

void Reader::read_line_bytes_with_processor(std::size_t start_bytes,
                                            std::size_t end_bytes,
                                            LineProcessor &processor) {
    p_impl_->read_line_bytes_with_processor(start_bytes, end_bytes, processor);
}

void Reader::set_buffer_size(std::size_t size) {
    p_impl_->set_buffer_size(size);
}

void Reader::reset() { p_impl_->reset(); }

bool Reader::is_valid() const { return p_impl_ && p_impl_->is_valid(); }

const std::string &Reader::get_gz_path() const {
    return p_impl_->get_gz_path();
}

const std::string &Reader::get_idx_path() const {
    return p_impl_->get_idx_path();
}
}  // namespace dftracer::utils
