#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/python/indexer_ext.h>
#include <dftracer/utils/python/json/helpers.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/filesystem.h>
#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace nb = nanobind;

using namespace nb::literals;
using namespace dftracer::utils;

constexpr std::uint64_t DEFAULT_STEP_SIZE_BYTES = 4 * 1024 * 1024;  // 4MB
constexpr std::uint64_t DEFAULT_STEP_SIZE_LINES = 1;

std::string trim_trailing(const char *data, std::size_t size) {
    if (size == 0) return "";

    std::size_t end = size;
    while (end > 0 && (std::isspace(data[end - 1]) || data[end - 1] == '\0')) {
        end--;
    }

    return std::string(data, end);
}

std::vector<std::string> split_lines(const std::string &data) {
    std::vector<std::string> lines;
    std::size_t start = 0;

    for (std::size_t i = 0; i < data.size(); ++i) {
        if (data[i] == '\n') {
            lines.push_back(trim_trailing(data.data() + start, i - start));
            start = i + 1;
        }
    }

    if (start < data.size()) {
        lines.push_back(
            trim_trailing(data.data() + start, data.size() - start));
    }

    return lines;
}

enum class DFTracerReaderMode {
    LineBytes,
    Bytes,
    Lines,
    JsonLines,
    JsonLinesBytes
};

template <DFTracerReaderMode Mode>
class DFTracerReader;

template <DFTracerReaderMode Mode>
class DFTracerReaderIterator {
   private:
    DFTracerReader<Mode> *reader_;
    std::uint64_t current_pos_;
    std::uint64_t max_pos_;
    std::uint64_t step_;

   public:
    DFTracerReaderIterator(DFTracerReader<Mode> *reader, std::uint64_t step)
        : reader_(reader), current_pos_(0), step_(step) {
        switch (Mode) {
            case DFTracerReaderMode::Lines:
            case DFTracerReaderMode::JsonLines:
                max_pos_ = reader_->get_num_lines();
                break;
            default:
                max_pos_ = reader_->get_max_bytes();
                break;
        }
    }

    DFTracerReaderIterator &__iter__() {
        if constexpr (Mode == DFTracerReaderMode::Lines ||
                      Mode == DFTracerReaderMode::JsonLines) {
            current_pos_ = 1;
        } else {
            current_pos_ = 0;
        }
        return *this;
    }

    typename DFTracerReader<Mode>::ReturnType __next__() {
        if (current_pos_ >= max_pos_) {
            throw nb::stop_iteration();
        }

        std::uint64_t end_pos = std::min(current_pos_ + step_, max_pos_);
        typename DFTracerReader<Mode>::ReturnType result;
        result = reader_->read(current_pos_, end_pos);
        current_pos_ = end_pos;
        return result;
    }
};

template <DFTracerReaderMode Mode>
class DFTracerReader {
   private:
    std::unique_ptr<Reader> reader_;
    std::string gzip_path_;
    std::string index_path_;
    std::uint64_t checkpoint_size_;
    bool is_open_;

    std::uint64_t current_pos_;
    std::uint64_t max_bytes_;
    std::uint64_t num_lines_;
    std::uint64_t default_step_;
    std::uint64_t default_step_lines_;

   public:
    using ReturnType = std::conditional_t<
        Mode == DFTracerReaderMode::JsonLines ||
            Mode == DFTracerReaderMode::JsonLinesBytes,
        nb::list,
        std::conditional_t<Mode == DFTracerReaderMode::Lines ||
                               Mode == DFTracerReaderMode::LineBytes,
                           std::vector<std::string>, std::string>>;

    DFTracerReader(
        const std::string &gzip_path,
        const std::optional<std::string> &index_path = std::nullopt,
        std::uint64_t checkpoint_size = Indexer::DEFAULT_CHECKPOINT_SIZE)
        : gzip_path_(gzip_path),
          checkpoint_size_(checkpoint_size),
          reader_(nullptr),
          is_open_(false),
          max_bytes_(0) {
        if constexpr (Mode == DFTracerReaderMode::Lines ||
                      Mode == DFTracerReaderMode::JsonLines) {
            current_pos_ = 1;
            default_step_ = DEFAULT_STEP_SIZE_LINES;
            default_step_lines_ = DEFAULT_STEP_SIZE_LINES;
        } else {
            current_pos_ = 0;
            default_step_ = DEFAULT_STEP_SIZE_BYTES;
            default_step_lines_ = DEFAULT_STEP_SIZE_LINES;
        }

        if (index_path.has_value()) {
            index_path_ = index_path.value();
        } else {
            index_path_ = gzip_path + ".idx";
        }

        open();
    }

    DFTracerReader(DFTracerIndexer *indexer)
        : reader_(nullptr), is_open_(false), max_bytes_(0) {
        if (!indexer) {
            throw std::runtime_error("Indexer cannot be null");
        }

        if constexpr (Mode == DFTracerReaderMode::Lines ||
                      Mode == DFTracerReaderMode::JsonLines) {
            current_pos_ = 1;
            default_step_ = DEFAULT_STEP_SIZE_LINES;
            default_step_lines_ = DEFAULT_STEP_SIZE_LINES;
        } else {
            current_pos_ = 0;
            default_step_ = DEFAULT_STEP_SIZE_BYTES;
            default_step_lines_ = DEFAULT_STEP_SIZE_LINES;
        }

        gzip_path_ = indexer->gz_path();
        index_path_ = indexer->idx_path();
        checkpoint_size_ = indexer->checkpoint_size();

        try {
            reader_ = std::make_unique<Reader>(indexer->get_indexer_ptr());
            is_open_ = true;
            max_bytes_ = static_cast<std::uint64_t>(reader_->get_max_bytes());
            num_lines_ = static_cast<std::uint64_t>(reader_->get_num_lines());
        } catch (const std::runtime_error &e) {
            throw std::runtime_error(
                "Failed to create DFT reader with indexer for gzip: " +
                gzip_path_ + " and index: " + index_path_ + " - " + e.what());
        }
    }

    ~DFTracerReader() { close(); }

    void open() {
        if (is_open_) {
            return;
        }

        if (!fs::exists(gzip_path_)) {
            throw std::runtime_error("Gzip file does not exist: " + gzip_path_);
        }

        try {
            reader_ = std::make_unique<Reader>(gzip_path_, index_path_,
                                               checkpoint_size_);
            is_open_ = true;
            max_bytes_ = static_cast<std::uint64_t>(reader_->get_max_bytes());
            num_lines_ = static_cast<std::uint64_t>(reader_->get_num_lines());
        } catch (const std::runtime_error &e) {
            throw std::runtime_error(
                "Failed to create DFT reader for gzip: " + gzip_path_ +
                " and index: " + index_path_ + " - " + e.what());
        }
    }

    void close() {
        if (is_open_ && reader_) {
            reader_->reset();
            reader_.reset();
            is_open_ = false;
            if constexpr (Mode == DFTracerReaderMode::Lines ||
                          Mode == DFTracerReaderMode::JsonLines) {
                current_pos_ = 1;
            } else {
                current_pos_ = 0;
            }
            max_bytes_ = 0;
            num_lines_ = 0;
            default_step_lines_ = DEFAULT_STEP_SIZE_LINES;
        }
    }

    std::uint64_t get_max_bytes() {
        ensure_open();
        try {
            return static_cast<std::uint64_t>(reader_->get_max_bytes());
        } catch (const std::runtime_error &e) {
            throw std::runtime_error("Failed to get maximum bytes: " +
                                     std::string(e.what()));
        }
    }

    std::uint64_t get_num_lines() {
        ensure_open();
        try {
            return static_cast<std::uint64_t>(reader_->get_num_lines());
        } catch (const std::runtime_error &e) {
            throw std::runtime_error("Failed to get number of lines: " +
                                     std::string(e.what()));
        }
    }

    void set_buffer_size(std::size_t size) {
        ensure_open();
        try {
            reader_->set_buffer_size(size);
        } catch (const std::runtime_error &e) {
            throw std::runtime_error("Failed to set buffer size: " +
                                     std::string(e.what()));
        }
    }

    void reset() {
        ensure_open();
        try {
            reader_->reset();
        } catch (const std::runtime_error &e) {
            throw std::runtime_error("Failed to reset reader: " +
                                     std::string(e.what()));
        }
    }

    bool is_valid() {
        if (!is_open_) {
            return false;
        }
        try {
            return reader_->is_valid();
        } catch (const std::runtime_error &e) {
            return false;
        }
    }

    std::string get_gz_path() {
        ensure_open();
        try {
            return reader_->get_gz_path();
        } catch (const std::runtime_error &e) {
            throw std::runtime_error("Failed to get gzip path: " +
                                     std::string(e.what()));
        }
    }

    std::string get_idx_path() {
        ensure_open();
        try {
            return reader_->get_idx_path();
        } catch (const std::runtime_error &e) {
            throw std::runtime_error("Failed to get index path: " +
                                     std::string(e.what()));
        }
    }

    DFTracerReader &__iter__() {
        ensure_open();
        if constexpr (Mode == DFTracerReaderMode::Lines ||
                      Mode == DFTracerReaderMode::JsonLines) {
            current_pos_ = 1;
        } else {
            current_pos_ = 0;
        }
        return *this;
    }

    DFTracerReaderIterator<Mode> iter(std::uint64_t step) {
        ensure_open();
        return DFTracerReaderIterator<Mode>(this, step);
    }

    ReturnType __next__() {
        ensure_open();
        std::uint64_t max_pos;
        if constexpr (Mode == DFTracerReaderMode::Lines ||
                      Mode == DFTracerReaderMode::JsonLines) {
            max_pos = num_lines_;
        } else {
            max_pos = max_bytes_;
        }

        if (current_pos_ >= max_pos) {
            throw nb::stop_iteration();
        }

        std::uint64_t end_pos = std::min(current_pos_ + default_step_, max_pos);
        ReturnType result = read(current_pos_, end_pos);
        current_pos_ = end_pos;
        return result;
    }

    void set_default_step(std::uint64_t step_bytes) {
        validate_step(step_bytes);
        default_step_ = step_bytes;
    }

    std::uint64_t get_default_step() const { return default_step_; }

    ReturnType read(std::uint64_t start, std::uint64_t end) {
        ensure_open();
        try {
            ReturnType result;
            const std::size_t buffer_size = 64 * 1024;
            std::vector<char> buffer(buffer_size);

            std::size_t bytes_read;
            if constexpr (Mode == DFTracerReaderMode::JsonLines) {
                auto json_objects = reader_->read_json_lines_owned(start, end);
                result = convert_jsondocs(json_objects);
            } else if constexpr (Mode == DFTracerReaderMode::JsonLinesBytes) {
                auto json_objects =
                    reader_->read_json_lines_bytes_owned(start, end);
                result = convert_jsondocs(json_objects);
            } else if constexpr (Mode == DFTracerReaderMode::Bytes) {
                while ((bytes_read = reader_->read(start, end, buffer.data(),
                                                   buffer.size())) > 0) {
                    result.append(buffer.data(), bytes_read);
                }
            } else if constexpr (Mode == DFTracerReaderMode::LineBytes) {
                std::string line_buffer;
                while ((bytes_read = reader_->read_line_bytes(
                            start, end, buffer.data(), buffer.size())) > 0) {
                    line_buffer.append(buffer.data(), bytes_read);
                }
                result = split_lines(line_buffer);
            } else if constexpr (Mode == DFTracerReaderMode::Lines) {
                std::string line_buffer = reader_->read_lines(start, end);
                result = split_lines(line_buffer);
            }
            return result;
        } catch (const std::runtime_error &e) {
            throw std::runtime_error("Failed to read range: " +
                                     std::string(e.what()));
        }
    }

   private:
    void ensure_open() {
        if (!is_open_) {
            throw std::runtime_error("Reader is not open");
        }
    }

    void validate_step(std::uint64_t step_bytes) {
        if (step_bytes == 0) {
            throw std::invalid_argument("step must be greater than 0");
        }
    }

   public:
    DFTracerReader &__enter__() { return *this; }

    bool __exit__(nb::args args) {
        close();
        return false;
    }

    std::string gzip_path() const { return gzip_path_; }

    std::string index_path() const { return index_path_; }

    bool is_open() const { return is_open_; }
};

template <DFTracerReaderMode Mode>
class DFTracerRangeIterator {
   private:
    DFTracerReader<Mode> *reader_;
    std::uint64_t start_pos_;
    std::uint64_t end_pos_;
    std::uint64_t current_pos_;
    std::uint64_t step_;

   public:
    DFTracerRangeIterator(DFTracerReader<Mode> *reader, std::uint64_t start,
                          std::uint64_t end, std::uint64_t step)
        : reader_(reader),
          start_pos_(start),
          end_pos_(end),
          current_pos_(start),
          step_(step) {
        if (!reader_) {
            throw std::invalid_argument("Reader cannot be null");
        }
        if (step == 0) {
            throw std::invalid_argument("Step must be greater than 0");
        }
        if (start >= end) {
            throw std::invalid_argument(
                "Start position must be less than end position");
        }

        if constexpr (Mode == DFTracerReaderMode::Lines ||
                      Mode == DFTracerReaderMode::JsonLines) {
            std::uint64_t num_lines = reader_->get_num_lines();
            if (end_pos_ > num_lines) {
                end_pos_ = num_lines;
            }
            if (start_pos_ >= num_lines) {
                throw std::invalid_argument(
                    "Start position exceeds number of lines");
            }
        } else {
            std::uint64_t max_bytes = reader_->get_max_bytes();
            if (end_pos_ > max_bytes) {
                end_pos_ = max_bytes;
            }
            if (start_pos_ >= max_bytes) {
                throw std::invalid_argument("Start position exceeds file size");
            }
        }
    }

    DFTracerRangeIterator &__iter__() {
        current_pos_ = start_pos_;
        return *this;
    }

    typename DFTracerReader<Mode>::ReturnType __next__() {
        if (current_pos_ >= end_pos_) {
            throw nb::stop_iteration();
        }

        std::uint64_t chunk_end = std::min(current_pos_ + step_, end_pos_);
        typename DFTracerReader<Mode>::ReturnType result =
            reader_->read(current_pos_, chunk_end);
        current_pos_ = chunk_end;
        return result;
    }

    std::uint64_t get_start() const { return start_pos_; }
    std::uint64_t get_end() const { return end_pos_; }
    std::uint64_t get_step() const { return step_; }
    std::uint64_t get_current() const { return current_pos_; }
};

// Mode aliases for template specializations
using DFTracerBytesReader = DFTracerReader<DFTracerReaderMode::Bytes>;
using DFTracerLineBytesReader = DFTracerReader<DFTracerReaderMode::LineBytes>;
using DFTracerLinesReader = DFTracerReader<DFTracerReaderMode::Lines>;
using DFTracerJsonLinesReader = DFTracerReader<DFTracerReaderMode::JsonLines>;
using DFTracerJsonLinesBytesReader =
    DFTracerReader<DFTracerReaderMode::JsonLinesBytes>;

using DFTracerBytesIterator = DFTracerReaderIterator<DFTracerReaderMode::Bytes>;
using DFTracerLineBytesIterator =
    DFTracerReaderIterator<DFTracerReaderMode::LineBytes>;
using DFTracerLinesIterator = DFTracerReaderIterator<DFTracerReaderMode::Lines>;
using DFTracerJsonLinesIterator =
    DFTracerReaderIterator<DFTracerReaderMode::JsonLines>;
using DFTracerJsonLinesBytesIterator =
    DFTracerReaderIterator<DFTracerReaderMode::JsonLinesBytes>;

using DFTracerLineBytesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::LineBytes>;
using DFTracerBytesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::Bytes>;
using DFTracerLinesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::Lines>;
using DFTracerJsonLinesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::JsonLines>;
using DFTracerJsonLinesBytesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::JsonLinesBytes>;

template <typename ReaderMode>
auto dft_reader_range_impl(ReaderMode &reader, std::uint64_t start,
                           std::uint64_t end, const std::string &mode,
                           std::uint64_t step = 0) {
    if (step == 0) {
        if (mode == "lines") {
            step = DEFAULT_STEP_SIZE_LINES;
        } else {
            step = DEFAULT_STEP_SIZE_BYTES;
        }
    }

    if (mode == "line_bytes") {
        if constexpr (std::is_same_v<ReaderMode, DFTracerLineBytesReader>) {
            return DFTracerLineBytesRangeIterator(&reader, start, end, step);
        } else {
            throw std::invalid_argument(
                "Reader type mismatch for line_bytes mode");
        }
    } else if (mode == "bytes") {
        if constexpr (std::is_same_v<ReaderMode, DFTracerBytesReader>) {
            return DFTracerBytesRangeIterator(&reader, start, end, step);
        } else {
            throw std::invalid_argument("Reader type mismatch for bytes mode");
        }
    } else if (mode == "lines") {
        if constexpr (std::is_same_v<ReaderMode, DFTracerLinesReader>) {
            return DFTracerLinesRangeIterator(&reader, start, end, step);
        } else {
            throw std::invalid_argument("Reader type mismatch for lines mode");
        }
    } else {
        throw std::invalid_argument(
            "Invalid mode. Must be 'line_bytes', 'bytes', or 'lines'");
    }
}

// Wrapper functions for different reader types
DFTracerLineBytesRangeIterator dft_reader_range(
    DFTracerLineBytesReader &reader, std::uint64_t start, std::uint64_t end,
    const std::string &mode = "line_bytes",
    std::uint64_t step = DEFAULT_STEP_SIZE_BYTES) {
    return dft_reader_range_impl(reader, start, end, mode, step);
}

DFTracerBytesRangeIterator dft_reader_range(
    DFTracerBytesReader &reader, std::uint64_t start, std::uint64_t end,
    const std::string &mode = "bytes",
    std::uint64_t step = DEFAULT_STEP_SIZE_BYTES) {
    return dft_reader_range_impl(reader, start, end, mode, step);
}

DFTracerLinesRangeIterator dft_reader_range(
    DFTracerLinesReader &reader, std::uint64_t start, std::uint64_t end,
    const std::string &mode = "lines",
    std::uint64_t step = DEFAULT_STEP_SIZE_LINES) {
    return dft_reader_range_impl(reader, start, end, mode, step);
}

void register_reader(nb::module_ &m) {
    nb::class_<DFTracerBytesIterator>(m, "DFTracerBytesIterator")
        .def("__iter__", &DFTracerBytesIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerBytesIterator::__next__,
             "Get next bytes chunk");

    nb::class_<DFTracerLineBytesIterator>(m, "DFTracerLineBytesIterator")
        .def("__iter__", &DFTracerLineBytesIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerLineBytesIterator::__next__,
             "Get next line bytes chunk");

    nb::class_<DFTracerLinesIterator>(m, "DFTracerLinesIterator")
        .def("__iter__", &DFTracerLinesIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerLinesIterator::__next__,
             "Get next lines chunk");

    nb::class_<DFTracerJsonLinesIterator>(m, "DFTracerJsonLinesIterator")
        .def("__iter__", &DFTracerJsonLinesIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerJsonLinesIterator::__next__,
             "Get next JSON lines chunk");

    nb::class_<DFTracerJsonLinesBytesIterator>(m,
                                               "DFTracerJsonLinesBytesIterator")
        .def("__iter__", &DFTracerJsonLinesBytesIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerJsonLinesBytesIterator::__next__,
             "Get next JSON lines bytes chunk");

    nb::class_<DFTracerBytesRangeIterator>(m, "DFTracerBytesRangeIterator")
        .def("__iter__", &DFTracerBytesRangeIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerBytesRangeIterator::__next__,
             "Get next bytes chunk")
        .def_prop_ro("start", &DFTracerBytesRangeIterator::get_start,
                     "Start position")
        .def_prop_ro("end", &DFTracerBytesRangeIterator::get_end,
                     "End position")
        .def_prop_ro("step", &DFTracerBytesRangeIterator::get_step, "Step size")
        .def_prop_ro("current", &DFTracerBytesRangeIterator::get_current,
                     "Current position");

    nb::class_<DFTracerLineBytesRangeIterator>(m,
                                               "DFTracerLineBytesRangeIterator")
        .def("__iter__", &DFTracerLineBytesRangeIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerLineBytesRangeIterator::__next__,
             "Get next line bytes chunk")
        .def_prop_ro("start", &DFTracerLineBytesRangeIterator::get_start,
                     "Start position")
        .def_prop_ro("end", &DFTracerLineBytesRangeIterator::get_end,
                     "End position")
        .def_prop_ro("step", &DFTracerLineBytesRangeIterator::get_step,
                     "Step size")
        .def_prop_ro("current", &DFTracerLineBytesRangeIterator::get_current,
                     "Current position");

    nb::class_<DFTracerLinesRangeIterator>(m, "DFTracerLinesRangeIterator")
        .def("__iter__", &DFTracerLinesRangeIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerLinesRangeIterator::__next__,
             "Get next lines chunk")
        .def_prop_ro("start", &DFTracerLinesRangeIterator::get_start,
                     "Start position")
        .def_prop_ro("end", &DFTracerLinesRangeIterator::get_end,
                     "End position")
        .def_prop_ro("step", &DFTracerLinesRangeIterator::get_step, "Step size")
        .def_prop_ro("current", &DFTracerLinesRangeIterator::get_current,
                     "Current position");

    nb::class_<DFTracerJsonLinesRangeIterator>(m,
                                               "DFTracerJsonLinesRangeIterator")
        .def("__iter__", &DFTracerJsonLinesRangeIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerJsonLinesRangeIterator::__next__,
             "Get next JSON lines chunk")
        .def_prop_ro("start", &DFTracerJsonLinesRangeIterator::get_start,
                     "Start position")
        .def_prop_ro("end", &DFTracerJsonLinesRangeIterator::get_end,
                     "End position")
        .def_prop_ro("step", &DFTracerJsonLinesRangeIterator::get_step,
                     "Step size")
        .def_prop_ro("current", &DFTracerJsonLinesRangeIterator::get_current,
                     "Current position");

    nb::class_<DFTracerJsonLinesBytesRangeIterator>(
        m, "DFTracerJsonLinesBytesRangeIterator")
        .def("__iter__", &DFTracerJsonLinesBytesRangeIterator::__iter__,
             nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerJsonLinesBytesRangeIterator::__next__,
             "Get next JSON lines bytes chunk")
        .def_prop_ro("start", &DFTracerJsonLinesBytesRangeIterator::get_start,
                     "Start position")
        .def_prop_ro("end", &DFTracerJsonLinesBytesRangeIterator::get_end,
                     "End position")
        .def_prop_ro("step", &DFTracerJsonLinesBytesRangeIterator::get_step,
                     "Step size")
        .def_prop_ro("current",
                     &DFTracerJsonLinesBytesRangeIterator::get_current,
                     "Current position");

    nb::class_<DFTracerBytesReader>(m, "DFTracerBytesReader")
        .def(
            nb::init<const std::string &, const std::optional<std::string> &>(),
            "gzip_path"_a, "index_path"_a = nb::none(),
            "Create a DFTracer bytes reader for a gzip file and its index")
        .def(nb::init<DFTracerIndexer *>(), "indexer"_a,
             "Create a DFTracer bytes reader from an existing indexer")
        .def("get_max_bytes", &DFTracerBytesReader::get_max_bytes,
             "Get the maximum byte position available in the file")
        .def("get_num_lines", &DFTracerBytesReader::get_num_lines,
             "Get the number of lines in the file")
        .def("set_buffer_size", &DFTracerBytesReader::set_buffer_size, "size"_a,
             "Set the buffer size for reading operations")
        .def("reset", &DFTracerBytesReader::reset,
             "Reset the reader to initial state")
        .def("is_valid", &DFTracerBytesReader::is_valid,
             "Check if the reader is valid")
        .def("get_gz_path", &DFTracerBytesReader::get_gz_path,
             "Get the gzip file path")
        .def("get_idx_path", &DFTracerBytesReader::get_idx_path,
             "Get the index file path")
        .def("iter", &DFTracerBytesReader::iter,
             "step"_a = DEFAULT_STEP_SIZE_BYTES,
             "Get iterator with optional step size")
        .def("__iter__", &DFTracerBytesReader::__iter__,
             nb::rv_policy::reference_internal, "Get iterator for the reader")
        .def("__next__", &DFTracerBytesReader::__next__,
             "Get next chunk with default step")
        .def("set_default_step", &DFTracerBytesReader::set_default_step,
             "step"_a, "Set default step for iteration")
        .def("get_default_step", &DFTracerBytesReader::get_default_step,
             "Get current default step")
        .def("read", &DFTracerBytesReader::read, "start"_a, "end"_a,
             "Read a range from the gzip file")
        .def("open", &DFTracerBytesReader::open, "Open the index database")
        .def("close", &DFTracerBytesReader::close, "Close the index database")
        .def("__enter__", &DFTracerBytesReader::__enter__,
             nb::rv_policy::reference_internal, "Enter context manager")
        .def("__exit__", &DFTracerBytesReader::__exit__, "Exit context manager")
        .def_prop_ro("gzip_path", &DFTracerBytesReader::gzip_path,
                     "Path to the gzip file")
        .def_prop_ro("index_path", &DFTracerBytesReader::index_path,
                     "Path to the index file")
        .def_prop_ro("is_open", &DFTracerBytesReader::is_open,
                     "Whether the database is open");

    nb::class_<DFTracerLineBytesReader>(m, "DFTracerLineBytesReader")
        .def(
            nb::init<const std::string &, const std::optional<std::string> &>(),
            "gzip_path"_a, "index_path"_a = nb::none(),
            "Create a DFTracer line bytes reader for a gzip file and its index")
        .def(nb::init<DFTracerIndexer *>(), "indexer"_a,
             "Create a DFTracer line bytes reader from an existing indexer")
        .def("get_max_bytes", &DFTracerLineBytesReader::get_max_bytes,
             "Get the maximum byte position available in the file")
        .def("get_num_lines", &DFTracerLineBytesReader::get_num_lines,
             "Get the number of lines in the file")
        .def("set_buffer_size", &DFTracerLineBytesReader::set_buffer_size,
             "size"_a, "Set the buffer size for reading operations")
        .def("reset", &DFTracerLineBytesReader::reset,
             "Reset the reader to initial state")
        .def("is_valid", &DFTracerLineBytesReader::is_valid,
             "Check if the reader is valid")
        .def("get_gz_path", &DFTracerLineBytesReader::get_gz_path,
             "Get the gzip file path")
        .def("get_idx_path", &DFTracerLineBytesReader::get_idx_path,
             "Get the index file path")
        .def("iter", &DFTracerLineBytesReader::iter,
             "step"_a = DEFAULT_STEP_SIZE_BYTES,
             "Get iterator with optional step size")
        .def("__iter__", &DFTracerLineBytesReader::__iter__,
             nb::rv_policy::reference_internal, "Get iterator for the reader")
        .def("__next__", &DFTracerLineBytesReader::__next__,
             "Get next chunk with default step")
        .def("set_default_step", &DFTracerLineBytesReader::set_default_step,
             "step"_a, "Set default step for iteration")
        .def("get_default_step", &DFTracerLineBytesReader::get_default_step,
             "Get current default step")
        .def("read", &DFTracerLineBytesReader::read, "start"_a, "end"_a,
             "Read a range from the gzip file")
        .def("open", &DFTracerLineBytesReader::open, "Open the index database")
        .def("close", &DFTracerLineBytesReader::close,
             "Close the index database")
        .def("__enter__", &DFTracerLineBytesReader::__enter__,
             nb::rv_policy::reference_internal, "Enter context manager")
        .def("__exit__", &DFTracerLineBytesReader::__exit__,
             "Exit context manager")
        .def_prop_ro("gzip_path", &DFTracerLineBytesReader::gzip_path,
                     "Path to the gzip file")
        .def_prop_ro("index_path", &DFTracerLineBytesReader::index_path,
                     "Path to the index file")
        .def_prop_ro("is_open", &DFTracerLineBytesReader::is_open,
                     "Whether the database is open");

    nb::class_<DFTracerLinesReader>(m, "DFTracerLinesReader")
        .def(
            nb::init<const std::string &, const std::optional<std::string> &>(),
            "gzip_path"_a, "index_path"_a = nb::none(),
            "Create a DFTracer lines reader for a gzip file and its index")
        .def(nb::init<DFTracerIndexer *>(), "indexer"_a,
             "Create a DFTracer lines reader from an existing indexer")
        .def("get_max_bytes", &DFTracerLinesReader::get_max_bytes,
             "Get the maximum byte position available in the file")
        .def("get_num_lines", &DFTracerLinesReader::get_num_lines,
             "Get the number of lines in the file")
        .def("set_buffer_size", &DFTracerLinesReader::set_buffer_size, "size"_a,
             "Set the buffer size for reading operations")
        .def("reset", &DFTracerLinesReader::reset,
             "Reset the reader to initial state")
        .def("is_valid", &DFTracerLinesReader::is_valid,
             "Check if the reader is valid")
        .def("get_gz_path", &DFTracerLinesReader::get_gz_path,
             "Get the gzip file path")
        .def("get_idx_path", &DFTracerLinesReader::get_idx_path,
             "Get the index file path")
        .def("iter", &DFTracerLinesReader::iter,
             "step"_a = DEFAULT_STEP_SIZE_LINES,
             "Get iterator with optional step size")
        .def("__iter__", &DFTracerLinesReader::__iter__,
             nb::rv_policy::reference_internal, "Get iterator for the reader")
        .def("__next__", &DFTracerLinesReader::__next__,
             "Get next chunk with default step")
        .def("set_default_step", &DFTracerLinesReader::set_default_step,
             "step"_a, "Set default step for iteration")
        .def("get_default_step", &DFTracerLinesReader::get_default_step,
             "Get current default step")
        .def("read", &DFTracerLinesReader::read, "start"_a, "end"_a,
             "Read a range from the gzip file")
        .def("open", &DFTracerLinesReader::open, "Open the index database")
        .def("close", &DFTracerLinesReader::close, "Close the index database")
        .def("__enter__", &DFTracerLinesReader::__enter__,
             nb::rv_policy::reference_internal, "Enter context manager")
        .def("__exit__", &DFTracerLinesReader::__exit__, "Exit context manager")
        .def_prop_ro("gzip_path", &DFTracerLinesReader::gzip_path,
                     "Path to the gzip file")
        .def_prop_ro("index_path", &DFTracerLinesReader::index_path,
                     "Path to the index file")
        .def_prop_ro("is_open", &DFTracerLinesReader::is_open,
                     "Whether the database is open");

    nb::class_<DFTracerJsonLinesReader>(m, "DFTracerJsonLinesReader")
        .def(
            nb::init<const std::string &, const std::optional<std::string> &>(),
            "gzip_path"_a, "index_path"_a = nb::none(),
            "Create a DFTracer JSON lines reader for a gzip file and its index")
        .def(nb::init<DFTracerIndexer *>(), "indexer"_a,
             "Create a DFTracer JSON lines reader from an existing indexer")
        .def("get_max_bytes", &DFTracerJsonLinesReader::get_max_bytes,
             "Get the maximum byte position available in the file")
        .def("get_num_lines", &DFTracerJsonLinesReader::get_num_lines,
             "Get the number of lines in the file")
        .def("set_buffer_size", &DFTracerJsonLinesReader::set_buffer_size,
             "size"_a, "Set the buffer size for reading operations")
        .def("reset", &DFTracerJsonLinesReader::reset,
             "Reset the reader to initial state")
        .def("is_valid", &DFTracerJsonLinesReader::is_valid,
             "Check if the reader is valid")
        .def("get_gz_path", &DFTracerJsonLinesReader::get_gz_path,
             "Get the gzip file path")
        .def("get_idx_path", &DFTracerJsonLinesReader::get_idx_path,
             "Get the index file path")
        .def("iter", &DFTracerJsonLinesReader::iter,
             "step"_a = DEFAULT_STEP_SIZE_LINES,
             "Get iterator with optional step size")
        .def("__iter__", &DFTracerJsonLinesReader::__iter__,
             nb::rv_policy::reference_internal, "Get iterator for the reader")
        .def("__next__", &DFTracerJsonLinesReader::__next__,
             "Get next chunk with default step")
        .def("set_default_step", &DFTracerJsonLinesReader::set_default_step,
             "step"_a, "Set default step for iteration")
        .def("get_default_step", &DFTracerJsonLinesReader::get_default_step,
             "Get current default step")
        .def("read", &DFTracerJsonLinesReader::read, "start"_a, "end"_a,
             "Read a range from the gzip file and return as Python list of "
             "dictionaries")
        .def("open", &DFTracerJsonLinesReader::open, "Open the index database")
        .def("close", &DFTracerJsonLinesReader::close,
             "Close the index database")
        .def("__enter__", &DFTracerJsonLinesReader::__enter__,
             nb::rv_policy::reference_internal, "Enter context manager")
        .def("__exit__", &DFTracerJsonLinesReader::__exit__,
             "Exit context manager")
        .def_prop_ro("gzip_path", &DFTracerJsonLinesReader::gzip_path,
                     "Path to the gzip file")
        .def_prop_ro("index_path", &DFTracerJsonLinesReader::index_path,
                     "Path to the index file")
        .def_prop_ro("is_open", &DFTracerJsonLinesReader::is_open,
                     "Whether the database is open");

    nb::class_<DFTracerJsonLinesBytesReader>(m, "DFTracerJsonLinesBytesReader")
        .def(
            nb::init<const std::string &, const std::optional<std::string> &>(),
            "gzip_path"_a, "index_path"_a = nb::none(),
            "Create a DFTracer JSON lines bytes reader for a gzip file and its "
            "index")
        .def(nb::init<DFTracerIndexer *>(), "indexer"_a,
             "Create a DFTracer JSON lines bytes reader from an existing "
             "indexer")
        .def("get_max_bytes", &DFTracerJsonLinesBytesReader::get_max_bytes,
             "Get the maximum byte position available in the file")
        .def("get_num_lines", &DFTracerJsonLinesBytesReader::get_num_lines,
             "Get the number of lines in the file")
        .def("set_buffer_size", &DFTracerJsonLinesBytesReader::set_buffer_size,
             "size"_a, "Set the buffer size for reading operations")
        .def("reset", &DFTracerJsonLinesBytesReader::reset,
             "Reset the reader to initial state")
        .def("is_valid", &DFTracerJsonLinesBytesReader::is_valid,
             "Check if the reader is valid")
        .def("get_gz_path", &DFTracerJsonLinesBytesReader::get_gz_path,
             "Get the gzip file path")
        .def("get_idx_path", &DFTracerJsonLinesBytesReader::get_idx_path,
             "Get the index file path")
        .def("iter", &DFTracerJsonLinesBytesReader::iter,
             "step"_a = DEFAULT_STEP_SIZE_BYTES,
             "Get iterator with optional step size")
        .def("__iter__", &DFTracerJsonLinesBytesReader::__iter__,
             nb::rv_policy::reference_internal, "Get iterator for the reader")
        .def("__next__", &DFTracerJsonLinesBytesReader::__next__,
             "Get next chunk with default step")
        .def("set_default_step",
             &DFTracerJsonLinesBytesReader::set_default_step, "step"_a,
             "Set default step for iteration")
        .def("get_default_step",
             &DFTracerJsonLinesBytesReader::get_default_step,
             "Get current default step")
        .def("read", &DFTracerJsonLinesBytesReader::read, "start"_a, "end"_a,
             "Read a range from the gzip file and return as Python list of "
             "dictionaries")
        .def("open", &DFTracerJsonLinesBytesReader::open,
             "Open the index database")
        .def("close", &DFTracerJsonLinesBytesReader::close,
             "Close the index database")
        .def("__enter__", &DFTracerJsonLinesBytesReader::__enter__,
             nb::rv_policy::reference_internal, "Enter context manager")
        .def("__exit__", &DFTracerJsonLinesBytesReader::__exit__,
             "Exit context manager")
        .def_prop_ro("gzip_path", &DFTracerJsonLinesBytesReader::gzip_path,
                     "Path to the gzip file")
        .def_prop_ro("index_path", &DFTracerJsonLinesBytesReader::index_path,
                     "Path to the index file")
        .def_prop_ro("is_open", &DFTracerJsonLinesBytesReader::is_open,
                     "Whether the database is open");

    // Generic dft_reader_range function with mode parameter
    m.def("dft_reader_range",
          static_cast<DFTracerLineBytesRangeIterator (*)(
              DFTracerLineBytesReader &, std::uint64_t, std::uint64_t,
              const std::string &, std::uint64_t)>(&dft_reader_range),
          "reader"_a, "start"_a, "end"_a, "mode"_a = "line_bytes",
          "step"_a = DEFAULT_STEP_SIZE_BYTES,
          "Create a range iterator with specified mode ('line_bytes', 'bytes', "
          "or 'lines')");

    m.def("dft_reader_range",
          static_cast<DFTracerBytesRangeIterator (*)(
              DFTracerBytesReader &, std::uint64_t, std::uint64_t,
              const std::string &, std::uint64_t)>(&dft_reader_range),
          "reader"_a, "start"_a, "end"_a, "mode"_a = "bytes",
          "step"_a = DEFAULT_STEP_SIZE_BYTES,
          "Create a range iterator with specified mode ('line_bytes', 'bytes', "
          "or 'lines')");

    m.def("dft_reader_range",
          static_cast<DFTracerLinesRangeIterator (*)(
              DFTracerLinesReader &, std::uint64_t, std::uint64_t,
              const std::string &, std::uint64_t)>(&dft_reader_range),
          "reader"_a, "start"_a, "end"_a, "mode"_a = "lines",
          "step"_a = DEFAULT_STEP_SIZE_LINES,
          "Create a range iterator with specified mode ('line_bytes', 'bytes', "
          "or 'lines')");

    m.attr("DFTracerReader") = m.attr("DFTracerLineBytesReader");
}
