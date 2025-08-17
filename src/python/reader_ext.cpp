#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/filesystem.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace nb = nanobind;

using namespace nb::literals;

namespace constants {

constexpr uint64_t DEFAULT_STEP_SIZE_BYTES = 4 * 1024 * 1024;  // 4MB
constexpr uint64_t DEFAULT_STEP_SIZE_LINES = 1;

}  // namespace constants

std::string trim_trailing(const char *data, size_t size) {
  if (size == 0) return "";

  size_t end = size;
  while (end > 0 && (std::isspace(data[end - 1]) || data[end - 1] == '\0')) {
    end--;
  }

  return std::string(data, end);
}

std::vector<std::string> split_lines(const std::string &data) {
  std::vector<std::string> lines;
  size_t start = 0;

  for (size_t i = 0; i < data.size(); ++i) {
    if (data[i] == '\n') {
      lines.push_back(trim_trailing(data.data() + start, i - start));
      start = i + 1;
    }
  }

  if (start < data.size()) {
    lines.push_back(trim_trailing(data.data() + start, data.size() - start));
  }

  return lines;
}

enum class DFTracerReaderMode { LineBytes, Bytes, Lines };

template <DFTracerReaderMode Mode>
class DFTracerReader;

template <DFTracerReaderMode Mode>
class DFTracerReaderIterator {
 private:
  DFTracerReader<Mode> *reader_;
  uint64_t current_pos_;
  uint64_t max_pos_;
  uint64_t step_;

 public:
  DFTracerReaderIterator(DFTracerReader<Mode> *reader, uint64_t step)
      : reader_(reader), current_pos_(0), step_(step) {
    switch (Mode) {
      case DFTracerReaderMode::Lines:
        max_pos_ = reader_->get_num_lines();
        break;
      default:
        max_pos_ = reader_->get_max_bytes();
        break;
    }
  }

  DFTracerReaderIterator &__iter__() {
    if constexpr (Mode == DFTracerReaderMode::Lines) {
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

    uint64_t end_pos = std::min(current_pos_ + step_, max_pos_);
    typename DFTracerReader<Mode>::ReturnType result;
    result = reader_->read(current_pos_, end_pos);
    current_pos_ = end_pos;
    return result;
  }
};

template <DFTracerReaderMode Mode>
class DFTracerReader {
 private:
  std::unique_ptr<dftracer::utils::reader::Reader> reader_;
  std::string gzip_path_;
  std::string index_path_;
  bool is_open_;

  uint64_t current_pos_;
  uint64_t max_bytes_;
  uint64_t num_lines_;
  uint64_t default_step_;
  uint64_t default_step_lines_;

 public:
  using ReturnType =
      std::conditional_t<Mode == DFTracerReaderMode::Lines ||
                             Mode == DFTracerReaderMode::LineBytes,
                         std::vector<std::string>, std::string>;

  DFTracerReader(const std::string &gzip_path,
                 const std::optional<std::string> &index_path = std::nullopt)
      : gzip_path_(gzip_path),
        reader_(nullptr),
        is_open_(false),
        max_bytes_(0) {
    if constexpr (Mode == DFTracerReaderMode::Lines) {
      current_pos_ = 1;
      default_step_ = constants::DEFAULT_STEP_SIZE_LINES;
      default_step_lines_ = constants::DEFAULT_STEP_SIZE_LINES;
    } else {
      current_pos_ = 0;
      default_step_ = constants::DEFAULT_STEP_SIZE_BYTES;
      default_step_lines_ = constants::DEFAULT_STEP_SIZE_LINES;
    }

    if (index_path.has_value()) {
      index_path_ = index_path.value();
    } else {
      index_path_ = gzip_path + ".idx";
    }

    open();
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
      reader_ = std::make_unique<dftracer::utils::reader::Reader>(gzip_path_,
                                                                  index_path_);
      is_open_ = true;
      max_bytes_ = static_cast<uint64_t>(reader_->get_max_bytes());
      num_lines_ = static_cast<uint64_t>(reader_->get_num_lines());
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
      if constexpr (Mode == DFTracerReaderMode::Lines) {
        current_pos_ = 1;
      } else {
        current_pos_ = 0;
      }
      max_bytes_ = 0;
      num_lines_ = 0;
      default_step_lines_ = constants::DEFAULT_STEP_SIZE_LINES;
    }
  }

  uint64_t get_max_bytes() {
    ensure_open();
    try {
      return static_cast<uint64_t>(reader_->get_max_bytes());
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to get maximum bytes: " +
                               std::string(e.what()));
    }
  }

  uint64_t get_num_lines() {
    ensure_open();
    try {
      return static_cast<uint64_t>(reader_->get_num_lines());
    } catch (const std::runtime_error &e) {
      throw std::runtime_error("Failed to get number of lines: " +
                               std::string(e.what()));
    }
  }

  DFTracerReader &__iter__() {
    ensure_open();
    if constexpr (Mode == DFTracerReaderMode::Lines) {
      current_pos_ = 1;
    } else {
      current_pos_ = 0;
    }
    return *this;
  }

  DFTracerReaderIterator<Mode> iter(uint64_t step) {
    ensure_open();
    return DFTracerReaderIterator<Mode>(this, step);
  }

  ReturnType __next__() {
    ensure_open();
    uint64_t max_pos;
    if constexpr (Mode == DFTracerReaderMode::Lines) {
      max_pos = num_lines_;
    } else {
      max_pos = max_bytes_;
    }

    if (current_pos_ >= max_pos) {
      throw nb::stop_iteration();
    }

    uint64_t end_pos = std::min(current_pos_ + default_step_, max_pos);
    ReturnType result = read(current_pos_, end_pos);
    current_pos_ = end_pos;
    return result;
  }

  void set_default_step(uint64_t step_bytes) {
    validate_step(step_bytes);
    default_step_ = step_bytes;
  }

  uint64_t get_default_step() const { return default_step_; }

  ReturnType read(uint64_t start, uint64_t end) {
    ensure_open();
    try {
      ReturnType result;
      const size_t buffer_size = 64 * 1024;
      std::vector<char> buffer(buffer_size);

      size_t bytes_read;
      if constexpr (Mode == DFTracerReaderMode::Bytes) {
        while ((bytes_read = reader_->read(start, end, buffer.data(),
                                           buffer.size())) > 0) {
          result.append(buffer.data(), bytes_read);
        }
      } else if constexpr (Mode == DFTracerReaderMode::LineBytes) {
        std::string line_buffer;
        while ((bytes_read = reader_->read_line_bytes(start, end, buffer.data(),
                                                      buffer.size())) > 0) {
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

  void validate_step(uint64_t step_bytes) {
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
  uint64_t start_pos_;
  uint64_t end_pos_;
  uint64_t current_pos_;
  uint64_t step_;

 public:
  DFTracerRangeIterator(DFTracerReader<Mode> *reader, uint64_t start,
                        uint64_t end, uint64_t step)
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

    if constexpr (Mode == DFTracerReaderMode::Lines) {
      uint64_t num_lines = reader_->get_num_lines();
      if (end_pos_ > num_lines) {
        end_pos_ = num_lines;
      }
      if (start_pos_ >= num_lines) {
        throw std::invalid_argument("Start position exceeds number of lines");
      }
    } else {
      uint64_t max_bytes = reader_->get_max_bytes();
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

    uint64_t chunk_end = std::min(current_pos_ + step_, end_pos_);
    typename DFTracerReader<Mode>::ReturnType result =
        reader_->read(current_pos_, chunk_end);
    current_pos_ = chunk_end;
    return result;
  }

  uint64_t get_start() const { return start_pos_; }
  uint64_t get_end() const { return end_pos_; }
  uint64_t get_step() const { return step_; }
  uint64_t get_current() const { return current_pos_; }
};

// Mode aliases for template specializations
using DFTracerBytesReader = DFTracerReader<DFTracerReaderMode::Bytes>;
using DFTracerLineBytesReader = DFTracerReader<DFTracerReaderMode::LineBytes>;
using DFTracerLinesReader = DFTracerReader<DFTracerReaderMode::Lines>;

using DFTracerBytesIterator = DFTracerReaderIterator<DFTracerReaderMode::Bytes>;
using DFTracerLineBytesIterator =
    DFTracerReaderIterator<DFTracerReaderMode::LineBytes>;
using DFTracerLinesIterator = DFTracerReaderIterator<DFTracerReaderMode::Lines>;

using DFTracerLineBytesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::LineBytes>;
using DFTracerBytesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::Bytes>;
using DFTracerLinesRangeIterator =
    DFTracerRangeIterator<DFTracerReaderMode::Lines>;

template <typename ReaderMode>
auto dft_reader_range_impl(ReaderMode &reader, uint64_t start, uint64_t end,
                           const std::string &mode, uint64_t step = 0) {
  if (step == 0) {
    if (mode == "lines") {
      step = constants::DEFAULT_STEP_SIZE_LINES;
    } else {
      step = constants::DEFAULT_STEP_SIZE_BYTES;
    }
  }

  if (mode == "line_bytes") {
    if constexpr (std::is_same_v<ReaderMode, DFTracerLineBytesReader>) {
      return DFTracerLineBytesRangeIterator(&reader, start, end, step);
    } else {
      throw std::invalid_argument("Reader type mismatch for line_bytes mode");
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
    DFTracerLineBytesReader &reader, uint64_t start, uint64_t end,
    const std::string &mode = "line_bytes",
    uint64_t step = constants::DEFAULT_STEP_SIZE_BYTES) {
  return dft_reader_range_impl(reader, start, end, mode, step);
}

DFTracerBytesRangeIterator dft_reader_range(
    DFTracerBytesReader &reader, uint64_t start, uint64_t end,
    const std::string &mode = "bytes",
    uint64_t step = constants::DEFAULT_STEP_SIZE_BYTES) {
  return dft_reader_range_impl(reader, start, end, mode, step);
}

DFTracerLinesRangeIterator dft_reader_range(
    DFTracerLinesReader &reader, uint64_t start, uint64_t end,
    const std::string &mode = "lines",
    uint64_t step = constants::DEFAULT_STEP_SIZE_LINES) {
  return dft_reader_range_impl(reader, start, end, mode, step);
}

NB_MODULE(reader_ext, m) {
  m.doc() = "DFTracer utilities reader extension";

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

  nb::class_<DFTracerBytesRangeIterator>(m, "DFTracerBytesRangeIterator")
      .def("__iter__", &DFTracerBytesRangeIterator::__iter__,
           nb::rv_policy::reference_internal, "Get iterator")
      .def("__next__", &DFTracerBytesRangeIterator::__next__,
           "Get next bytes chunk")
      .def_prop_ro("start", &DFTracerBytesRangeIterator::get_start,
                   "Start position")
      .def_prop_ro("end", &DFTracerBytesRangeIterator::get_end, "End position")
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
      .def_prop_ro("end", &DFTracerLinesRangeIterator::get_end, "End position")
      .def_prop_ro("step", &DFTracerLinesRangeIterator::get_step, "Step size")
      .def_prop_ro("current", &DFTracerLinesRangeIterator::get_current,
                   "Current position");

  nb::class_<DFTracerBytesReader>(m, "DFTracerBytesReader")
      .def(nb::init<const std::string &, const std::optional<std::string> &>(),
           "gzip_path"_a, "index_path"_a = nb::none(),
           "Create a DFTracer bytes reader for a gzip file and its index")
      .def("get_max_bytes", &DFTracerBytesReader::get_max_bytes,
           "Get the maximum byte position available in the file")
      .def("get_num_lines", &DFTracerBytesReader::get_num_lines,
           "Get the number of lines in the file")
      .def("iter", &DFTracerBytesReader::iter,
           "step"_a = constants::DEFAULT_STEP_SIZE_BYTES,
           "Get iterator with optional step size")
      .def("__iter__", &DFTracerBytesReader::__iter__,
           nb::rv_policy::reference_internal, "Get iterator for the reader")
      .def("__next__", &DFTracerBytesReader::__next__,
           "Get next chunk with default step")
      .def("set_default_step", &DFTracerBytesReader::set_default_step, "step"_a,
           "Set default step for iteration")
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
      .def(nb::init<const std::string &, const std::optional<std::string> &>(),
           "gzip_path"_a, "index_path"_a = nb::none(),
           "Create a DFTracer line bytes reader for a gzip file and its index")
      .def("get_max_bytes", &DFTracerLineBytesReader::get_max_bytes,
           "Get the maximum byte position available in the file")
      .def("get_num_lines", &DFTracerLineBytesReader::get_num_lines,
           "Get the number of lines in the file")
      .def("iter", &DFTracerLineBytesReader::iter,
           "step"_a = constants::DEFAULT_STEP_SIZE_BYTES,
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
      .def("close", &DFTracerLineBytesReader::close, "Close the index database")
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
      .def(nb::init<const std::string &, const std::optional<std::string> &>(),
           "gzip_path"_a, "index_path"_a = nb::none(),
           "Create a DFTracer lines reader for a gzip file and its index")
      .def("get_max_bytes", &DFTracerLinesReader::get_max_bytes,
           "Get the maximum byte position available in the file")
      .def("get_num_lines", &DFTracerLinesReader::get_num_lines,
           "Get the number of lines in the file")
      .def("iter", &DFTracerLinesReader::iter,
           "step"_a = constants::DEFAULT_STEP_SIZE_LINES,
           "Get iterator with optional step size")
      .def("__iter__", &DFTracerLinesReader::__iter__,
           nb::rv_policy::reference_internal, "Get iterator for the reader")
      .def("__next__", &DFTracerLinesReader::__next__,
           "Get next chunk with default step")
      .def("set_default_step", &DFTracerLinesReader::set_default_step, "step"_a,
           "Set default step for iteration")
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

  // Generic dft_reader_range function with mode parameter
  m.def("dft_reader_range",
        static_cast<DFTracerLineBytesRangeIterator(*)(DFTracerLineBytesReader&, uint64_t, uint64_t, const std::string&, uint64_t)>(&dft_reader_range),
        "reader"_a, "start"_a, "end"_a, "mode"_a = "line_bytes", "step"_a = constants::DEFAULT_STEP_SIZE_BYTES,
        "Create a range iterator with specified mode ('line_bytes', 'bytes', or 'lines')");

  m.def("dft_reader_range",
        static_cast<DFTracerBytesRangeIterator(*)(DFTracerBytesReader&, uint64_t, uint64_t, const std::string&, uint64_t)>(&dft_reader_range),
        "reader"_a, "start"_a, "end"_a, "mode"_a = "bytes", "step"_a = constants::DEFAULT_STEP_SIZE_BYTES,
        "Create a range iterator with specified mode ('line_bytes', 'bytes', or 'lines')");

  m.def("dft_reader_range",
        static_cast<DFTracerLinesRangeIterator(*)(DFTracerLinesReader&, uint64_t, uint64_t, const std::string&, uint64_t)>(&dft_reader_range),
        "reader"_a, "start"_a, "end"_a, "mode"_a = "lines", "step"_a = constants::DEFAULT_STEP_SIZE_LINES,
        "Create a range iterator with specified mode ('line_bytes', 'bytes', or 'lines')");

  // Alias DFTracerLineBytesReader as DFTracerReader for common use
  m.attr("DFTracerReader") = m.attr("DFTracerLineBytesReader");
}
