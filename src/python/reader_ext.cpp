#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/filesystem.h>
#include <dftracer/utils/utils/logger.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace nb = nanobind;

using namespace nb::literals;

constexpr uint64_t DEFAULT_STEP_SIZE = 4 * 1024 * 1024;  // 4MB

std::string trim_trailing(const char *data, size_t size) {
  if (size == 0) return "";

  size_t end = size;
  while (end > 0 && (std::isspace(data[end - 1]) || data[end - 1] == '\0')) {
    end--;
  }

  return std::string(data, end);
}

class DFTracerReader {
 private:
  std::unique_ptr<dftracer::utils::reader::Reader> reader_;
  std::string gzip_path_;
  std::string index_path_;
  bool is_open_;

  uint64_t current_pos_;
  uint64_t max_bytes_;
  uint64_t default_step_;

 public:
  DFTracerReader(const std::string &gzip_path,
                 const std::optional<std::string> &index_path = std::nullopt)
      : gzip_path_(gzip_path),
        reader_(nullptr),
        is_open_(false),
        current_pos_(0),
        max_bytes_(0),
        default_step_(DEFAULT_STEP_SIZE) {
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
      current_pos_ = 0;
      max_bytes_ = 0;
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

  template <bool Raw>
  class Iterator {
   private:
    DFTracerReader *reader_;
    uint64_t current_pos_;
    uint64_t max_bytes_;
    uint64_t step_;

   public:
    Iterator(DFTracerReader *reader, uint64_t step)
        : reader_(reader), current_pos_(0), step_(step) {
      max_bytes_ = reader_->get_max_bytes();
    }

    Iterator begin() {
      current_pos_ = 0;
      return *this;
    }

    Iterator end() {
      Iterator end_iter = *this;
      end_iter.current_pos_ = max_bytes_;
      return end_iter;
    }

    bool operator!=(const Iterator &other) const {
      return current_pos_ < other.current_pos_;
    }

    std::string operator*() {
      uint64_t end_pos = std::min(current_pos_ + step_, max_bytes_);
      return Raw ? reader_->read(current_pos_, end_pos)
                 : reader_->read_line_bytes(current_pos_, end_pos);
    }

    Iterator &operator++() {
      current_pos_ = std::min(current_pos_ + step_, max_bytes_);
      return *this;
    }

    Iterator iter() {
      current_pos_ = 0;
      return *this;
    }

    std::string next() {
      if (current_pos_ >= max_bytes_) {
        throw nb::stop_iteration();
      }

      uint64_t end_pos = std::min(current_pos_ + step_, max_bytes_);
      std::string result =
          Raw ? reader_->read(current_pos_, end_pos)
              : reader_->read_line_bytes(current_pos_, end_pos);
      current_pos_ = end_pos;
      return result;
    }
  };

  using LineIterator = Iterator<false>;
  using RawIterator = Iterator<true>;

  LineIterator iter(uint64_t step_bytes = DEFAULT_STEP_SIZE) {
    ensure_open();
    validate_step(step_bytes);
    return LineIterator(this, step_bytes);
  }

  RawIterator raw_iter(uint64_t step_bytes = DEFAULT_STEP_SIZE) {
    ensure_open();
    validate_step(step_bytes);
    return RawIterator(this, step_bytes);
  }

  DFTracerReader &__iter__() {
    ensure_open();
    current_pos_ = 0;
    return *this;
  }

  std::string __next__() {
    ensure_open();
    if (current_pos_ >= max_bytes_) {
      throw nb::stop_iteration();
    }

    uint64_t end_pos = std::min(current_pos_ + default_step_, max_bytes_);
    std::string result = read(current_pos_, end_pos);
    current_pos_ = end_pos;
    return result;
  }

  void set_default_step(uint64_t step_bytes) {
    validate_step(step_bytes);
    default_step_ = step_bytes;
  }

  uint64_t get_default_step() const { return default_step_; }

  std::string read_line_bytes(uint64_t start_bytes, uint64_t end_bytes) {
    return read_internal(start_bytes, end_bytes, false);
  }

  std::string read(uint64_t start_bytes, uint64_t end_bytes) {
    return read_internal(start_bytes, end_bytes, true);
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

  std::string read_internal(uint64_t start_bytes, uint64_t end_bytes,
                            bool raw) {
    ensure_open();

    try {
      std::string result;
      const size_t buffer_size = 64 * 1024;  // 64KB buffer
      std::vector<char> buffer(buffer_size);

      size_t bytes_read;
      while ((bytes_read = raw ? reader_->read(start_bytes, end_bytes,
                                               buffer.data(), buffer.size())
                               : reader_->read_line_bytes(
                                     start_bytes, end_bytes, buffer.data(),
                                     buffer.size())) > 0) {
        result.append(buffer.data(), bytes_read);
      }

      return result;
    } catch (const std::runtime_error &e) {
      const std::string operation = raw ? "raw data" : "data";
      throw std::runtime_error("Failed to read " + operation +
                               " range: " + std::string(e.what()));
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

template <bool Raw>
class DFTracerRangeIterator {
 private:
  DFTracerReader *reader_;
  uint64_t start_pos_;
  uint64_t end_pos_;
  uint64_t current_pos_;
  uint64_t step_;

 public:
  DFTracerRangeIterator(DFTracerReader *reader, uint64_t start, uint64_t end,
                        uint64_t step)
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

    uint64_t max_bytes = reader_->get_max_bytes();
    if (end_pos_ > max_bytes) {
      end_pos_ = max_bytes;
    }
    if (start_pos_ >= max_bytes) {
      throw std::invalid_argument("Start position exceeds file size");
    }
  }

  DFTracerRangeIterator &__iter__() {
    current_pos_ = start_pos_;
    return *this;
  }

  std::string __next__() {
    if (current_pos_ >= end_pos_) {
      throw nb::stop_iteration();
    }

    uint64_t chunk_end = std::min(current_pos_ + step_, end_pos_);
    std::string result =
        Raw ? reader_->read(current_pos_, chunk_end)
            : reader_->read_line_bytes(current_pos_, chunk_end);
    current_pos_ = chunk_end;
    return result;
  }

  uint64_t get_start() const { return start_pos_; }
  uint64_t get_end() const { return end_pos_; }
  uint64_t get_step() const { return step_; }
  uint64_t get_current() const { return current_pos_; }
};

using DFTracerLineRangeIterator = DFTracerRangeIterator<false>;
using DFTracerRawRangeIterator = DFTracerRangeIterator<true>;

DFTracerLineRangeIterator dft_reader_range(DFTracerReader &reader,
                                           uint64_t start, uint64_t end,
                                           uint64_t step = DEFAULT_STEP_SIZE) {
  return DFTracerLineRangeIterator(&reader, start, end, step);
}

DFTracerRawRangeIterator dft_reader_raw_range(
    DFTracerReader &reader, uint64_t start, uint64_t end,
    uint64_t step = DEFAULT_STEP_SIZE) {
  return DFTracerRawRangeIterator(&reader, start, end, step);
}

NB_MODULE(reader_ext, m) {
  m.doc() = "DFTracer utilities reader extension";

  nb::class_<DFTracerReader::LineIterator>(m, "LineIterator")
      .def("__iter__", &DFTracerReader::LineIterator::iter,
           "Get line-aware iterator")
      .def("__next__", &DFTracerReader::LineIterator::next,
           "Get next chunk with line processing");

  nb::class_<DFTracerReader::RawIterator>(m, "RawIterator")
      .def("__iter__", &DFTracerReader::RawIterator::iter, "Get raw iterator")
      .def("__next__", &DFTracerReader::RawIterator::next,
           "Get next raw chunk without processing");

  nb::class_<DFTracerLineRangeIterator>(m, "DFTracerLineRangeIterator")
      .def("__iter__", &DFTracerLineRangeIterator::__iter__,
           nb::rv_policy::reference_internal, "Get iterator")
      .def("__next__", &DFTracerLineRangeIterator::__next__,
           "Get next chunk with line processing")
      .def_prop_ro("start", &DFTracerLineRangeIterator::get_start,
                   "Start position")
      .def_prop_ro("end", &DFTracerLineRangeIterator::get_end, "End position")
      .def_prop_ro("step", &DFTracerLineRangeIterator::get_step, "Step size")
      .def_prop_ro("current", &DFTracerLineRangeIterator::get_current,
                   "Current position");

  nb::class_<DFTracerRawRangeIterator>(m, "DFTracerRawRangeIterator")
      .def("__iter__", &DFTracerRawRangeIterator::__iter__,
           nb::rv_policy::reference_internal, "Get iterator")
      .def("__next__", &DFTracerRawRangeIterator::__next__,
           "Get next raw chunk without processing")
      .def_prop_ro("start", &DFTracerRawRangeIterator::get_start,
                   "Start position")
      .def_prop_ro("end", &DFTracerRawRangeIterator::get_end, "End position")
      .def_prop_ro("step", &DFTracerRawRangeIterator::get_step, "Step size")
      .def_prop_ro("current", &DFTracerRawRangeIterator::get_current,
                   "Current position");

  nb::class_<DFTracerReader>(m, "DFTracerReader")
      .def(nb::init<const std::string &, const std::optional<std::string> &>(),
           "gzip_path"_a, "index_path"_a = nb::none(),
           "Create a DFTracer reader for a gzip file and its index")
      .def("get_max_bytes", &DFTracerReader::get_max_bytes,
           "Get the maximum byte position available in the file")
      .def("iter", &DFTracerReader::iter, "step_bytes"_a = DEFAULT_STEP_SIZE,
           "Get line-aware iterator with optional step in bytes")
      .def("raw_iter", &DFTracerReader::raw_iter,
           "step_bytes"_a = DEFAULT_STEP_SIZE,
           "Get raw iterator with optional step in bytes")
      .def("__iter__", &DFTracerReader::__iter__,
           nb::rv_policy::reference_internal, "Get iterator for the reader")
      .def("__next__", &DFTracerReader::__next__,
           "Get next chunk with default step")
      .def("set_default_step", &DFTracerReader::set_default_step,
           "step_bytes"_a, "Set default step for iteration")
      .def("get_default_step", &DFTracerReader::get_default_step,
           "Get current default step")
      .def("read", &DFTracerReader::read, "start_bytes"_a, "end_bytes"_a,
           "Read a range of bytes from the gzip file")
      .def("read_line_bytes", &DFTracerReader::read_line_bytes, "start_bytes"_a,
           "end_bytes"_a, "Read line-aligned bytes from the gzip file")
      .def("open", &DFTracerReader::open, "Open the index database")
      .def("close", &DFTracerReader::close, "Close the index database")
      .def("__enter__", &DFTracerReader::__enter__,
           nb::rv_policy::reference_internal, "Enter context manager")
      .def("__exit__", &DFTracerReader::__exit__, "Exit context manager")
      .def_prop_ro("gzip_path", &DFTracerReader::gzip_path,
                   "Path to the gzip file")
      .def_prop_ro("index_path", &DFTracerReader::index_path,
                   "Path to the index file")
      .def_prop_ro("is_open", &DFTracerReader::is_open,
                   "Whether the database is open");

  m.def("dft_reader_range", &dft_reader_range, "reader"_a, "start"_a, "end"_a,
        "step"_a = DEFAULT_STEP_SIZE,
        "Create a line-aware range iterator for reading specific byte ranges "
        "with optional step size");

  m.def("dft_reader_raw_range", &dft_reader_raw_range, "reader"_a, "start"_a,
        "end"_a, "step"_a = DEFAULT_STEP_SIZE,
        "Create a raw range iterator for reading specific byte ranges with "
        "optional step size");

  m.def("set_log_level", &dftracer::utils::utils::set_log_level, "level"_a,
        "Set the global log level using a string (trace, debug, info, warn, "
        "error, critical, off)");

  m.def("set_log_level_int", &dftracer::utils::utils::set_log_level_int,
        "level"_a,
        "Set the global log level using an integer (0=trace, 1=debug, 2=info, "
        "3=warn, 4=error, 5=critical, 6=off)");

  m.def("get_log_level_string", &dftracer::utils::utils::get_log_level_string,
        "Get the current global log level as a string");

  m.def("get_log_level_int", &dftracer::utils::utils::get_log_level_int,
        "Get the current global log level as an integer");
}
