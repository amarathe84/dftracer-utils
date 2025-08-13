#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>

#include <dft_utils/indexer/indexer.h>
#include <dft_utils/reader/reader.h>
#include <dft_utils/utils/logger.h>
#include <dft_utils/utils/filesystem.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace nb = nanobind;

using namespace nb::literals;

std::string trim_trailing(const char *data, size_t size)
{
    if (size == 0)
        return "";

    size_t end = size;
    while (end > 0 && (std::isspace(data[end - 1]) || data[end - 1] == '\0'))
    {
        end--;
    }

    return std::string(data, end);
}

class DFTracerReader
{
  private:
    std::unique_ptr<dft::reader::Reader> reader_;
    std::string gzip_path_;
    std::string index_path_;
    bool is_open_;

    uint64_t current_pos_;
    uint64_t max_bytes_;
    uint64_t default_step_;

  public:
    DFTracerReader(const std::string &gzip_path, const std::optional<std::string> &index_path = std::nullopt)
        : gzip_path_(gzip_path), reader_(nullptr), is_open_(false), current_pos_(0), max_bytes_(0),
          default_step_(1024 * 1024)
    {

        if (index_path.has_value())
        {
            index_path_ = index_path.value();
        }
        else
        {
            index_path_ = gzip_path + ".idx";
        }

        open();
    }

    ~DFTracerReader()
    {
        close();
    }

    void open()
    {
        if (is_open_)
        {
            return;
        }

        if (!fs::exists(gzip_path_)) {
          throw std::runtime_error("Gzip file does not exist: " + gzip_path_);
        }

        try {
            reader_ = std::make_unique<dft::reader::Reader>(gzip_path_, index_path_);
            is_open_ = true;
            max_bytes_ = static_cast<uint64_t>(reader_->get_max_bytes());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error("Failed to create DFT reader for gzip: " + gzip_path_ + " and index: " + index_path_ + " - " + e.what());
        }
    }

    void close()
    {
        if (is_open_ && reader_)
        {
            reader_.reset();
            is_open_ = false;
            current_pos_ = 0;
            max_bytes_ = 0;
        }
    }

    uint64_t get_max_bytes()
    {
        if (!is_open_)
        {
            throw std::runtime_error("Reader is not open");
        }

        try {
            return static_cast<uint64_t>(reader_->get_max_bytes());
        } catch (const std::runtime_error& e) {
            throw std::runtime_error("Failed to get maximum bytes: " + std::string(e.what()));
        }
    }

    class ByteIterator
    {
      private:
        DFTracerReader *reader_;
        uint64_t current_pos_;
        uint64_t max_bytes_;
        uint64_t step_;

      public:
        ByteIterator(DFTracerReader *reader, uint64_t step) : reader_(reader), current_pos_(0), step_(step)
        {
            max_bytes_ = reader_->get_max_bytes();
        }

        ByteIterator begin()
        {
            current_pos_ = 0;
            return *this;
        }

        ByteIterator end()
        {
            ByteIterator end_iter = *this;
            end_iter.current_pos_ = max_bytes_;
            return end_iter;
        }

        bool operator!=(const ByteIterator &other) const
        {
            return current_pos_ < other.current_pos_;
        }

        std::string operator*()
        {
            uint64_t end_pos = std::min(current_pos_ + step_, max_bytes_);
            return reader_->read(current_pos_, end_pos);
        }

        ByteIterator &operator++()
        {
            current_pos_ = std::min(current_pos_ + step_, max_bytes_);
            return *this;
        }

        ByteIterator iter()
        {
            current_pos_ = 0;
            return *this;
        }

        std::string next()
        {
            if (current_pos_ >= max_bytes_)
            {
                throw nb::stop_iteration();
            }

            uint64_t end_pos = std::min(current_pos_ + step_, max_bytes_);

            // Read whatever is available, even if it's less than the step
            std::string result = reader_->read(current_pos_, end_pos);
            current_pos_ = end_pos;
            return result;
        }
    };

    ByteIterator iterator()
    {
        if (!is_open_)
        {
            throw std::runtime_error("Reader is not open");
        }
        return ByteIterator(this, 1024 * 1024);
    }

    ByteIterator iter(uint64_t step_bytes)
    {
        if (!is_open_)
        {
            throw std::runtime_error("Reader is not open");
        }
        if (step_bytes == 0)
        {
            throw std::invalid_argument("step must be greater than 0");
        }
        return ByteIterator(this, step_bytes);
    }

    DFTracerReader &__iter__()
    {
        if (!is_open_)
        {
            throw std::runtime_error("Reader is not open");
        }
        current_pos_ = 0;
        return *this;
    }

    std::string __next__()
    {
        if (!is_open_)
        {
            throw std::runtime_error("Reader is not open");
        }

        if (current_pos_ >= max_bytes_)
        {
            throw nb::stop_iteration();
        }

        uint64_t end_pos = std::min(current_pos_ + default_step_, max_bytes_);

        if (end_pos == max_bytes_ && current_pos_ < max_bytes_)
        {
            std::string result = read(current_pos_, end_pos);
            current_pos_ = end_pos;
            return result;
        }

        std::string result = read(current_pos_, end_pos);
        current_pos_ = end_pos;
        return result;
    }

    void set_default_step(uint64_t step_bytes)
    {
        if (step_bytes == 0)
        {
            throw std::invalid_argument("Step must be greater than 0");
        }
        default_step_ = step_bytes;
    }

    uint64_t get_default_step() const
    {
        return default_step_;
    }

    std::string read(uint64_t start_bytes, uint64_t end_bytes)
    {
        if (!is_open_)
        {
            throw std::runtime_error("Reader is not open");
        }

        try {
            std::string result;
            const size_t buffer_size = 64 * 1024; // 64KB buffer
            std::vector<char> buffer(buffer_size);
            
            // Stream data until no more available
            while (true)
            {
                size_t bytes_read = reader_->read(start_bytes, end_bytes, buffer.data(), buffer.size());
                
                if (bytes_read == 0)
                {
                    break; // No more data available
                }
                
                result.append(buffer.data(), bytes_read);
            }
            
            return result;
        } catch (const std::runtime_error& e) {
            throw std::runtime_error("Failed to read data range: " + std::string(e.what()));
        }
    }


    DFTracerReader &__enter__()
    {
        return *this;
    }

    bool __exit__(nb::args args)
    {
        close();
        return false;
    }

    std::string gzip_path() const
    {
        return gzip_path_;
    }

    std::string index_path() const
    {
        return index_path_;
    }

    bool is_open() const
    {
        return is_open_;
    }
};

class DFTracerRangeIterator
{
  private:
    DFTracerReader *reader_;
    uint64_t start_pos_;
    uint64_t end_pos_;
    uint64_t current_pos_;
    uint64_t step_;

  public:
    DFTracerRangeIterator(DFTracerReader *reader, uint64_t start, uint64_t end, uint64_t step)
        : reader_(reader), start_pos_(start), end_pos_(end), current_pos_(start), step_(step)
    {
        if (!reader_)
        {
            throw std::invalid_argument("Reader cannot be null");
        }
        if (step == 0)
        {
            throw std::invalid_argument("Step must be greater than 0");
        }
        if (start >= end)
        {
            throw std::invalid_argument("Start position must be less than end position");
        }

        uint64_t max_bytes = reader_->get_max_bytes();
        if (end_pos_ > max_bytes)
        {
            end_pos_ = max_bytes;
        }
        if (start_pos_ >= max_bytes)
        {
            throw std::invalid_argument("Start position exceeds file size");
        }
    }

    DFTracerRangeIterator &__iter__()
    {
        current_pos_ = start_pos_;
        return *this;
    }

    std::string __next__()
    {
        if (current_pos_ >= end_pos_)
        {
            throw nb::stop_iteration();
        }

        uint64_t chunk_end = std::min(current_pos_ + step_, end_pos_);
        std::string result = reader_->read(current_pos_, chunk_end);
        current_pos_ = chunk_end;
        return result;
    }

    uint64_t get_start() const
    {
        return start_pos_;
    }
    uint64_t get_end() const
    {
        return end_pos_;
    }
    uint64_t get_step() const
    {
        return step_;
    }
    uint64_t get_current() const
    {
        return current_pos_;
    }
};

DFTracerRangeIterator
dft_reader_range(DFTracerReader &reader, uint64_t start, uint64_t end, uint64_t step = 1024 * 1024)
{
    return DFTracerRangeIterator(&reader, start, end, step);
}

NB_MODULE(dft_utils_reader_ext, m)
{
    m.doc() = "DFTracer utilities reader extension";

    nb::class_<DFTracerReader::ByteIterator>(m, "ByteIterator")
        .def("__iter__", &DFTracerReader::ByteIterator::iter, "Get iterator")
        .def("__next__", &DFTracerReader::ByteIterator::next, "Get next chunk");

    nb::class_<DFTracerRangeIterator>(m, "DFTracerRangeIterator")
        .def("__iter__", &DFTracerRangeIterator::__iter__, nb::rv_policy::reference_internal, "Get iterator")
        .def("__next__", &DFTracerRangeIterator::__next__, "Get next chunk")
        .def_prop_ro("start", &DFTracerRangeIterator::get_start, "Start position")
        .def_prop_ro("end", &DFTracerRangeIterator::get_end, "End position")
        .def_prop_ro("step", &DFTracerRangeIterator::get_step, "Step size")
        .def_prop_ro("current", &DFTracerRangeIterator::get_current, "Current position");

    nb::class_<DFTracerReader>(m, "DFTracerReader")
        .def(nb::init<const std::string &, const std::optional<std::string> &>(),
             "gzip_path"_a,
             "index_path"_a = nb::none(),
             "Create a DFTracer reader for a gzip file and its index")
        .def("get_max_bytes", &DFTracerReader::get_max_bytes, "Get the maximum byte position available in the file")
        .def("iterator", &DFTracerReader::iterator, "Get iterator with default 1MB step")
        .def("iter", &DFTracerReader::iter, "step_bytes"_a, "Get iterator with custom step in bytes")
        .def("__iter__", &DFTracerReader::__iter__, nb::rv_policy::reference_internal, "Get iterator for the reader")
        .def("__next__", &DFTracerReader::__next__, "Get next chunk with default step")
        .def("set_default_step", &DFTracerReader::set_default_step, "step_bytes"_a, "Set default step for iteration")
        .def("get_default_step", &DFTracerReader::get_default_step, "Get current default step")
        .def("read", &DFTracerReader::read, "start_bytes"_a, "end_bytes"_a, "Read a range of bytes from the gzip file")
        .def("open", &DFTracerReader::open, "Open the index database")
        .def("close", &DFTracerReader::close, "Close the index database")
        .def("__enter__", &DFTracerReader::__enter__, nb::rv_policy::reference_internal, "Enter context manager")
        .def("__exit__", &DFTracerReader::__exit__, "Exit context manager")
        .def_prop_ro("gzip_path", &DFTracerReader::gzip_path, "Path to the gzip file")
        .def_prop_ro("index_path", &DFTracerReader::index_path, "Path to the index file")
        .def_prop_ro("is_open", &DFTracerReader::is_open, "Whether the database is open");

    m.def("dft_reader_range",
          &dft_reader_range,
          "reader"_a,
          "start"_a,
          "end"_a,
          "step"_a = 1024 * 1024,
          "Create a range iterator for reading specific byte ranges with optional step size");

    m.def("set_log_level",
          &dft::utils::set_log_level,
          "level"_a,
          "Set the global log level using a string (trace, debug, info, warn, error, critical, off)");

    m.def("set_log_level_int",
          &dft::utils::set_log_level_int,
          "level"_a,
          "Set the global log level using an integer (0=trace, 1=debug, 2=info, 3=warn, 4=error, 5=critical, 6=off)");

    m.def("get_log_level_string", &dft::utils::get_log_level_string, "Get the current global log level as a string");

    m.def("get_log_level_int", &dft::utils::get_log_level_int, "Get the current global log level as an integer");
}
