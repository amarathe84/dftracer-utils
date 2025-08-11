#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/optional.h>

#include <dftracer_utils/reader/reader.h>
#include <dftracer_utils/reader/indexer.h>

#include <sqlite3.h>
#include <string>
#include <optional>
#include <stdexcept>
#include <memory>
#include <algorithm>
#include <cctype>

namespace nb = nanobind;

using namespace nb::literals;

// Utility function to trim trailing whitespace and null characters
std::string trim_trailing(const char* data, size_t size) {
    if (size == 0) return "";
    
    // Find the last non-whitespace, non-null character
    size_t end = size;
    while (end > 0 && (std::isspace(data[end - 1]) || data[end - 1] == '\0')) {
        end--;
    }
    
    return std::string(data, end);
}

class DFTracerReader {
private:
    sqlite3* db_;
    std::string gzip_path_;
    std::string index_path_;
    bool db_opened_;
    
public:
    DFTracerReader(const std::string& gzip_path, const std::optional<std::string>& index_path = std::nullopt)
        : gzip_path_(gzip_path), db_(nullptr), db_opened_(false) {

        if (index_path.has_value()) {
            index_path_ = index_path.value();
        } else {
            index_path_ = gzip_path + ".idx";
        }

        open();
    }

    ~DFTracerReader() {
        close();
    }

    void open() {
        if (db_opened_) {
            return;
        }
        
        if (sqlite3_open(index_path_.c_str(), &db_) != SQLITE_OK) {
            throw std::runtime_error("Failed to open index database: " + std::string(sqlite3_errmsg(db_)));
        }
        db_opened_ = true;
    }

    void close() {
        if (db_opened_ && db_) {
            sqlite3_close(db_);
            db_ = nullptr;
            db_opened_ = false;
        }
    }
    
    std::string read(uint64_t start_bytes, uint64_t end_bytes) {
        if (!db_opened_) {
            throw std::runtime_error("Database is not open");
        }
        
        char* output = nullptr;
        size_t output_size = 0;
        
        int result = read_data_range_bytes(db_, gzip_path_.c_str(), start_bytes, end_bytes, &output, &output_size);
        
        if (result != 0) {
            if (output) {
                free(output);
            }
            throw std::runtime_error("Failed to read data range");
        }
        
        std::string data = trim_trailing(output, output_size);
        free(output);
        return data;
    }

    std::string read_mb(double start_mb, double end_mb) {
        if (!db_opened_) {
            throw std::runtime_error("Database is not open");
        }
        
        char* output = nullptr;
        size_t output_size = 0;
        
        int result = read_data_range_megabytes(db_, gzip_path_.c_str(), start_mb, end_mb, &output, &output_size);
        
        if (result != 0) {
            if (output) {
                free(output);
            }
            throw std::runtime_error("Failed to read data range");
        }
        
        std::string data = trim_trailing(output, output_size);
        free(output);
        return data;
    }

    DFTracerReader& __enter__() {
        return *this;
    }

    bool __exit__(nb::args args) {
        close();
        return false;
    }
    
    // Properties
    std::string gzip_path() const { return gzip_path_; }
    std::string index_path() const { return index_path_; }
    bool is_open() const { return db_opened_; }
};

NB_MODULE(dft_reader_ext, m) {
    m.doc() = "DFTracer utilities reader extension";
    
    nb::class_<DFTracerReader>(m, "DFTracerReader")
        .def(nb::init<const std::string&, const std::optional<std::string>&>(),
             "gzip_path"_a, "index_path"_a = nb::none(),
             "Create a DFTracer reader for a gzip file and its index")
        .def("read", &DFTracerReader::read,
             "start_bytes"_a, "end_bytes"_a,
             "Read a range of bytes from the gzip file")
        .def("read_mb", &DFTracerReader::read_mb,
             "start_mb"_a, "end_mb"_a,
             "Read a range of megabytes from the gzip file")
        .def("open", &DFTracerReader::open,
             "Open the index database")
        .def("close", &DFTracerReader::close,
             "Close the index database")
        .def("__enter__", &DFTracerReader::__enter__, nb::rv_policy::reference_internal,
             "Enter context manager")
        .def("__exit__", &DFTracerReader::__exit__,
             "Exit context manager")
        .def_prop_ro("gzip_path", &DFTracerReader::gzip_path,
                     "Path to the gzip file")
        .def_prop_ro("index_path", &DFTracerReader::index_path,
                     "Path to the index file")
        .def_prop_ro("is_open", &DFTracerReader::is_open,
                     "Whether the database is open");
}
