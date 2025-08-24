#include <dftracer/utils/utils/json.h>
#include <simdjson.h>

#include <thread>

namespace dftracer::utils::json {

namespace {

size_t find_last_newline(const char* data, size_t n) {
  for (size_t i = n; i > 0; --i) {
    char c = data[i - 1];
    if (c == '\n' || c == '\r') return i;
  }
  return 0;
}

}  // anonymous namespace

// Thread-local DOM parser for better performance
thread_local simdjson::dom::parser tl_parser;

// ========================================
// OwnedJsonDocument Implementation
// ========================================

void OwnedJsonDocument::ensure_parsed() const {
  if (!parsed_) {
    auto result = tl_parser.parse(data_.data(), data_.size());
    if (!result.error()) {
      element_ = result.value();
      parsed_ = true;
    }
  }
}

// Constructors
OwnedJsonDocument::OwnedJsonDocument() : parsed_(false) {}

OwnedJsonDocument::OwnedJsonDocument(std::string json_data)
    : data_(std::move(json_data)), parsed_(false) {}

OwnedJsonDocument::OwnedJsonDocument(const char* json_data, size_t size)
    : data_(json_data, size), parsed_(false) {}

OwnedJsonDocument::OwnedJsonDocument(const simdjson::dom::element& element)
    : element_(element), parsed_(true) {
  data_ = simdjson::minify(element);
}

// Copy constructor
OwnedJsonDocument::OwnedJsonDocument(const OwnedJsonDocument& other)
    : data_(other.data_), parsed_(false) {}

// Move constructor
OwnedJsonDocument::OwnedJsonDocument(OwnedJsonDocument&& other) noexcept
    : data_(std::move(other.data_)), parsed_(false) {}

// Assignment operators
OwnedJsonDocument& OwnedJsonDocument::operator=(
    const OwnedJsonDocument& other) {
  if (this != &other) {
    data_ = other.data_;
    parsed_ = false;
  }
  return *this;
}

OwnedJsonDocument& OwnedJsonDocument::operator=(
    OwnedJsonDocument&& other) noexcept {
  if (this != &other) {
    data_ = std::move(other.data_);
    parsed_ = false;
  }
  return *this;
}

// Type checking methods
bool OwnedJsonDocument::is_object() const {
  ensure_parsed();
  return parsed_ && element_.is_object();
}

bool OwnedJsonDocument::is_array() const {
  ensure_parsed();
  return parsed_ && element_.is_array();
}

bool OwnedJsonDocument::is_string() const {
  ensure_parsed();
  return parsed_ && element_.is_string();
}

bool OwnedJsonDocument::is_int64() const {
  ensure_parsed();
  return parsed_ && element_.is_int64();
}

bool OwnedJsonDocument::is_uint64() const {
  ensure_parsed();
  return parsed_ && element_.is_uint64();
}

bool OwnedJsonDocument::is_double() const {
  ensure_parsed();
  return parsed_ && element_.is_double();
}

bool OwnedJsonDocument::is_bool() const {
  ensure_parsed();
  return parsed_ && element_.is_bool();
}

bool OwnedJsonDocument::is_null() const {
  ensure_parsed();
  return parsed_ && element_.is_null();
}

simdjson::dom::element_type OwnedJsonDocument::type() const {
  return element_.type();
}

// Value extraction methods
simdjson::simdjson_result<simdjson::dom::object> OwnedJsonDocument::get_object()
    const {
  ensure_parsed();
  return element_.get_object();
}

simdjson::simdjson_result<simdjson::dom::array> OwnedJsonDocument::get_array()
    const {
  ensure_parsed();
  return element_.get_array();
}

simdjson::simdjson_result<std::string_view> OwnedJsonDocument::get_string()
    const {
  ensure_parsed();
  return element_.get_string();
}

simdjson::simdjson_result<int64_t> OwnedJsonDocument::get_int64() const {
  ensure_parsed();
  return element_.get_int64();
}

simdjson::simdjson_result<uint64_t> OwnedJsonDocument::get_uint64() const {
  ensure_parsed();
  return element_.get_uint64();
}

simdjson::simdjson_result<double> OwnedJsonDocument::get_double() const {
  ensure_parsed();
  return element_.get_double();
}

simdjson::simdjson_result<bool> OwnedJsonDocument::get_bool() const {
  ensure_parsed();
  return element_.get_bool();
}

// Utility methods
bool OwnedJsonDocument::is_valid() const {
  ensure_parsed();
  return parsed_;
}

const std::string& OwnedJsonDocument::raw_data() const { return data_; }

std::string OwnedJsonDocument::minify() const {
  ensure_parsed();
  if (parsed_) {
    return simdjson::minify(element_);
  }
  return data_;  // Return raw data if parsing failed
}

std::ostream& operator<<(std::ostream& os, const JsonDocument& doc) {
  os << simdjson::minify(doc);
  return os;
}

std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs) {
  for (size_t i = 0; i < docs.size(); ++i) {
    if (i > 0) os << "\n";
    dftracer::utils::json::operator<<(os, docs[i]);
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const OwnedJsonDocument& doc) {
  os << doc.minify();
  return os;
}

std::ostream& operator<<(std::ostream& os, const OwnedJsonDocuments& docs) {
  for (size_t i = 0; i < docs.size(); ++i) {
    if (i > 0) os << "\n";
    dftracer::utils::json::operator<<(os, docs[i]);
  }
  return os;
}

template <typename JsonDocs>
JsonDocs _parse_json_lines_impl(const char* data, size_t size) {
  JsonDocs out;

  // Quick estimation for better memory allocation
  size_t estimated_lines = size / 80 + 16;
  out.reserve(estimated_lines);

  // Trim trailing partial line
  size_t parse_len = find_last_newline(data, size);
  if (parse_len == 0) parse_len = size;

  const char* start = data;
  const char* end = data + parse_len;

  while (start < end) {
    const char* line_end = start;
    while (line_end < end && *line_end != '\n' && *line_end != '\r') {
      ++line_end;
    }

    if (line_end > start) {
      size_t line_size = static_cast<size_t>(line_end - start);

      if constexpr (std::is_same_v<JsonDocs, OwnedJsonDocuments>) {
        out.emplace_back(start, line_size);
      } else {
        auto el = tl_parser.parse(start, line_size);
        if (!el.error()) {
          out.emplace_back(std::move(el.value()));
        }
      }
    }

    // Skip newline characters
    while (line_end < end && (*line_end == '\n' || *line_end == '\r')) {
      ++line_end;
    }
    start = line_end;
  }

  return out;
}

JsonDocuments parse_json_lines(const char* data, size_t size) {
  return _parse_json_lines_impl<JsonDocuments>(data, size);
}

OwnedJsonDocuments parse_json_lines_owned(const char* data, size_t size) {
  return _parse_json_lines_impl<OwnedJsonDocuments>(data, size);
}

JsonDocument parse_json(const char* data, size_t size) {
  auto doc = tl_parser.parse(data, size);
  if (doc.error()) {
    return JsonDocument();
  }
  return doc.value();
}

OwnedJsonDocument parse_json_owned(const char* data, size_t size) {
  return OwnedJsonDocument(data, size);
}

template <typename JsonDoc>
std::string _get_string_field_impl(const JsonDoc& doc, const std::string& key) {
  if (!doc.is_object()) return "";

  auto obj_result = doc.get_object();
  if (obj_result.error()) return "";

  auto obj = obj_result.value();
  for (auto field : obj) {
    std::string field_key = std::string(field.key);
    if (field_key == key) {
      if (field.value.is_string()) {
        auto str_result = field.value.get_string();
        if (!str_result.error()) {
          return std::string(str_result.value());
        }
      }
    }
  }
  return "";
}

template <typename JsonDoc>
double _get_double_field_impl(const JsonDoc& doc, const std::string& key) {
  if (!doc.is_object()) return 0.0;

  auto obj_result = doc.get_object();
  if (obj_result.error()) return 0.0;

  auto obj = obj_result.value();
  for (auto field : obj) {
    std::string field_key = std::string(field.key);
    if (field_key == key) {
      if (field.value.is_double()) {
        auto val_result = field.value.get_double();
        if (!val_result.error()) {
          return val_result.value();
        }
      } else if (field.value.is_int64()) {
        auto val_result = field.value.get_int64();
        if (!val_result.error()) {
          return static_cast<double>(val_result.value());
        }
      } else if (field.value.is_uint64()) {
        auto val_result = field.value.get_uint64();
        if (!val_result.error()) {
          return static_cast<double>(val_result.value());
        }
      } else if (field.value.is_string()) {
        auto str_result = field.value.get_string();
        if (!str_result.error()) {
          try {
            return std::stod(std::string(str_result.value()));
          } catch (...) {
            // Invalid number string, return 0.0
          }
        }
      }
    }
  }
  return 0.0;
}

template <typename JsonDoc>
uint64_t _get_uint64_field_impl(const JsonDoc& doc, const std::string& key) {
  if (!doc.is_object()) return 0;

  auto obj_result = doc.get_object();
  if (obj_result.error()) return 0;

  auto obj = obj_result.value();
  for (auto field : obj) {
    std::string field_key = std::string(field.key);
    if (field_key == key) {
      if (field.value.is_uint64()) {
        auto val_result = field.value.get_uint64();
        if (!val_result.error()) {
          return val_result.value();
        }
      } else if (field.value.is_int64()) {
        auto val_result = field.value.get_int64();
        if (!val_result.error()) {
          return static_cast<uint64_t>(val_result.value());
        }
      } else if (field.value.is_double()) {
        auto val_result = field.value.get_double();
        if (!val_result.error()) {
          return static_cast<uint64_t>(val_result.value());
        }
      } else if (field.value.is_string()) {
        auto str_result = field.value.get_string();
        if (!str_result.error()) {
          try {
            return std::stoull(std::string(str_result.value()));
          } catch (...) {
            // Invalid number string, return 0
          }
        }
      }
    }
  }
  return 0;
}

template <typename JsonDoc>
std::string _get_args_string_field_impl(const JsonDoc& doc,
                                        const std::string& key) {
  if (!doc.is_object()) return "";

  auto obj_result = doc.get_object();
  if (obj_result.error()) return "";

  auto obj = obj_result.value();
  for (auto field : obj) {
    std::string field_key = std::string(field.key);
    if (field_key == "args" && field.value.is_object()) {
      auto args_result = field.value.get_object();
      if (!args_result.error()) {
        auto args = args_result.value();
        for (auto arg_field : args) {
          std::string arg_key = std::string(arg_field.key);
          if (arg_key == key && arg_field.value.is_string()) {
            auto str_result = arg_field.value.get_string();
            if (!str_result.error()) {
              return std::string(str_result.value());
            }
          }
        }
      }
    }
  }
  return "";
}

std::string get_string_field(const JsonDocument& doc, const std::string& key) {
  return _get_string_field_impl(doc, key);
}

double get_double_field(const JsonDocument& doc, const std::string& key) {
  return _get_double_field_impl(doc, key);
}

uint64_t get_uint64_field(const JsonDocument& doc, const std::string& key) {
  return _get_uint64_field_impl(doc, key);
}

std::string get_args_string_field(const JsonDocument& doc,
                                  const std::string& key) {
  return _get_args_string_field_impl(doc, key);
}

std::string get_string_field_owned(const OwnedJsonDocument& doc,
                                   const std::string& key) {
  return _get_string_field_impl(doc, key);
}

double get_double_field_owned(const OwnedJsonDocument& doc,
                              const std::string& key) {
  return _get_double_field_impl(doc, key);
}

uint64_t get_uint64_field_owned(const OwnedJsonDocument& doc,
                                const std::string& key) {
  return _get_uint64_field_impl(doc, key);
}

std::string get_args_string_field_owned(const OwnedJsonDocument& doc,
                                        const std::string& key) {
  return _get_args_string_field_impl(doc, key);
}

}  // namespace dftracer::utils::json
