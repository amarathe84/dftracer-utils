#include <dftracer/utils/utils/json.h>
#include <simdjson.h>

#include <thread>

namespace dftracer {
namespace utils {
namespace json {

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

JsonDocument parse_json(const char* data, size_t size) {
  auto doc = tl_parser.parse(data, size);
  if (doc.error()) {
    // Return null element on error
    return simdjson::dom::element{};
  }

  return doc.value();
}

JsonDocuments parse_json_lines(const char* data, size_t size) {
  JsonDocuments out;

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

      auto doc = tl_parser.parse(start, line_size);

      if (!doc.error()) {
        out.emplace_back(doc.value());
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

std::string get_string_field(const JsonDocument& doc, const std::string& key) {
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

double get_double_field(const JsonDocument& doc, const std::string& key) {
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

uint64_t get_uint64_field(const JsonDocument& doc, const std::string& key) {
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

std::string get_args_string_field(const JsonDocument& doc,
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

}  // namespace json
}  // namespace utils
}  // namespace dftracer
