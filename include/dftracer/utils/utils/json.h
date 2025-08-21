#ifndef __DFTRACER_UTILS_UTILS_DFT_EVENTS_H
#define __DFTRACER_UTILS_UTILS_DFT_EVENTS_H

#include <simdjson.h>

#include <iosfwd>
#include <string>
#include <unordered_map>
#include <vector>

#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ranges.h>

namespace dftracer {
namespace utils {
namespace json {

class OwnedJsonDocument {
private:
    mutable std::string data_;
    mutable simdjson::dom::element element_;
    mutable bool parsed_;

    void ensure_parsed() const;

public:
    // Constructors
    OwnedJsonDocument();
    OwnedJsonDocument(std::string json_data);
    OwnedJsonDocument(const char* json_data, size_t size);
    OwnedJsonDocument(const simdjson::dom::element& element);
    
    // Copy/Move constructors
    OwnedJsonDocument(const OwnedJsonDocument& other);
    OwnedJsonDocument(OwnedJsonDocument&& other) noexcept;
    
    // Assignment operators
    OwnedJsonDocument& operator=(const OwnedJsonDocument& other);
    OwnedJsonDocument& operator=(OwnedJsonDocument&& other) noexcept;

    // API compatible with simdjson::dom::element
    bool is_object() const;
    bool is_array() const;
    bool is_string() const;
    bool is_int64() const;
    bool is_uint64() const;
    bool is_double() const;
    bool is_bool() const;
    bool is_null() const;

    simdjson::dom::element_type type() const;

    simdjson::simdjson_result<simdjson::dom::object> get_object() const;
    simdjson::simdjson_result<simdjson::dom::array> get_array() const;
    simdjson::simdjson_result<std::string_view> get_string() const;
    simdjson::simdjson_result<int64_t> get_int64() const;
    simdjson::simdjson_result<uint64_t> get_uint64() const;
    simdjson::simdjson_result<double> get_double() const;
    simdjson::simdjson_result<bool> get_bool() const;

    // operator

    // inline operator const simdjson::dom::element&() const {
    //   ensure_parsed();
    //   return element_;
    // }

    // Additional utility methods
    bool is_valid() const;
    const std::string& raw_data() const;
    std::string minify() const;
};

using JsonDocument = simdjson::dom::element;
using JsonDocuments = std::vector<JsonDocument>;

// using JsonDocument = OwnedJsonDocument;
// using JsonDocuments = std::vector<JsonDocument>;

using OwnedJsonDocuments = std::vector<OwnedJsonDocument>;

JsonDocument parse_json(const char* data, size_t size);
OwnedJsonDocument parse_json_owned(const char* data, size_t size);

JsonDocuments parse_json_lines(const char* data, size_t size);
OwnedJsonDocuments parse_json_lines_owned(const char* data, size_t size);

std::string get_string_field(const JsonDocument& doc, const std::string& key);
double get_double_field(const JsonDocument& doc, const std::string& key);
uint64_t get_uint64_field(const JsonDocument& doc, const std::string& key);
std::string get_args_string_field(const JsonDocument& doc,
                                  const std::string& key);

std::string get_string_field_owned(const OwnedJsonDocument& doc, const std::string& key);
double get_double_field_owned(const OwnedJsonDocument& doc, const std::string& key);
uint64_t get_uint64_field_owned(const OwnedJsonDocument& doc, const std::string& key);
std::string get_args_string_field_owned(const OwnedJsonDocument& doc, const std::string& key);


std::ostream& operator<<(std::ostream& os, const JsonDocument& doc);
std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs);

}  // namespace json
}  // namespace utils
}  // namespace dftracer


template <>
struct fmt::formatter<dftracer::utils::json::JsonDocument> : fmt::formatter<std::string> {
  auto format(const dftracer::utils::json::JsonDocument& doc, fmt::format_context& ctx) {
    std::string s = simdjson::minify(doc);
    return fmt::formatter<std::string>::format(s, ctx);
  }
};

template <>
struct fmt::formatter<dftracer::utils::json::JsonDocuments> : fmt::formatter<std::string> {
  auto format(const dftracer::utils::json::JsonDocuments& docs, fmt::format_context& ctx) {
    std::string s;
    for (size_t i = 0; i < docs.size(); ++i) {
      if (i > 0) s += "\n";
      s += simdjson::minify(docs[i]);
    }
    return fmt::formatter<std::string>::format(s, ctx);
  }
};

template <>
struct fmt::formatter<dftracer::utils::json::OwnedJsonDocument> : fmt::formatter<std::string> {
  auto format(const dftracer::utils::json::OwnedJsonDocument& doc, fmt::format_context& ctx) {
    std::string s = doc.minify();
    return fmt::formatter<std::string>::format(s, ctx);
  }
};

template <>
struct fmt::formatter<dftracer::utils::json::OwnedJsonDocuments> : fmt::formatter<std::string> {
  auto format(const dftracer::utils::json::OwnedJsonDocuments& docs, fmt::format_context& ctx) {
    std::string s;
    for (size_t i = 0; i < docs.size(); ++i) {
      if (i > 0) s += "\n";
      s += docs[i].minify();
    }
    return fmt::formatter<std::string>::format(s, ctx);
  }
};

#endif
