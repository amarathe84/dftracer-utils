#ifndef __DFTRACER_UTILS_UTILS_DFT_EVENTS_H
#define __DFTRACER_UTILS_UTILS_DFT_EVENTS_H

#include <simdjson.h>

#include <iosfwd>
#include <string>
#include <unordered_map>
#include <vector>

#include <spdlog/fmt/fmt.h>

namespace dftracer {
namespace utils {
namespace json {

// High-performance JSON types using simdjson DOM directly - no conversion
using JsonDocument = simdjson::dom::element;
using JsonDocuments = std::vector<JsonDocument>;

JsonDocuments parse_json_lines(const char* data, size_t size);
JsonDocument parse_json(const char* data, size_t size);

// Helper functions for extracting values from JSON documents
std::string get_string_field(const JsonDocument& doc, const std::string& key);
double get_double_field(const JsonDocument& doc, const std::string& key);
uint64_t get_uint64_field(const JsonDocument& doc, const std::string& key);
std::string get_args_string_field(const JsonDocument& doc,
                                  const std::string& key);

std::ostream& operator<<(std::ostream& os, const JsonDocument& doc);
std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs);

}  // namespace json
}  // namespace utils
}  // namespace dftracer



// fmt formatter for JsonDocument
template <>
struct fmt::formatter<dftracer::utils::json::JsonDocument> : fmt::formatter<std::string> {
  auto format(const dftracer::utils::json::JsonDocument& doc, fmt::format_context& ctx) {
    std::string s = simdjson::minify(doc);
    return fmt::formatter<std::string>::format(s, ctx);
  }
};

// fmt formatter for JsonDocuments
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

#endif
