#ifndef DFTRACER_UTILS_UTILS_JSON_H
#define DFTRACER_UTILS_UTILS_JSON_H

#include <simdjson.h>

#include <cstdint>
#include <cstdio>
#include <iosfwd>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::json {
using JsonParser = simdjson::dom::parser;
using JsonDocument = simdjson::dom::element;
using JsonDocuments = std::vector<JsonDocument>;

JsonDocument parse_json(const JsonParser& parser, const char* data,
                        std::size_t size);
JsonDocuments parse_json_lines(JsonParser& parser, const char* data,
                               std::size_t size);

std::string get_string_field(const JsonDocument& doc, const std::string& key);
double get_double_field(const JsonDocument& doc, const std::string& key);
std::uint64_t get_uint64_field(const JsonDocument& doc, const std::string& key);
std::string get_args_string_field(const JsonDocument& doc,
                                  const std::string& key);

std::ostream& operator<<(std::ostream& os, const JsonDocument& doc);
std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs);
}  // namespace dftracer::utils::json

#endif
