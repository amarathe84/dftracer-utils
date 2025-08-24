#ifndef DFTRACER_UTILS_UTILS_JSON_H
#define DFTRACER_UTILS_UTILS_JSON_H

#include <simdjson.h>
#include <cstdio>
#include <iostream>

#include <iosfwd>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::json {

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

  template <class Archive>
  void serialize(Archive& ar) {
    ar(data_);
  }
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

std::string get_string_field_owned(const OwnedJsonDocument& doc,
                                   const std::string& key);
double get_double_field_owned(const OwnedJsonDocument& doc,
                              const std::string& key);
uint64_t get_uint64_field_owned(const OwnedJsonDocument& doc,
                                const std::string& key);
std::string get_args_string_field_owned(const OwnedJsonDocument& doc,
                                        const std::string& key);

std::ostream& operator<<(std::ostream& os, const JsonDocument& doc);
std::ostream& operator<<(std::ostream& os, const JsonDocuments& docs);

std::ostream& operator<<(std::ostream& os, const OwnedJsonDocument& doc);
std::ostream& operator<<(std::ostream& os, const OwnedJsonDocuments& docs);
}  // namespace dftracer::utils::json

#endif
