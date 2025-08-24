#ifndef USERS_RAYANDREW_PROJECTS_DFTRACER_DFTRACER_UTILS_SRC_DFTRACER_UTILS_PYTHON_JSON_EXT_H
#define USERS_RAYANDREW_PROJECTS_DFTRACER_DFTRACER_UTILS_SRC_DFTRACER_UTILS_PYTHON_JSON_EXT_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/make_iterator.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <simdjson.h>

#include <string>
#include <vector>

namespace nb = nanobind;

// Forward declarations
class JsonDocument;
class JsonArray;
class JsonKeysIterator;
class JsonValuesIterator;
class JsonItemsIterator;
class JsonArrayIterator;

// Function declarations - implementations in json_ext.cpp
nb::object convert_lazy(const dftracer::utils::json::JsonDocument& elem);
nb::object convert_primitive(const dftracer::utils::json::JsonDocument& elem);
nb::list jsondocs_to_python(
    const std::vector<dftracer::utils::json::JsonDocument>& docs);

class JsonDocument {
 private:
  dftracer::utils::json::JsonDocument doc;

 public:
  explicit JsonDocument(const dftracer::utils::json::JsonDocument& d);
  explicit JsonDocument(const std::string& json_str);

  // Dict-like access: json["key"]
  nb::object __getitem__(const std::string& key);

  // Get all keys: json.keys() - returns lazy iterator
  JsonKeysIterator keys();

  // Get all values: json.values() - returns lazy iterator
  JsonValuesIterator values();

  // Get all items: json.items() - returns lazy iterator of (key, value) tuples
  JsonItemsIterator items();

  // Check key existence: "key" in json
  bool __contains__(const std::string& key);

  // Get number of keys: len(json)
  size_t __len__();

  // String representation: str(json) or print(json)
  std::string __str__();
  std::string __repr__();

  // Iterator support: for key in json
  JsonKeysIterator __iter__();

  // Get with default: json.get("key", default)
  nb::object get(const std::string& key, nb::object default_val = nb::none());
};

// Lazy JSON Array wrapper
class JsonArray {
 private:
  dftracer::utils::json::JsonDocument doc;

 public:
  explicit JsonArray(const dftracer::utils::json::JsonDocument& d);

  // List-like access: arr[index]
  nb::object __getitem__(int index);

  // Get array length: len(arr)
  size_t __len__();

  // String representation
  std::string __str__();
  std::string __repr__();

  // Iterator support: for item in arr
  JsonArrayIterator __iter__();

  // Additional list-like methods for better compatibility
  bool __contains__(nb::object item);
  int index(nb::object item);
  int count(nb::object item);
  nb::list to_list();  // Convert to actual Python list
};

// Lazy iterator classes
class JsonKeysIterator {
 private:
  dftracer::utils::json::JsonDocument doc;
  simdjson::dom::object::iterator current;
  simdjson::dom::object::iterator end;
  bool is_valid;

 public:
  explicit JsonKeysIterator(const dftracer::utils::json::JsonDocument& d);
  JsonKeysIterator& __iter__();
  nb::object __next__();
};

class JsonValuesIterator {
 private:
  dftracer::utils::json::JsonDocument doc;
  simdjson::dom::object::iterator current;
  simdjson::dom::object::iterator end;
  bool is_valid;

 public:
  explicit JsonValuesIterator(const dftracer::utils::json::JsonDocument& d);
  JsonValuesIterator& __iter__();
  nb::object __next__();
};

class JsonItemsIterator {
 private:
  dftracer::utils::json::JsonDocument doc;
  simdjson::dom::object::iterator current;
  simdjson::dom::object::iterator end;
  bool is_valid;

 public:
  explicit JsonItemsIterator(const dftracer::utils::json::JsonDocument& d);
  JsonItemsIterator& __iter__();
  nb::object __next__();
};

class JsonArrayIterator {
 private:
  dftracer::utils::json::JsonDocument doc;
  simdjson::dom::array::iterator current;
  simdjson::dom::array::iterator end;
  bool is_valid;

 public:
  explicit JsonArrayIterator(const dftracer::utils::json::JsonDocument& d);
  JsonArrayIterator& __iter__();
  nb::object __next__();
};

#endif
