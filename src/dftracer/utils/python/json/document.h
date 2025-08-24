#ifndef DFTRACER_UTILS_PYTHON_JSON_DOCUMENT_H
#define DFTRACER_UTILS_PYTHON_JSON_DOCUMENT_H

#include <dftracer/utils/python/json/document.h>
#include <dftracer/utils/python/json/items_iterator.h>
#include <dftracer/utils/python/json/keys_iterator.h>
#include <dftracer/utils/python/json/values_iterator.h>
#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

class JsonDocument {
 private:
  dftracer::utils::json::JsonDocument doc;

 public:
  explicit JsonDocument(const dftracer::utils::json::JsonDocument& d);
  explicit JsonDocument(const std::string& json_str);
  nb::object __getitem__(const std::string& key);
  JsonKeysIterator keys();
  JsonValuesIterator values();
  JsonItemsIterator items();
  bool __contains__(const std::string& key);
  size_t __len__();
  bool __bool__();
  std::string __str__();
  std::string __repr__();
  JsonKeysIterator __iter__();
  nb::object get(const std::string& key, nb::object default_val = nb::none());
};

#endif  // DFTRACER_UTILS_PYTHON_JSON_DOCUMENT_H
