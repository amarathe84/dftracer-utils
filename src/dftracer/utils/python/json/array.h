#ifndef DFTRACER_UTILS_PYTHON_JSON_ARRAY_H
#define DFTRACER_UTILS_PYTHON_JSON_ARRAY_H

#include <dftracer/utils/python/json/array_iterator.h>
#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>

#include <string>

class JsonArray {
 private:
  dftracer::utils::json::JsonDocument doc;

 public:
  explicit JsonArray(const dftracer::utils::json::JsonDocument& d);
  nb::object __getitem__(int index);
  size_t __len__();
  std::string __str__();
  std::string __repr__();
  JsonArrayIterator __iter__();
  bool __contains__(nb::object item);
  int index(nb::object item);
  int count(nb::object item);
  nb::list to_list();
};

#endif  // DFTRACER_UTILS_PYTHON_JSON_ARRAY_H
