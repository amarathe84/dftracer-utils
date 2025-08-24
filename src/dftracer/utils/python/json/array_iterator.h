#ifndef DFTRACER_UTILS_PYTHON_JSON_ARRAY_ITERATOR_H
#define DFTRACER_UTILS_PYTHON_JSON_ARRAY_ITERATOR_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>
#include <simdjson.h>

namespace nb = nanobind;

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

#endif  // DFTRACER_UTILS_PYTHON_JSON_ARRAY_ITERATOR_H
