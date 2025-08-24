#ifndef DFTRACER_UTILS_PYTHON_JSON_ITEMS_ITERATOR_H
#define DFTRACER_UTILS_PYTHON_JSON_ITEMS_ITERATOR_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

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

#endif  // DFTRACER_UTILS_PYTHON_JSON_ITEMS_ITERATOR_H
