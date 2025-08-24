#include <dftracer/utils/python/json/helpers.h>
#include <dftracer/utils/python/json/items_iterator.h>
#include <nanobind/stl/string.h>

JsonItemsIterator::JsonItemsIterator(
    const dftracer::utils::json::JsonDocument& d)
    : doc(d) {
  if (doc.is_object()) {
    auto obj = doc.get_object();
    current = obj.begin();
    end = obj.end();
    is_valid = true;
  } else {
    is_valid = false;
  }
}

JsonItemsIterator& JsonItemsIterator::__iter__() { return *this; }

nb::object JsonItemsIterator::__next__() {
  if (!is_valid || current == end) {
    throw nb::stop_iteration();
  }

  std::string key = std::string((*current).key);
  nb::object value = convert_lazy((*current).value);
  ++current;

  // Return a tuple (key, value)
  return nb::make_tuple(key, value);
}
