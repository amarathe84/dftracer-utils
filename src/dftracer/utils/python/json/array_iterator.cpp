#include <dftracer/utils/python/json/array_iterator.h>
#include <dftracer/utils/python/json/helpers.h>

JsonArrayIterator::JsonArrayIterator(
    const dftracer::utils::json::JsonDocument& d)
    : doc(d) {
  if (doc.is_array()) {
    auto arr = doc.get_array();
    current = arr.begin();
    end = arr.end();
    is_valid = true;
  } else {
    is_valid = false;
  }
}

JsonArrayIterator& JsonArrayIterator::__iter__() { return *this; }

nb::object JsonArrayIterator::__next__() {
  if (!is_valid || current == end) {
    throw nb::stop_iteration();
  }

  nb::object value = convert_lazy(*current);
  ++current;
  return value;
}
