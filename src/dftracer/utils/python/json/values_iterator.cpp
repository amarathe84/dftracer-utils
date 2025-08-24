#include <dftracer/utils/python/json/helpers.h>
#include <dftracer/utils/python/json/values_iterator.h>
#include <nanobind/stl/string.h>

JsonValuesIterator::JsonValuesIterator(
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

JsonValuesIterator& JsonValuesIterator::__iter__() { return *this; }

nb::object JsonValuesIterator::__next__() {
  if (!is_valid || current == end) {
    throw nb::stop_iteration();
  }

  nb::object value = convert_lazy((*current).value);
  ++current;
  return value;
}
