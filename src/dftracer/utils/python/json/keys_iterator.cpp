#include <dftracer/utils/python/json/keys_iterator.h>
#include <nanobind/stl/string.h>

JsonKeysIterator::JsonKeysIterator(const dftracer::utils::json::JsonDocument& d)
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

JsonKeysIterator& JsonKeysIterator::__iter__() { return *this; }

nb::object JsonKeysIterator::__next__() {
  if (!is_valid || current == end) {
    throw nb::stop_iteration();
  }

  std::string key = std::string((*current).key);
  ++current;
  return nb::cast(key);
}
