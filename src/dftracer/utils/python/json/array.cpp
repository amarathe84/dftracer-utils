#include <dftracer/utils/python/json/array.h>
#include <dftracer/utils/python/json/helpers.h>

JsonArray::JsonArray(const dftracer::utils::json::JsonDocument& d) : doc(d) {}

nb::object JsonArray::__getitem__(int index) {
  if (!doc.is_array()) {
    throw nb::index_error("Document is not an array");
  }

  auto arr = doc.get_array();

  // Handle negative indices (not supported)
  if (index < 0) {
    throw nb::index_error("Negative indexing not supported");
  }

  // Check bounds
  if (static_cast<size_t>(index) >= arr.size()) {
    throw nb::index_error("Array index out of range");
  }

  auto iter = arr.begin();
  std::advance(iter, index);
  return convert_lazy(*iter);
}

size_t JsonArray::__len__() {
  if (!doc.is_array()) return 0;

  auto arr = doc.get_array();
  return arr.size();
}

std::string JsonArray::__str__() { return simdjson::minify(doc); }

std::string JsonArray::__repr__() {
  return "JsonArray(" + simdjson::minify(doc) + ")";
}

JsonArrayIterator JsonArray::__iter__() { return JsonArrayIterator(doc); }

bool JsonArray::__contains__(nb::object item) {
  if (!doc.is_array()) return false;

  auto arr = doc.get_array();
  for (auto elem : arr) {
    nb::object converted = convert_lazy(elem);
    // Use Python's equality comparison
    try {
      nb::object result = converted.attr("__eq__")(item);
      if (nb::cast<bool>(result)) {
        return true;
      }
    } catch (...) {
      // If comparison fails, continue
      continue;
    }
  }
  return false;
}

int JsonArray::index(nb::object item) {
  if (!doc.is_array()) {
    throw nb::value_error("list.index(x): x not in list");
  }

  auto arr = doc.get_array();
  int idx = 0;
  for (auto elem : arr) {
    nb::object converted = convert_lazy(elem);
    try {
      nb::object result = converted.attr("__eq__")(item);
      if (nb::cast<bool>(result)) {
        return idx;
      }
    } catch (...) {
      // If comparison fails, continue
    }
    idx++;
  }
  throw nb::value_error("list.index(x): x not in list");
}

int JsonArray::count(nb::object item) {
  if (!doc.is_array()) return 0;

  auto arr = doc.get_array();
  int count = 0;
  for (auto elem : arr) {
    nb::object converted = convert_lazy(elem);
    try {
      nb::object result = converted.attr("__eq__")(item);
      if (nb::cast<bool>(result)) {
        count++;
      }
    } catch (...) {
      // If comparison fails, continue
    }
  }
  return count;
}

nb::list JsonArray::to_list() {
  nb::list result;
  if (!doc.is_array()) return result;

  auto arr = doc.get_array();
  for (auto elem : arr) {
    result.append(convert_lazy(elem));
  }
  return result;
}
