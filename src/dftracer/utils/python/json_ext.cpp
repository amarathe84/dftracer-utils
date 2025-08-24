#include <dftracer/utils/python/json/array.h>
#include <dftracer/utils/python/json/array_iterator.h>
#include <dftracer/utils/python/json/document.h>
#include <dftracer/utils/python/json/items_iterator.h>
#include <dftracer/utils/python/json/keys_iterator.h>
#include <dftracer/utils/python/json/values_iterator.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/list.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;

using namespace nb::literals;

void register_json(nb::module_& m) {
  nb::class_<JsonDocument>(m, "JsonDocument")
      .def(nb::init<const std::string&>(), "json_str"_a,
           "Create from JSON string")
      .def("__getitem__", &JsonDocument::__getitem__, "Get item by key")
      .def("__contains__", &JsonDocument::__contains__, "Check if key exists")
      .def("__len__", &JsonDocument::__len__, "Get number of keys")
      .def("__str__", &JsonDocument::__str__, "String representation")
      .def("__repr__", &JsonDocument::__repr__, "String representation")
      .def("__iter__", &JsonDocument::__iter__, "Iterator over keys")
      .def("keys", &JsonDocument::keys, "Get lazy iterator over keys")
      .def("values", &JsonDocument::values, "Get lazy iterator over values")
      .def("items", &JsonDocument::items,
           "Get lazy iterator over (key, value) pairs")
      .def("get", &JsonDocument::get, "Get value with optional default",
           nb::arg("key"), nb::arg("default") = nb::none());

  nb::class_<JsonKeysIterator>(m, "JsonKeysIterator")
      .def("__iter__", &JsonKeysIterator::__iter__,
           nb::rv_policy::reference_internal)
      .def("__next__", &JsonKeysIterator::__next__);

  nb::class_<JsonValuesIterator>(m, "JsonValuesIterator")
      .def("__iter__", &JsonValuesIterator::__iter__,
           nb::rv_policy::reference_internal)
      .def("__next__", &JsonValuesIterator::__next__);

  nb::class_<JsonItemsIterator>(m, "JsonItemsIterator")
      .def("__iter__", &JsonItemsIterator::__iter__,
           nb::rv_policy::reference_internal)
      .def("__next__", &JsonItemsIterator::__next__);

  nb::class_<JsonArray>(m, "JsonArray")
      .def("__getitem__", &JsonArray::__getitem__, "Get item by index")
      .def("__len__", &JsonArray::__len__, "Get number of items")
      .def("__str__", &JsonArray::__str__, "String representation")
      .def("__repr__", &JsonArray::__repr__, "String representation")
      .def("__iter__", &JsonArray::__iter__, "Iterator over items")
      .def("__contains__", &JsonArray::__contains__,
           "Check if item exists in array")
      .def("index", &JsonArray::index, "Find index of item in array")
      .def("count", &JsonArray::count, "Count occurrences of item in array")
      .def("to_list", &JsonArray::to_list, "Convert to Python list");

  nb::class_<JsonArrayIterator>(m, "JsonArrayIterator")
      .def("__iter__", &JsonArrayIterator::__iter__,
           nb::rv_policy::reference_internal)
      .def("__next__", &JsonArrayIterator::__next__);

  // Register JsonArray as a Sequence to make it more list-like
  // This allows isinstance(arr, collections.abc.Sequence) to return True
  m.attr("_register_json_array_as_sequence") = nb::cpp_function([]() {
    nb::module_ collections_abc = nb::module_::import_("collections.abc");
    nb::object sequence_abc = collections_abc.attr("Sequence");
    nb::object json_array_class =
        nb::module_::import_("dftracer.utils").attr("JsonArray");
    sequence_abc.attr("register")(json_array_class);
  });
}
