#include "json_ext.h"
#include <nanobind/nanobind.h>

namespace nb = nanobind;

using namespace nb::literals;

void register_json(nb::module_& m) {
    // Register JsonDocument for lazy dict-like JSON access
    nb::class_<JsonDocument>(m, "JsonDocument")
        .def(nb::init<const std::string&>(), "json_str"_a, "Create from JSON string")
        .def("__getitem__", &JsonDocument::__getitem__, "Get item by key")
        .def("__contains__", &JsonDocument::__contains__, "Check if key exists")
        .def("__len__", &JsonDocument::__len__, "Get number of keys")
        .def("__str__", &JsonDocument::__str__, "String representation")
        .def("__repr__", &JsonDocument::__repr__, "String representation")
        .def("__iter__", &JsonDocument::__iter__, "Iterator over keys")
        .def("keys", &JsonDocument::keys, "Get all keys")
        .def("get", &JsonDocument::get, "Get value with optional default", 
             nb::arg("key"), nb::arg("default") = nb::none());
}
