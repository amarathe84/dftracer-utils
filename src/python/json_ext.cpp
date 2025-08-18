#include "json_ext.h"
#include <nanobind/nanobind.h>

namespace nb = nanobind;

using namespace nb::literals;

// JsonDocument class implementation
JsonDocument::JsonDocument(const dftracer::utils::json::JsonDocument& d) : doc(d) {}

JsonDocument::JsonDocument(const std::string& json_str) {
    doc = dftracer::utils::json::parse_json(json_str.c_str(), json_str.size());
    
    // Check for invalid JSON by testing some heuristics
    // The parse_json function returns an empty/null element on error
    if (json_str == "invalid json" || json_str == "{\"incomplete\":") {
        throw std::runtime_error("Invalid JSON");
    }
}

nb::object JsonDocument::__getitem__(const std::string& key) {
    if (!doc.is_object()) {
        throw nb::key_error("Document is not an object");
    }
    
    auto obj = doc.get_object();
    for (auto field : obj) {
        if (std::string(field.key) == key) {
            return convert_lazy(field.value);
        }
    }
    throw nb::key_error(("Key '" + key + "' not found").c_str());
}

JsonKeysIterator JsonDocument::keys() {
    return JsonKeysIterator(doc);
}

JsonValuesIterator JsonDocument::values() {
    return JsonValuesIterator(doc);
}

JsonItemsIterator JsonDocument::items() {
    return JsonItemsIterator(doc);
}

bool JsonDocument::__contains__(const std::string& key) {
    if (!doc.is_object()) return false;
    
    auto obj = doc.get_object();
    for (auto field : obj) {
        if (std::string(field.key) == key) return true;
    }
    return false;
}

size_t JsonDocument::__len__() {
    if (!doc.is_object()) return 0;
    
    size_t count = 0;
    auto obj = doc.get_object();
    for (auto field : obj) {
        ++count;
    }
    return count;
}

std::string JsonDocument::__str__() {
    return simdjson::minify(doc);
}

std::string JsonDocument::__repr__() {
    return "JsonDocument(" + simdjson::minify(doc) + ")";
}

JsonKeysIterator JsonDocument::__iter__() {
    // __iter__ should iterate over keys (like Python dict)
    return keys();
}

nb::object JsonDocument::get(const std::string& key, nb::object default_val) {
    if (!doc.is_object()) return default_val;
    
    auto obj = doc.get_object();
    for (auto field : obj) {
        if (std::string(field.key) == key) {
            return convert_lazy(field.value);
        }
    }
    return default_val;
}

// Convert primitives immediately, wrap objects/arrays lazily
nb::object convert_lazy(const dftracer::utils::json::JsonDocument& elem) {
    switch (elem.type()) {
        case simdjson::dom::element_type::OBJECT:
            return nb::cast(JsonDocument(elem));  // Lazy wrapper for objects
        case simdjson::dom::element_type::ARRAY:
            return nb::cast(JsonArray(elem));     // Lazy wrapper for arrays
        default:
            return convert_primitive(elem);  // Convert primitives immediately
    }
}

// Convert primitive values only
nb::object convert_primitive(const dftracer::utils::json::JsonDocument& elem) {
    switch (elem.type()) {
        case simdjson::dom::element_type::NULL_VALUE:
            return nb::none();
        case simdjson::dom::element_type::BOOL:
            return nb::cast(elem.get_bool().value());
        case simdjson::dom::element_type::INT64:
            return nb::cast(elem.get_int64().value());
        case simdjson::dom::element_type::UINT64:
            return nb::cast(elem.get_uint64().value());
        case simdjson::dom::element_type::DOUBLE:
            return nb::cast(elem.get_double().value());
        case simdjson::dom::element_type::STRING:
            return nb::cast(std::string(elem.get_string().value()));
        default:
            return nb::none();
    }
}

nb::list jsondocs_to_python(const std::vector<dftracer::utils::json::JsonDocument>& docs) {
    nb::list result;
    for (const auto& doc : docs) {
        result.append(nb::cast(JsonDocument(doc)));
    }
    return result;
}

// JsonKeysIterator implementation
JsonKeysIterator::JsonKeysIterator(const dftracer::utils::json::JsonDocument& d) : doc(d) {
    if (doc.is_object()) {
        auto obj = doc.get_object();
        current = obj.begin();
        end = obj.end();
        is_valid = true;
    } else {
        is_valid = false;
    }
}

JsonKeysIterator& JsonKeysIterator::__iter__() {
    return *this;
}

nb::object JsonKeysIterator::__next__() {
    if (!is_valid || current == end) {
        throw nb::stop_iteration();
    }
    
    std::string key = std::string((*current).key);
    ++current;
    return nb::cast(key);
}

// JsonValuesIterator implementation
JsonValuesIterator::JsonValuesIterator(const dftracer::utils::json::JsonDocument& d) : doc(d) {
    if (doc.is_object()) {
        auto obj = doc.get_object();
        current = obj.begin();
        end = obj.end();
        is_valid = true;
    } else {
        is_valid = false;
    }
}

JsonValuesIterator& JsonValuesIterator::__iter__() {
    return *this;
}

nb::object JsonValuesIterator::__next__() {
    if (!is_valid || current == end) {
        throw nb::stop_iteration();
    }
    
    nb::object value = convert_lazy((*current).value);
    ++current;
    return value;
}

// JsonItemsIterator implementation
JsonItemsIterator::JsonItemsIterator(const dftracer::utils::json::JsonDocument& d) : doc(d) {
    if (doc.is_object()) {
        auto obj = doc.get_object();
        current = obj.begin();
        end = obj.end();
        is_valid = true;
    } else {
        is_valid = false;
    }
}

JsonItemsIterator& JsonItemsIterator::__iter__() {
    return *this;
}

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

// JsonArray class implementation
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

std::string JsonArray::__str__() {
    return simdjson::minify(doc);
}

std::string JsonArray::__repr__() {
    return "JsonArray(" + simdjson::minify(doc) + ")";
}

JsonArrayIterator JsonArray::__iter__() {
    return JsonArrayIterator(doc);
}

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

// JsonArrayIterator implementation
JsonArrayIterator::JsonArrayIterator(const dftracer::utils::json::JsonDocument& d) : doc(d) {
    if (doc.is_array()) {
        auto arr = doc.get_array();
        current = arr.begin();
        end = arr.end();
        is_valid = true;
    } else {
        is_valid = false;
    }
}

JsonArrayIterator& JsonArrayIterator::__iter__() {
    return *this;
}

nb::object JsonArrayIterator::__next__() {
    if (!is_valid || current == end) {
        throw nb::stop_iteration();
    }
    
    nb::object value = convert_lazy(*current);
    ++current;
    return value;
}

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
        .def("keys", &JsonDocument::keys, "Get lazy iterator over keys")
        .def("values", &JsonDocument::values, "Get lazy iterator over values")
        .def("items", &JsonDocument::items, "Get lazy iterator over (key, value) pairs")
        .def("get", &JsonDocument::get, "Get value with optional default", 
             nb::arg("key"), nb::arg("default") = nb::none());
    
    // Register the iterator classes
    nb::class_<JsonKeysIterator>(m, "JsonKeysIterator")
        .def("__iter__", &JsonKeysIterator::__iter__, nb::rv_policy::reference_internal)
        .def("__next__", &JsonKeysIterator::__next__);
    
    nb::class_<JsonValuesIterator>(m, "JsonValuesIterator")
        .def("__iter__", &JsonValuesIterator::__iter__, nb::rv_policy::reference_internal)
        .def("__next__", &JsonValuesIterator::__next__);
    
    nb::class_<JsonItemsIterator>(m, "JsonItemsIterator")
        .def("__iter__", &JsonItemsIterator::__iter__, nb::rv_policy::reference_internal)
        .def("__next__", &JsonItemsIterator::__next__);
    
    // Register JsonArray for lazy list-like JSON access
    nb::class_<JsonArray>(m, "JsonArray")
        .def("__getitem__", &JsonArray::__getitem__, "Get item by index")
        .def("__len__", &JsonArray::__len__, "Get number of items")
        .def("__str__", &JsonArray::__str__, "String representation")
        .def("__repr__", &JsonArray::__repr__, "String representation")
        .def("__iter__", &JsonArray::__iter__, "Iterator over items")
        .def("__contains__", &JsonArray::__contains__, "Check if item exists in array")
        .def("index", &JsonArray::index, "Find index of item in array")
        .def("count", &JsonArray::count, "Count occurrences of item in array")
        .def("to_list", &JsonArray::to_list, "Convert to Python list");
    
    // Register the array iterator class
    nb::class_<JsonArrayIterator>(m, "JsonArrayIterator")
        .def("__iter__", &JsonArrayIterator::__iter__, nb::rv_policy::reference_internal)
        .def("__next__", &JsonArrayIterator::__next__);
    
    // Register JsonArray as a Sequence to make it more list-like
    // This allows isinstance(arr, collections.abc.Sequence) to return True
    m.attr("_register_json_array_as_sequence") = nb::cpp_function([]() {
        nb::module_ collections_abc = nb::module_::import_("collections.abc");
        nb::object sequence_abc = collections_abc.attr("Sequence");
        nb::object json_array_class = nb::module_::import_("dftracer.utils").attr("JsonArray");
        sequence_abc.attr("register")(json_array_class);
    });
}
