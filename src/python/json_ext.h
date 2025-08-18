#ifndef __DFTRACER_UTILS_PYTHON_JSON_EXT_H
#define __DFTRACER_UTILS_PYTHON_JSON_EXT_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <simdjson.h>

#include <string>
#include <vector>

namespace nb = nanobind;

// Forward declarations
class JsonDocument;
nb::object convert_lazy(const dftracer::utils::json::JsonDocument& elem);
nb::object convert_primitive(const dftracer::utils::json::JsonDocument& elem);

class JsonDocument {
private:
    dftracer::utils::json::JsonDocument doc;

public:
    explicit JsonDocument(const dftracer::utils::json::JsonDocument& d) : doc(d) {}
    
    // Constructor from JSON string
    explicit JsonDocument(const std::string& json_str) {
        doc = dftracer::utils::json::parse_json(json_str.c_str(), json_str.size());
    }
    
    // Dict-like access: json["key"]
    nb::object __getitem__(const std::string& key) {
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
    
    // Get all keys: json.keys()
    nb::list keys() {
        nb::list result;
        if (doc.is_object()) {
            auto obj = doc.get_object();
            for (auto field : obj) {
                result.append(nb::cast(std::string(field.key)));
            }
        }
        return result;
    }
    
    // Check key existence: "key" in json
    bool __contains__(const std::string& key) {
        if (!doc.is_object()) return false;
        
        auto obj = doc.get_object();
        for (auto field : obj) {
            if (std::string(field.key) == key) return true;
        }
        return false;
    }
    
    // Get number of keys: len(json)
    size_t __len__() {
        if (!doc.is_object()) return 0;
        
        size_t count = 0;
        auto obj = doc.get_object();
        for (auto field : obj) {
            ++count;
        }
        return count;
    }
    
    // String representation: str(json) or print(json)
    std::string __str__() {
        return simdjson::minify(doc);
    }
    
    std::string __repr__() {
        return "JsonDocument(" + simdjson::minify(doc) + ")";
    }
    
    // Iterator support: for key in json
    nb::list __iter__() {
        return keys();
    }
    
    // Get with default: json.get("key", default)
    nb::object get(const std::string& key, nb::object default_val = nb::none()) {
        if (!doc.is_object()) return default_val;
        
        auto obj = doc.get_object();
        for (auto field : obj) {
            if (std::string(field.key) == key) {
                return convert_lazy(field.value);
            }
        }
        return default_val;
    }
};

// Convert primitives immediately, wrap objects/arrays lazily
inline nb::object convert_lazy(const dftracer::utils::json::JsonDocument& elem) {
    switch (elem.type()) {
        case simdjson::dom::element_type::OBJECT:
            return nb::cast(JsonDocument(elem));  // Lazy wrapper for objects
        case simdjson::dom::element_type::ARRAY: {
            // For arrays, we could also make lazy, but for now convert to list
            nb::list py_list;
            auto arr = elem.get_array();
            for (auto element : arr) {
                py_list.append(convert_lazy(element));
            }
            return py_list;
        }
        default:
            return convert_primitive(elem);  // Convert primitives immediately
    }
}

// Convert primitive values only
inline nb::object convert_primitive(const dftracer::utils::json::JsonDocument& elem) {
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

inline nb::list jsondocs_to_python(const std::vector<dftracer::utils::json::JsonDocument>& docs) {
    nb::list result;
    for (const auto& doc : docs) {
        result.append(nb::cast(JsonDocument(doc)));
    }
    return result;
}

#endif
