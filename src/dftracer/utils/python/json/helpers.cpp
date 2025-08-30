#include <dftracer/utils/python/json/helpers.h>
#include <nanobind/stl/list.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <simdjson.h>
#include <sstream>

namespace nb = nanobind;

nb::object deep_copy_json_value(const simdjson::dom::element& elem);

nb::object convert_jsondoc(const std::string& json) {
    dftracer::utils::json::JsonParser parser;
    auto doc =
        dftracer::utils::json::parse_json(parser, json.data(), json.size());
    return deep_copy_json_value(doc);
}

// Helper function to convert ondemand value to nanobind object
nb::object convert_ondemand_value(simdjson::ondemand::value val) {
    auto type_result = val.type();
    if (type_result.error()) {
        return nb::none();
    }
    
    switch (type_result.value()) {
        case simdjson::ondemand::json_type::null:
            return nb::none();
        case simdjson::ondemand::json_type::boolean: {
            auto bool_result = val.get_bool();
            if (bool_result.error()) return nb::none();
            return nb::cast(bool_result.value());
        }
        case simdjson::ondemand::json_type::number: {
            // Try different number types
            auto int_result = val.get_int64();
            if (!int_result.error()) {
                return nb::cast(int_result.value());
            }
            auto uint_result = val.get_uint64();
            if (!uint_result.error()) {
                return nb::cast(uint_result.value());
            }
            auto double_result = val.get_double();
            if (!double_result.error()) {
                return nb::cast(double_result.value());
            }
            return nb::cast(0);  // fallback
        }
        case simdjson::ondemand::json_type::string: {
            auto str_result = val.get_string();
            if (str_result.error()) return nb::none();
            std::string_view sv = str_result.value();
            return nb::cast(std::string(sv));
        }
        case simdjson::ondemand::json_type::array: {
            auto array_result = val.get_array();
            if (array_result.error()) return nb::none();
            
            nb::list result;
            for (auto item : array_result.value()) {
                // item.value() returns simdjson::ondemand::value directly
                result.append(convert_ondemand_value(item.value()));
            }
            return result;
        }
        case simdjson::ondemand::json_type::object: {
            auto obj_result = val.get_object();
            if (obj_result.error()) return nb::none();
            
            nb::dict result;
            for (auto field : obj_result.value()) {
                // Use the unescaped_key() method on the field directly
                auto key_result = field.unescaped_key();
                auto val_result = field.value();
                
                if (!key_result.error() && !val_result.error()) {
                    std::string_view key_sv = key_result.value();
                    std::string key_str(key_sv);
                    result[nb::cast(key_str)] = convert_ondemand_value(val_result.value());
                }
            }
            return result;
        }
        default:
            return nb::none();
    }
}

nb::list convert_jsondocs(const std::string& json_docs) {
    if (json_docs.empty()) {
        return nb::list();
    }

    nb::list result;
    simdjson::ondemand::parser parser;
    
    // Process each line individually to avoid buffer sharing issues
    std::istringstream stream(json_docs);
    std::string line;
    
    while (std::getline(stream, line)) {
        if (line.empty()) continue;
        
        try {
            // Each iteration gets fresh parser state
            simdjson::padded_string padded_line(line);
            auto doc = parser.iterate(padded_line);
            auto obj_result = doc.get_value();
            
            if (!obj_result.error()) {
                result.append(convert_ondemand_value(obj_result.value()));
            }
        } catch (const simdjson::simdjson_error&) {
            // Skip malformed JSON lines
            continue;
        }
    }
    
    return result;
}

nb::object deep_copy_json_value(const simdjson::dom::element& elem) {
    switch (elem.type()) {
        case simdjson::dom::element_type::NULL_VALUE:
            return nb::none();
        case simdjson::dom::element_type::BOOL: {
            auto bool_result = elem.get_bool();
            if (bool_result.error()) return nb::none();
            return nb::cast(bool_result.value());
        }
        case simdjson::dom::element_type::INT64: {
            auto int_result = elem.get_int64();
            if (int_result.error()) return nb::none();
            return nb::cast(int_result.value());
        }
        case simdjson::dom::element_type::UINT64: {
            auto uint_result = elem.get_uint64();
            if (uint_result.error()) return nb::none();
            return nb::cast(uint_result.value());
        }
        case simdjson::dom::element_type::DOUBLE: {
            auto double_result = elem.get_double();
            if (double_result.error()) return nb::none();
            return nb::cast(double_result.value());
        }
        case simdjson::dom::element_type::STRING: {
            auto str_result = elem.get_string();
            if (str_result.error()) return nb::none();
            std::string val(str_result.value());
            return nb::cast(std::move(val));
        }
        case simdjson::dom::element_type::ARRAY: {
            auto arr_result = elem.get_array();
            if (arr_result.error()) return nb::none();
            nb::list result;
            for (auto item : arr_result.value()) {
                result.append(deep_copy_json_value(item));
            }
            return result;
        }
        case simdjson::dom::element_type::OBJECT: {
            auto obj_result = elem.get_object();
            if (obj_result.error()) return nb::none();
            nb::dict result;
            for (auto field : obj_result.value()) {
                std::string key(field.key);
                result[nb::cast(std::move(key))] =
                    deep_copy_json_value(field.value);
            }
            return result;
        }
        default:
            return nb::none();
    }
}
