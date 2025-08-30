#include <dftracer/utils/python/json/helpers.h>
#include <nanobind/stl/list.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;

nb::object deep_copy_json_value(const simdjson::dom::element& elem);

nb::object convert_jsondoc(const std::string& json) {
    dftracer::utils::json::JsonParser parser;
    auto doc =
        dftracer::utils::json::parse_json(parser, json.data(), json.size());
    return deep_copy_json_value(doc);
}

nb::list convert_jsondocs(const std::string& json_docs) {
    if (json_docs.empty()) {
        return nb::list();
    }

    dftracer::utils::json::JsonParser parser;
    auto parsed_docs = dftracer::utils::json::parse_json_lines(
        parser, json_docs.data(), json_docs.size());

    nb::list result;
    for (const auto& doc : parsed_docs) {
        result.append(deep_copy_json_value(doc));
    }
    return result;
}

nb::object deep_copy_json_value(const simdjson::dom::element& elem) {
    switch (elem.type()) {
        case simdjson::dom::element_type::NULL_VALUE:
            return nb::none();
        case simdjson::dom::element_type::BOOL: {
            bool val = elem.get_bool().value();
            return nb::cast(val);
        }
        case simdjson::dom::element_type::INT64: {
            int64_t val = elem.get_int64().value();
            return nb::cast(val);
        }
        case simdjson::dom::element_type::UINT64: {
            uint64_t val = elem.get_uint64().value();
            return nb::cast(val);
        }
        case simdjson::dom::element_type::DOUBLE: {
            double val = elem.get_double().value();
            return nb::cast(val);
        }
        case simdjson::dom::element_type::STRING: {
            std::string val(elem.get_string().value());
            return nb::cast(std::move(val));
        }
        case simdjson::dom::element_type::ARRAY: {
            auto arr = elem.get_array().value();
            nb::list result;
            for (auto item : arr) {
                result.append(deep_copy_json_value(item));
            }
            return result;
        }
        case simdjson::dom::element_type::OBJECT: {
            auto obj = elem.get_object().value();
            nb::dict result;
            for (auto field : obj) {
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
