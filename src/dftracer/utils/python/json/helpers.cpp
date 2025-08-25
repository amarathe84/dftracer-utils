#include <dftracer/utils/python/json/helpers.h>
#include <nanobind/stl/list.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

nb::object convert_jsondoc(
    const dftracer::utils::json::OwnedJsonDocument& doc) {
  return convert_jsondoc_element(doc.get_element());
}

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

nb::object convert_jsondoc_element(
    const dftracer::utils::json::JsonDocument& elem) {
  switch (elem.type()) {
    case simdjson::dom::element_type::OBJECT: {
      nb::dict result;
      auto obj = elem.get_object().value();
      for (auto field : obj) {
        std::string key = std::string(field.key);
        dftracer::utils::json::JsonDocument value_doc(field.value);
        result[nb::cast(key)] = convert_jsondoc_element(value_doc);
      }
      return result;
    }
    case simdjson::dom::element_type::ARRAY: {
      nb::list result;
      auto arr = elem.get_array().value();
      for (auto item : arr) {
        dftracer::utils::json::JsonDocument item_doc(item);
        result.append(convert_jsondoc_element(item_doc));
      }
      return result;
    }
    default:
      return convert_primitive(elem);
  }
}

nb::list convert_jsondocs(
    const dftracer::utils::json::OwnedJsonDocuments& docs) {
  nb::list result;
  for (const auto& doc : docs) {
    result.append(convert_jsondoc(doc));
  }
  return result;
}
