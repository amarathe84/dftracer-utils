#include <dftracer/utils/python/json/array.h>
#include <dftracer/utils/python/json/document.h>
#include <dftracer/utils/python/json/helpers.h>
#include <nanobind/stl/list.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

nb::object convert_lazy(const dftracer::utils::json::JsonDocument& elem) {
  switch (elem.type()) {
    case simdjson::dom::element_type::OBJECT:
      return nb::cast(JsonDocument(elem));
    case simdjson::dom::element_type::ARRAY:
      return nb::cast(JsonArray(elem));
    default:
      return convert_primitive(elem);
  }
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

nb::list jsondocs_to_python(
    const std::vector<dftracer::utils::json::JsonDocument>& docs) {
  nb::list result;
  for (const auto& doc : docs) {
    result.append(nb::cast(JsonDocument(doc)));
  }
  return result;
}
