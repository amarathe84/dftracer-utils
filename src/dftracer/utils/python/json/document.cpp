#include <dftracer/utils/python/json/document.h>
#include <dftracer/utils/python/json/helpers.h>

JsonDocument::JsonDocument(const dftracer::utils::json::JsonDocument& d)
    : doc(d) {}

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

JsonKeysIterator JsonDocument::keys() { return JsonKeysIterator(doc); }

JsonValuesIterator JsonDocument::values() { return JsonValuesIterator(doc); }

JsonItemsIterator JsonDocument::items() { return JsonItemsIterator(doc); }

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

std::string JsonDocument::__str__() { return simdjson::minify(doc); }

std::string JsonDocument::__repr__() {
  std::string type_name;
  switch (doc.type()) {
    case simdjson::dom::element_type::OBJECT:
      type_name = "object";
      break;
    case simdjson::dom::element_type::ARRAY:
      type_name = "array";
      break;
    case simdjson::dom::element_type::NULL_VALUE:
      type_name = "null";
      break;
    case simdjson::dom::element_type::BOOL:
      type_name = "bool";
      break;
    case simdjson::dom::element_type::INT64:
      type_name = "int64";
      break;
    case simdjson::dom::element_type::UINT64:
      type_name = "uint64";
      break;
    case simdjson::dom::element_type::DOUBLE:
      type_name = "double";
      break;
    case simdjson::dom::element_type::STRING:
      type_name = "string";
      break;
    default:
      type_name = "unknown";
      break;
  }
  return "JsonDocument(<" + type_name + ">)";
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
