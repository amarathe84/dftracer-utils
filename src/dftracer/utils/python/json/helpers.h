#ifndef DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
#define DFTRACER_UTILS_PYTHON_JSON_HELPERS_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

// @todo: deprecate
nb::object convert_lazy(const dftracer::utils::json::JsonDocument& elem);
// @todo: deprecate
nb::list jsondocs_to_python(
    const dftracer::utils::json::JsonDocument& docs);
// @todo: deprecate
nb::object old_convert_primitive(const dftracer::utils::json::JsonDocument& elem);

nb::object convert_primitive(const dftracer::utils::json::OwnedJsonDocument& elem);
nb::object convert_jsondoc(const dftracer::utils::json::OwnedJsonDocument& elem);
nb::list convert_jsondocs(
    const dftracer::utils::json::OwnedJsonDocuments& docs);

#endif  // DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
