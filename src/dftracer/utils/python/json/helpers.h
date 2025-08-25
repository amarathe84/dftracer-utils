#ifndef DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
#define DFTRACER_UTILS_PYTHON_JSON_HELPERS_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

nb::object convert_primitive(const dftracer::utils::json::JsonDocument& elem);
nb::object convert_jsondoc(const dftracer::utils::json::OwnedJsonDocument& doc);
nb::list convert_jsondocs(
    const dftracer::utils::json::OwnedJsonDocuments& docs);

#endif  // DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
