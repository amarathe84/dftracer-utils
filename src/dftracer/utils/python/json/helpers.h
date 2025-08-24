#ifndef DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
#define DFTRACER_UTILS_PYTHON_JSON_HELPERS_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

nb::object convert_lazy(const dftracer::utils::json::JsonDocument& elem);
nb::object convert_primitive(const dftracer::utils::json::JsonDocument& elem);
nb::list jsondocs_to_python(
    const std::vector<dftracer::utils::json::JsonDocument>& docs);

#endif  // DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
