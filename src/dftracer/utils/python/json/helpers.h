#ifndef DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
#define DFTRACER_UTILS_PYTHON_JSON_HELPERS_H

#include <dftracer/utils/utils/json.h>
#include <nanobind/nanobind.h>

nanobind::object convert_jsondoc(const std::string& json);
nanobind::list convert_jsondocs(const std::string& json_docs);

#endif  // DFTRACER_UTILS_PYTHON_JSON_HELPERS_H
