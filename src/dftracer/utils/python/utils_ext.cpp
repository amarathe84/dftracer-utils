#include <dftracer/utils/common/logging.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;

using namespace nb::literals;

void register_logging() { DFTRACER_UTILS_LOGGER_INIT(); }

void register_utils(nb::module_& m) {
    m.def("_register_logging", &register_logging);
}
