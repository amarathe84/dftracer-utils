#include <dftracer/utils/utils/logger.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace nb = nanobind;

using namespace nb::literals;

NB_MODULE(utils_ext, m) {
  m.def("set_log_level", &dftracer::utils::logger::set_log_level, "level"_a,
        "Set the global log level using a string (trace, debug, info, warn, "
        "error, critical, off)");

  m.def("set_log_level_int", &dftracer::utils::logger::set_log_level_int,
        "level"_a,
        "Set the global log level using an integer (0=trace, 1=debug, 2=info, "
        "3=warn, 4=error, 5=critical, 6=off)");

  m.def("get_log_level_string", &dftracer::utils::logger::get_log_level_string,
        "Get the current global log level as a string");

  m.def("get_log_level_int", &dftracer::utils::logger::get_log_level_int,
        "Get the current global log level as an integer");
}
