#include <nanobind/nanobind.h>

#include "json_ext.h"

namespace nb = nanobind;
using namespace nb::literals;

// Forward declarations for registration functions
void register_indexer(nb::module_& m);
void register_reader(nb::module_& m);
void register_utils(nb::module_& m);
void register_json(nb::module_& m);

NB_MODULE(dftracer_utils_ext, m) {
  m.doc() = "DFTracer utilities extension";

  // Register all functionality
  register_json(m);
  register_indexer(m);
  register_reader(m);
  register_utils(m);
}