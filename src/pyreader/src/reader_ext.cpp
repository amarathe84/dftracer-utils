#include <nanobind/nanobind.h>

#include <dftracer_utils/reader/reader.h>

namespace nb = nanobind;

using namespace nb::literals;

NB_MODULE(dft_reader_ext, m) {
    m.doc() = "DFTracer utilities reader extension";
    m.def("add", [](int a, int b) { return a + b; }, "a"_a, "b"_a);
}
