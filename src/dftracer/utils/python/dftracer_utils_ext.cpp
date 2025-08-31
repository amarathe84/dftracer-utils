#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <dftracer/utils/python/indexer.h>
#include <dftracer/utils/python/indexer_checkpoint.h>
#include <dftracer/utils/python/json.h>
#include <dftracer/utils/python/reader.h>

static PyModuleDef dftracer_utils_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "dftracer_utils_ext",
    .m_doc =
        "DFTracer utils module with indexer, reader, and lazy JSON "
        "functionality",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit_dftracer_utils_ext(void) {
    PyObject *m;
    m = PyModule_Create(&dftracer_utils_module);
    if (m == NULL) return NULL;
    if (init_indexer_checkpoint(m) < 0) return NULL;
    if (init_json(m) < 0) return NULL;
    if (init_reader(m) < 0) return NULL;
    if (init_indexer(m) < 0) return NULL;
    return m;
}
