#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <dftracer/utils/python/indexer.h>
#include <dftracer/utils/python/json.h>
#include <dftracer/utils/python/reader.h>

static PyModuleDef dftracer_utils_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "dftracer_utils_ext",
    .m_doc = "DFTracer utils module with indexer and reader functionality",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit_dftracer_utils_ext(void) {
    PyObject *m;

    if (PyType_Ready(&IndexCheckpointType) < 0) return NULL;

    if (PyType_Ready(&DFTracerIndexerType) < 0) return NULL;

    if (PyType_Ready(&DFTracerReaderType) < 0) return NULL;

    if (PyType_Ready(&DFTracerJSONType) < 0) return NULL;

    m = PyModule_Create(&dftracer_utils_module);
    if (m == NULL) return NULL;

    Py_INCREF(&IndexCheckpointType);
    if (PyModule_AddObject(m, "IndexCheckpoint",
                           (PyObject *)&IndexCheckpointType) < 0) {
        Py_DECREF(&IndexCheckpointType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&DFTracerIndexerType);
    if (PyModule_AddObject(m, "DFTracerIndexer",
                           (PyObject *)&DFTracerIndexerType) < 0) {
        Py_DECREF(&DFTracerIndexerType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&DFTracerReaderType);
    if (PyModule_AddObject(m, "DFTracerReader",
                           (PyObject *)&DFTracerReaderType) < 0) {
        Py_DECREF(&DFTracerReaderType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
