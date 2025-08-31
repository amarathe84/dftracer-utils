#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <dftracer/utils/python/indexer.h>
#include <dftracer/utils/python/indexer_checkpoint.h>
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

    if (PyType_Ready(&IndexerCheckpointType) < 0) return NULL;

    if (PyType_Ready(&IndexerType) < 0) return NULL;

    if (PyType_Ready(&ReaderType) < 0) return NULL;

    if (PyType_Ready(&JSONType) < 0) return NULL;

    m = PyModule_Create(&dftracer_utils_module);
    if (m == NULL) return NULL;

    Py_INCREF(&IndexerCheckpointType);
    if (PyModule_AddObject(m, "IndexerCheckpoint",
                           (PyObject *)&IndexerCheckpointType) < 0) {
        Py_DECREF(&IndexerCheckpointType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&IndexerType);
    if (PyModule_AddObject(m, "Indexer", (PyObject *)&IndexerType) < 0) {
        Py_DECREF(&IndexerType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(&ReaderType);
    if (PyModule_AddObject(m, "Reader", (PyObject *)&ReaderType) < 0) {
        Py_DECREF(&ReaderType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
