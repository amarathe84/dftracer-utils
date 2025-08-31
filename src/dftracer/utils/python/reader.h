#ifndef DFTRACER_UTILS_PYTHON_READER_H
#define DFTRACER_UTILS_PYTHON_READER_H

#include <Python.h>
#include <dftracer/utils/python/indexer.h>
#include <dftracer/utils/reader/reader.h>

typedef struct {
    PyObject_HEAD dft_reader_handle_t handle;
    PyObject *gz_path;
    PyObject *idx_path;
    size_t checkpoint_size;
    size_t buffer_size;
} ReaderObject;

extern PyTypeObject ReaderType;

PyMODINIT_FUNC PyInit_reader(void);

#endif
