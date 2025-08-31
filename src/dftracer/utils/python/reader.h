#ifndef DFTRACER_UTILS_PYTHON_READER_H
#define DFTRACER_UTILS_PYTHON_READER_H

#include <Python.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/python/indexer.h>

// Python object wrapper for DFTracerReader
typedef struct {
    PyObject_HEAD
    dft_reader_handle_t handle;
    PyObject *gz_path;
    PyObject *idx_path;
    size_t checkpoint_size;
    size_t buffer_size;
} DFTracerReaderObject;

// Type objects
extern PyTypeObject DFTracerReaderType;

// Module initialization
PyMODINIT_FUNC PyInit_reader(void);

#endif
