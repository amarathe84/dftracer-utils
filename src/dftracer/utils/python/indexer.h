#ifndef DFTRACER_UTILS_PYTHON_INDEXER_H
#define DFTRACER_UTILS_PYTHON_INDEXER_H

#include <Python.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/indexer/checkpoint.h>

// Python object wrapper for IndexCheckpoint
typedef struct {
    PyObject_HEAD
    dft_indexer_checkpoint_t checkpoint;
} IndexCheckpointObject;

// Python object wrapper for DFTracerIndexer
typedef struct {
    PyObject_HEAD
    dft_indexer_handle_t handle;
    PyObject *gz_path;
    PyObject *idx_path;
    size_t checkpoint_size;
} DFTracerIndexerObject;

// Type objects
extern PyTypeObject IndexCheckpointType;
extern PyTypeObject DFTracerIndexerType;

// Module initialization
PyMODINIT_FUNC PyInit_indexer(void);

#endif