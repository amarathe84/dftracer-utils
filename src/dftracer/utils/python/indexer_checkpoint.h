#ifndef DFTRACER_UTILS_PYTHON_INDEXER_CHECKPOINT_H
#define DFTRACER_UTILS_PYTHON_INDEXER_CHECKPOINT_H

#include <Python.h>
#include <dftracer/utils/indexer/checkpoint.h>

typedef struct {
    PyObject_HEAD dft_indexer_checkpoint_t checkpoint;
} IndexerCheckpointObject;

PyObject *IndexerCheckpoint_new(PyTypeObject *type, PyObject *args,
                                PyObject *kwds);

extern PyTypeObject IndexerCheckpointType;

#endif  // DFTRACER_UTILS_PYTHON_INDEXER_CHECKPOINT_H
