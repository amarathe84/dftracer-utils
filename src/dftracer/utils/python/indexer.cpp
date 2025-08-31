#include <dftracer/utils/python/indexer.h>
#include <structmember.h>

// IndexCheckpoint Python object implementation
static PyObject *IndexCheckpoint_new(PyTypeObject *type, PyObject *args,
                                     PyObject *kwds) {
    IndexCheckpointObject *self;
    self = (IndexCheckpointObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        // Initialize checkpoint with zeros
        memset(&self->checkpoint, 0, sizeof(dft_indexer_checkpoint_t));
    }
    return (PyObject *)self;
}

static PyMemberDef IndexCheckpoint_members[] = {
    {"checkpoint_idx", T_ULONGLONG,
     offsetof(IndexCheckpointObject, checkpoint.checkpoint_idx), 0,
     "Checkpoint index"},
    {"uc_offset", T_ULONGLONG,
     offsetof(IndexCheckpointObject, checkpoint.uc_offset), 0,
     "Uncompressed offset"},
    {"uc_size", T_ULONGLONG,
     offsetof(IndexCheckpointObject, checkpoint.uc_size), 0,
     "Uncompressed size"},
    {"c_offset", T_ULONGLONG,
     offsetof(IndexCheckpointObject, checkpoint.c_offset), 0,
     "Compressed offset"},
    {"c_size", T_ULONGLONG, offsetof(IndexCheckpointObject, checkpoint.c_size),
     0, "Compressed size"},
    {"bits", T_UINT, offsetof(IndexCheckpointObject, checkpoint.bits), 0,
     "Bit position"},
    {"num_lines", T_ULONGLONG,
     offsetof(IndexCheckpointObject, checkpoint.num_lines), 0,
     "Number of lines in this chunk"},
    {NULL} /* Sentinel */
};

PyTypeObject IndexCheckpointType = {
    PyVarObject_HEAD_INIT(NULL, 0) "indexer.IndexCheckpoint", /* tp_name */
    sizeof(IndexCheckpointObject),                            /* tp_basicsize */
    0,                                                        /* tp_itemsize */
    0,                                                        /* tp_dealloc */
    0,                         /* tp_vectorcall_offset */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_as_async */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "IndexCheckpoint objects", /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    0,                         /* tp_methods */
    IndexCheckpoint_members,   /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    IndexCheckpoint_new,       /* tp_new */
};

// DFTracerIndexer Python object implementation
static void DFTracerIndexer_dealloc(DFTracerIndexerObject *self) {
    if (self->handle) {
        dft_indexer_destroy(self->handle);
    }
    Py_XDECREF(self->gz_path);
    Py_XDECREF(self->idx_path);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *DFTracerIndexer_new(PyTypeObject *type, PyObject *args,
                                     PyObject *kwds) {
    DFTracerIndexerObject *self;
    self = (DFTracerIndexerObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->handle = NULL;
        self->gz_path = NULL;
        self->idx_path = NULL;
        self->checkpoint_size = 0;
    }
    return (PyObject *)self;
}

static int DFTracerIndexer_init(DFTracerIndexerObject *self, PyObject *args,
                                PyObject *kwds) {
    static const char *kwlist[] = {"gz_path", "idx_path", "checkpoint_size",
                                   "force_rebuild", NULL};
    const char *gz_path;
    const char *idx_path = NULL;
    size_t checkpoint_size = 1024 * 1024;  // Default 1MB
    int force_rebuild = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|snp", (char **)kwlist,
                                     &gz_path, &idx_path, &checkpoint_size,
                                     &force_rebuild)) {
        return -1;
    }

    // Store string references
    self->gz_path = PyUnicode_FromString(gz_path);
    if (!self->gz_path) {
        return -1;
    }

    if (idx_path) {
        self->idx_path = PyUnicode_FromString(idx_path);
    } else {
        // Create default idx path
        PyObject *gz_path_obj = PyUnicode_FromString(gz_path);
        self->idx_path = PyUnicode_FromFormat("%U.idx", gz_path_obj);
        Py_DECREF(gz_path_obj);
    }

    if (!self->idx_path) {
        Py_DECREF(self->gz_path);
        return -1;
    }

    self->checkpoint_size = checkpoint_size;

    // Get C strings
    const char *idx_path_str = PyUnicode_AsUTF8(self->idx_path);
    if (!idx_path_str) {
        return -1;
    }

    // Create indexer handle
    self->handle = dft_indexer_create(gz_path, idx_path_str, checkpoint_size,
                                      force_rebuild);
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create indexer");
        return -1;
    }

    return 0;
}

static PyObject *DFTracerIndexer_build(DFTracerIndexerObject *self,
                                       PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    int result = dft_indexer_build(self->handle);
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to build index");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *DFTracerIndexer_need_rebuild(DFTracerIndexerObject *self,
                                              PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    int result = dft_indexer_need_rebuild(self->handle);
    return PyBool_FromLong(result);
}

static PyObject *DFTracerIndexer_exists(DFTracerIndexerObject *self,
                                        PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    int result = dft_indexer_exists(self->handle);
    return PyBool_FromLong(result);
}

static PyObject *DFTracerIndexer_get_max_bytes(DFTracerIndexerObject *self,
                                               PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    uint64_t result = dft_indexer_get_max_bytes(self->handle);
    return PyLong_FromUnsignedLongLong(result);
}

static PyObject *DFTracerIndexer_get_num_lines(DFTracerIndexerObject *self,
                                               PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    uint64_t result = dft_indexer_get_num_lines(self->handle);
    return PyLong_FromUnsignedLongLong(result);
}

static PyObject *DFTracerIndexer_find_checkpoint(DFTracerIndexerObject *self,
                                                 PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    size_t target_offset;
    if (!PyArg_ParseTuple(args, "n", &target_offset)) {
        return NULL;
    }

    dft_indexer_checkpoint_t checkpoint;
    int found =
        dft_indexer_find_checkpoint(self->handle, target_offset, &checkpoint);

    if (!found) {
        Py_RETURN_NONE;
    }

    // Create IndexCheckpoint object
    IndexCheckpointObject *cp_obj =
        (IndexCheckpointObject *)IndexCheckpoint_new(&IndexCheckpointType, NULL,
                                                     NULL);
    if (!cp_obj) {
        return NULL;
    }

    cp_obj->checkpoint = checkpoint;
    return (PyObject *)cp_obj;
}

static PyObject *DFTracerIndexer_get_checkpoints(DFTracerIndexerObject *self,
                                                 PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    dft_indexer_checkpoint_t *checkpoints = NULL;
    size_t count = 0;

    int result =
        dft_indexer_get_checkpoints(self->handle, &checkpoints, &count);
    if (result != 0 || !checkpoints) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get checkpoints");
        return NULL;
    }

    PyObject *list = PyList_New(count);
    if (!list) {
        free(checkpoints);
        return NULL;
    }

    for (size_t i = 0; i < count; i++) {
        IndexCheckpointObject *cp_obj =
            (IndexCheckpointObject *)IndexCheckpoint_new(&IndexCheckpointType,
                                                         NULL, NULL);
        if (!cp_obj) {
            Py_DECREF(list);
            free(checkpoints);
            return NULL;
        }
        cp_obj->checkpoint = checkpoints[i];
        PyList_SetItem(list, i, (PyObject *)cp_obj);
    }

    free(checkpoints);
    return list;
}

static PyObject *DFTracerIndexer_gz_path(DFTracerIndexerObject *self,
                                         void *closure) {
    Py_INCREF(self->gz_path);
    return self->gz_path;
}

static PyObject *DFTracerIndexer_idx_path(DFTracerIndexerObject *self,
                                          void *closure) {
    Py_INCREF(self->idx_path);
    return self->idx_path;
}

static PyObject *DFTracerIndexer_checkpoint_size(DFTracerIndexerObject *self,
                                                 void *closure) {
    return PyLong_FromSize_t(self->checkpoint_size);
}

static PyMethodDef DFTracerIndexer_methods[] = {
    {"build", (PyCFunction)DFTracerIndexer_build, METH_NOARGS,
     "Build or rebuild the index"},
    {"need_rebuild", (PyCFunction)DFTracerIndexer_need_rebuild, METH_NOARGS,
     "Check if a rebuild is needed"},
    {"exists", (PyCFunction)DFTracerIndexer_exists, METH_NOARGS,
     "Check if the index file exists"},
    {"get_max_bytes", (PyCFunction)DFTracerIndexer_get_max_bytes, METH_NOARGS,
     "Get the maximum uncompressed bytes in the indexed file"},
    {"get_num_lines", (PyCFunction)DFTracerIndexer_get_num_lines, METH_NOARGS,
     "Get the total number of lines in the indexed file"},
    {"find_checkpoint", (PyCFunction)DFTracerIndexer_find_checkpoint,
     METH_VARARGS, "Find the best checkpoint for a given uncompressed offset"},
    {"get_checkpoints", (PyCFunction)DFTracerIndexer_get_checkpoints,
     METH_NOARGS, "Get all checkpoints for this file as a list"},
    {NULL} /* Sentinel */
};

static PyGetSetDef DFTracerIndexer_getsetters[] = {
    {"gz_path", (getter)DFTracerIndexer_gz_path, NULL, "Path to the gzip file",
     NULL},
    {"idx_path", (getter)DFTracerIndexer_idx_path, NULL,
     "Path to the index file", NULL},
    {"checkpoint_size", (getter)DFTracerIndexer_checkpoint_size, NULL,
     "Checkpoint size in bytes", NULL},
    {NULL} /* Sentinel */
};

PyTypeObject DFTracerIndexerType = {
    PyVarObject_HEAD_INIT(NULL, 0) "indexer.DFTracerIndexer", /* tp_name */
    sizeof(DFTracerIndexerObject),                            /* tp_basicsize */
    0,                                                        /* tp_itemsize */
    (destructor)DFTracerIndexer_dealloc,                      /* tp_dealloc */
    0,                                        /* tp_vectorcall_offset */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_as_async */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    "DFTracerIndexer objects",                /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    DFTracerIndexer_methods,                  /* tp_methods */
    0,                                        /* tp_members */
    DFTracerIndexer_getsetters,               /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    (initproc)DFTracerIndexer_init,           /* tp_init */
    0,                                        /* tp_alloc */
    DFTracerIndexer_new,                      /* tp_new */
};

// Module definition
static PyModuleDef indexermodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "indexer",
    .m_doc = "DFTracer indexer module",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit_indexer(void) {
    PyObject *m;

    if (PyType_Ready(&IndexCheckpointType) < 0) return NULL;

    if (PyType_Ready(&DFTracerIndexerType) < 0) return NULL;

    m = PyModule_Create(&indexermodule);
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

    return m;
}
