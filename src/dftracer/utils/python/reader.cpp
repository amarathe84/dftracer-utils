#include <Python.h>
#include <dftracer/utils/python/pylist_line_processor.h>
#include <dftracer/utils/python/reader.h>
#include <dftracer/utils/python/json.h>
#include <dftracer/utils/python/lazy_json_line_processor.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/timer.h>
#include <structmember.h>

#include <cstring>

static void DFTracerReader_dealloc(DFTracerReaderObject *self) {
    if (self->handle) {
        dft_reader_destroy(self->handle);
    }
    Py_XDECREF(self->gz_path);
    Py_XDECREF(self->idx_path);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *DFTracerReader_new(PyTypeObject *type, PyObject *args,
                                    PyObject *kwds) {
    DFTracerReaderObject *self;
    self = (DFTracerReaderObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->handle = NULL;
        self->gz_path = NULL;
        self->idx_path = NULL;
        self->checkpoint_size = 1024 * 1024;
        self->buffer_size = 1024 * 1024;
    }
    return (PyObject *)self;
}

static int DFTracerReader_init(DFTracerReaderObject *self, PyObject *args,
                               PyObject *kwds) {
    static const char *kwlist[] = {"gz_path", "idx_path", "checkpoint_size",
                                   "indexer", NULL};
    const char *gz_path;
    const char *idx_path = NULL;
    size_t checkpoint_size = 1024 * 1024;
    DFTracerIndexerObject *indexer = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|snO", (char **)kwlist,
                                     &gz_path, &idx_path, &checkpoint_size,
                                     &indexer)) {
        return -1;
    }

    self->gz_path = PyUnicode_FromString(gz_path);
    if (!self->gz_path) {
        return -1;
    }

    if (idx_path) {
        self->idx_path = PyUnicode_FromString(idx_path);
    } else {
        PyObject *gz_path_obj = PyUnicode_FromString(gz_path);
        self->idx_path = PyUnicode_FromFormat("%U.idx", gz_path_obj);
        Py_DECREF(gz_path_obj);
    }

    if (!self->idx_path) {
        Py_DECREF(self->gz_path);
        return -1;
    }

    self->checkpoint_size = checkpoint_size;

    if (indexer && indexer->handle) {
        self->handle = dft_reader_create_with_indexer(indexer->handle);
    } else {
        const char *idx_path_str = PyUnicode_AsUTF8(self->idx_path);
        if (!idx_path_str) {
            return -1;
        }
        self->handle =
            dft_reader_create(gz_path, idx_path_str, checkpoint_size);
    }

    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create reader");
        return -1;
    }

    return 0;
}

static PyObject *DFTracerReader_get_max_bytes(DFTracerReaderObject *self,
                                              PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t max_bytes;
    int result = dft_reader_get_max_bytes(self->handle, &max_bytes);
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get max bytes");
        return NULL;
    }

    return PyLong_FromSize_t(max_bytes);
}

static PyObject *DFTracerReader_get_num_lines(DFTracerReaderObject *self,
                                              PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t num_lines;
    int result = dft_reader_get_num_lines(self->handle, &num_lines);
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get num lines");
        return NULL;
    }

    return PyLong_FromSize_t(num_lines);
}

static PyObject *DFTracerReader_reset(DFTracerReaderObject *self,
                                      PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    dft_reader_reset(self->handle);
    Py_RETURN_NONE;
}

static PyObject *DFTracerReader_read_into_buffer(DFTracerReaderObject *self,
                                                 PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t start_bytes, end_bytes;
    Py_buffer buffer;

    if (!PyArg_ParseTuple(args, "nny*", &start_bytes, &end_bytes, &buffer)) {
        return NULL;
    }

    if (!PyBuffer_IsContiguous(&buffer, 'C') || buffer.readonly) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError,
                        "Buffer must be contiguous and writable");
        return NULL;
    }

    int bytes_read = dft_reader_read(self->handle, start_bytes, end_bytes,
                                     (char *)buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);

    if (bytes_read < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to read data");
        return NULL;
    }

    return PyLong_FromLong(bytes_read);
}

static PyObject *DFTracerReader_read_line_bytes_into_buffer(
    DFTracerReaderObject *self, PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t start_bytes, end_bytes;
    Py_buffer buffer;

    if (!PyArg_ParseTuple(args, "nny*", &start_bytes, &end_bytes, &buffer)) {
        return NULL;
    }

    if (!PyBuffer_IsContiguous(&buffer, 'C') || buffer.readonly) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError,
                        "Buffer must be contiguous and writable");
        return NULL;
    }

    int bytes_read = dft_reader_read_line_bytes(
        self->handle, start_bytes, end_bytes, (char *)buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);

    if (bytes_read < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to read line bytes data");
        return NULL;
    }

    return PyLong_FromLong(bytes_read);
}

static PyObject *DFTracerReader_read_lines_into_buffer(
    DFTracerReaderObject *self, PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t start_line, end_line;
    Py_buffer buffer;

    if (!PyArg_ParseTuple(args, "nny*", &start_line, &end_line, &buffer)) {
        return NULL;
    }

    if (!PyBuffer_IsContiguous(&buffer, 'C') || buffer.readonly) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError,
                        "Buffer must be contiguous and writable");
        return NULL;
    }

    size_t bytes_written;
    int result =
        dft_reader_read_lines(self->handle, start_line, end_line,
                              (char *)buffer.buf, buffer.len, &bytes_written);
    PyBuffer_Release(&buffer);

    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to read lines data");
        return NULL;
    }

    return PyLong_FromSize_t(bytes_written);
}

static PyObject *DFTracerReader_read(DFTracerReaderObject *self,
                                     PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t start_bytes, end_bytes;
    if (!PyArg_ParseTuple(args, "nn", &start_bytes, &end_bytes)) {
        return NULL;
    }

    size_t buffer_size = self->buffer_size;
    char *buffer = (char *)PyMem_RawMalloc(buffer_size);
    if (!buffer) {
        return PyErr_NoMemory();
    }

    PyObject *result = PyBytes_FromStringAndSize("", 0);
    if (!result) {
        PyMem_RawFree(buffer);
        return NULL;
    }

    int bytes_read;
    while ((bytes_read = dft_reader_read(self->handle, start_bytes, end_bytes,
                                         buffer, buffer_size)) > 0) {
        PyObject *chunk = PyBytes_FromStringAndSize(buffer, bytes_read);
        if (!chunk) {
            PyMem_RawFree(buffer);
            Py_DECREF(result);
            return NULL;
        }

        PyBytes_ConcatAndDel(&result, chunk);
        if (!result) {
            PyMem_RawFree(buffer);
            return NULL;
        }
    }

    PyMem_RawFree(buffer);

    if (bytes_read < 0) {
        Py_DECREF(result);
        PyErr_SetString(PyExc_RuntimeError, "Failed to read data");
        return NULL;
    }

    return result;
}

static PyObject *DFTracerReader_read_lines(DFTracerReaderObject *self,
                                           PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t start_line, end_line;
    if (!PyArg_ParseTuple(args, "nn", &start_line, &end_line)) {
        return NULL;
    }

    if (start_line < 1) {
        PyErr_SetString(PyExc_ValueError,
                        "start_line must be >= 1 (1-based indexing)");
        return NULL;
    }
    if (end_line < start_line) {
        PyErr_SetString(PyExc_ValueError, "end_line must be >= start_line");
        return NULL;
    }

    try {
        PyListLineProcessor processor;
        dftracer::utils::Reader *cpp_reader =
            static_cast<dftracer::utils::Reader *>(self->handle);
        cpp_reader->read_lines_with_processor(start_line, end_line, processor);
        return processor.get_result();
    } catch (const std::exception &e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
}

static PyObject *DFTracerReader_read_line_bytes(DFTracerReaderObject *self,
                                                PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t start_bytes, end_bytes;
    if (!PyArg_ParseTuple(args, "nn", &start_bytes, &end_bytes)) {
        return NULL;
    }

    try {
        PyListLineProcessor processor;
        dftracer::utils::Reader *cpp_reader =
            static_cast<dftracer::utils::Reader *>(self->handle);
        cpp_reader->read_line_bytes_with_processor(start_bytes, end_bytes,
                                                   processor);
        return processor.get_result();
    } catch (const std::exception &e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
}

static PyObject *DFTracerReader_read_line_bytes_json(DFTracerReaderObject *self,
                                                     PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Reader not initialized");
        return NULL;
    }

    size_t start_bytes, end_bytes;
    if (!PyArg_ParseTuple(args, "nn", &start_bytes, &end_bytes)) {
        return NULL;
    }

    try {
        PyLazyJSONLineProcessor processor;
        dftracer::utils::Reader *cpp_reader =
            static_cast<dftracer::utils::Reader *>(self->handle);
        cpp_reader->read_line_bytes_with_processor(start_bytes, end_bytes,
                                                   processor);
        return processor.get_result();
    } catch (const std::exception &e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return NULL;
    }
}

static PyObject *DFTracerReader_gz_path(DFTracerReaderObject *self,
                                        void *closure) {
    Py_INCREF(self->gz_path);
    return self->gz_path;
}

static PyObject *DFTracerReader_idx_path(DFTracerReaderObject *self,
                                         void *closure) {
    Py_INCREF(self->idx_path);
    return self->idx_path;
}

static PyObject *DFTracerReader_checkpoint_size(DFTracerReaderObject *self,
                                                void *closure) {
    return PyLong_FromSize_t(self->checkpoint_size);
}

static PyObject *DFTracerReader_buffer_size(DFTracerReaderObject *self,
                                            void *closure) {
    return PyLong_FromSize_t(self->buffer_size);
}

static int DFTracerReader_set_buffer_size(DFTracerReaderObject *self,
                                          PyObject *value, void *closure) {
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "Cannot delete buffer_size attribute");
        return -1;
    }

    if (!PyLong_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "Buffer size must be an integer");
        return -1;
    }

    size_t new_size = PyLong_AsSize_t(value);
    if (PyErr_Occurred()) {
        return -1;
    }

    if (new_size == 0) {
        PyErr_SetString(PyExc_ValueError, "Buffer size must be greater than 0");
        return -1;
    }

    self->buffer_size = new_size;
    return 0;
}

static PyMethodDef DFTracerReader_methods[] = {
    {"get_max_bytes", (PyCFunction)DFTracerReader_get_max_bytes, METH_NOARGS,
     "Get the maximum byte position available in the file"},
    {"get_num_lines", (PyCFunction)DFTracerReader_get_num_lines, METH_NOARGS,
     "Get the total number of lines in the file"},
    {"reset", (PyCFunction)DFTracerReader_reset, METH_NOARGS,
     "Reset the reader to initial state"},

    {"read", (PyCFunction)DFTracerReader_read, METH_VARARGS,
     "Read raw bytes and return as bytes (start_bytes, end_bytes)"},
    {"read_lines", (PyCFunction)DFTracerReader_read_lines, METH_VARARGS,
     "Read lines and return as list[str] (start_line, end_line)"},
    {"read_line_bytes", (PyCFunction)DFTracerReader_read_line_bytes,
     METH_VARARGS,
     "Read line bytes and return as list[str] (start_bytes, end_bytes)"},
    {"read_line_bytes_json", (PyCFunction)DFTracerReader_read_line_bytes_json,
     METH_VARARGS,
     "Read line bytes and return as list[DFTracerJSON] (start_bytes, end_bytes)"},
    {NULL}};

static PyGetSetDef DFTracerReader_getsetters[] = {
    {"gz_path", (getter)DFTracerReader_gz_path, NULL, "Path to the gzip file",
     NULL},
    {"idx_path", (getter)DFTracerReader_idx_path, NULL,
     "Path to the index file", NULL},
    {"checkpoint_size", (getter)DFTracerReader_checkpoint_size, NULL,
     "Checkpoint size in bytes", NULL},
    {"buffer_size", (getter)DFTracerReader_buffer_size,
     (setter)DFTracerReader_set_buffer_size,
     "Internal buffer size for read operations", NULL},
    {NULL}};

PyTypeObject DFTracerReaderType = {
    PyVarObject_HEAD_INIT(NULL, 0) "reader.DFTracerReader", /* tp_name */
    sizeof(DFTracerReaderObject),                           /* tp_basicsize */
    0,                                                      /* tp_itemsize */
    (destructor)DFTracerReader_dealloc,                     /* tp_dealloc */
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
    "DFTracerReader objects with zero-copy buffer operations", /* tp_doc */
    0,                                                         /* tp_traverse */
    0,                                                         /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    DFTracerReader_methods,        /* tp_methods */
    0,                             /* tp_members */
    DFTracerReader_getsetters,     /* tp_getset */
    0,                             /* tp_base */
    0,                             /* tp_dict */
    0,                             /* tp_descr_get */
    0,                             /* tp_descr_set */
    0,                             /* tp_dictoffset */
    (initproc)DFTracerReader_init, /* tp_init */
    0,                             /* tp_alloc */
    DFTracerReader_new,            /* tp_new */
};

static PyObject *reader_create_buffer(PyObject *self, PyObject *args) {
    size_t size;
    if (!PyArg_ParseTuple(args, "n", &size)) {
        return NULL;
    }

    return PyByteArray_FromStringAndSize(NULL, size);
}

static PyMethodDef reader_module_methods[] = {
    {"create_buffer", reader_create_buffer, METH_VARARGS,
     "Create a writable buffer of specified size for zero-copy operations"},
    {NULL}};

static PyModuleDef readermodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "reader",
    .m_doc = "DFTracer reader module with zero-copy buffer operations",
    .m_size = -1,
    .m_methods = reader_module_methods,
};

PyMODINIT_FUNC PyInit_reader(void) {
    PyObject *m;

    if (PyType_Ready(&DFTracerReaderType) < 0) return NULL;

    m = PyModule_Create(&readermodule);
    if (m == NULL) return NULL;

    Py_INCREF(&DFTracerReaderType);
    if (PyModule_AddObject(m, "DFTracerReader",
                           (PyObject *)&DFTracerReaderType) < 0) {
        Py_DECREF(&DFTracerReaderType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
