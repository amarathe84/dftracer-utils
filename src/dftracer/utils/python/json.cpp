#include <dftracer/utils/python/json.h>
#include <cstring>
#include <iostream>

static void DFTracerJSON_dealloc(DFTracerJSONObject* self) {
    if (self->json_string) {
        delete self->json_string;
    }
    if (self->doc) {
        yyjson_doc_free(self->doc);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* DFTracerJSON_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    DFTracerJSONObject* self;
    self = (DFTracerJSONObject*)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->json_string = nullptr;
        self->doc = nullptr;
        self->parsed = false;
    }
    return (PyObject*)self;
}

static int DFTracerJSON_init(DFTracerJSONObject* self, PyObject* args, PyObject* kwds) {
    const char* json_str;
    if (!PyArg_ParseTuple(args, "s", &json_str)) {
        return -1;
    }
    
    self->json_string = new std::string(json_str);
    self->doc = nullptr;
    self->parsed = false;
    return 0;
}

static bool DFTracerJSON_ensure_parsed(DFTracerJSONObject* self) {
    if (!self->parsed && self->json_string) {
        self->doc = yyjson_read(self->json_string->c_str(), self->json_string->length(), 0);
        self->parsed = true;
        if (!self->doc) {
            PyErr_SetString(PyExc_ValueError, "Failed to parse JSON");
            return false;
        }
    }
    return self->doc != nullptr;
}

static PyObject* DFTracerJSON_contains(DFTracerJSONObject* self, PyObject* key) {
    if (!DFTracerJSON_ensure_parsed(self)) {
        return NULL;
    }
    
    if (!PyUnicode_Check(key)) {
        PyErr_SetString(PyExc_TypeError, "Key must be a string");
        return NULL;
    }
    
    const char* key_str = PyUnicode_AsUTF8(key);
    if (!key_str) {
        return NULL;
    }
    
    yyjson_val* root = yyjson_doc_get_root(self->doc);
    if (!yyjson_is_obj(root)) {
        Py_RETURN_FALSE;
    }
    
    yyjson_val* val = yyjson_obj_get(root, key_str);
    if (val) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static int DFTracerJSON_contains_sq(PyObject* self_obj, PyObject* key) {
    DFTracerJSONObject* self = (DFTracerJSONObject*)self_obj;
    PyObject* result = DFTracerJSON_contains(self, key);
    if (!result) {
        return -1;
    }
    
    int is_true = PyObject_IsTrue(result);
    Py_DECREF(result);
    return is_true;
}

static PyObject* yyjson_val_to_python(yyjson_val* val) {
    if (yyjson_is_null(val)) {
        Py_RETURN_NONE;
    } else if (yyjson_is_bool(val)) {
        if (yyjson_get_bool(val)) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    } else if (yyjson_is_int(val)) {
        return PyLong_FromLongLong(yyjson_get_int(val));
    } else if (yyjson_is_real(val)) {
        return PyFloat_FromDouble(yyjson_get_real(val));
    } else if (yyjson_is_str(val)) {
        return PyUnicode_FromString(yyjson_get_str(val));
    } else if (yyjson_is_arr(val)) {
        size_t idx, max;
        yyjson_val* item;
        PyObject* list = PyList_New(0);
        if (!list) return NULL;
        
        yyjson_arr_foreach(val, idx, max, item) {
            PyObject* py_item = yyjson_val_to_python(item);
            if (!py_item) {
                Py_DECREF(list);
                return NULL;
            }
            if (PyList_Append(list, py_item) < 0) {
                Py_DECREF(py_item);
                Py_DECREF(list);
                return NULL;
            }
            Py_DECREF(py_item);
        }
        return list;
    } else if (yyjson_is_obj(val)) {
        size_t idx, max;
        yyjson_val* key_val, *val_val;
        PyObject* dict = PyDict_New();
        if (!dict) return NULL;
        
        yyjson_obj_foreach(val, idx, max, key_val, val_val) {
            const char* key_str = yyjson_get_str(key_val);
            PyObject* py_key = PyUnicode_FromString(key_str);
            PyObject* py_val = yyjson_val_to_python(val_val);
            
            if (!py_key || !py_val) {
                Py_XDECREF(py_key);
                Py_XDECREF(py_val);
                Py_DECREF(dict);
                return NULL;
            }
            
            if (PyDict_SetItem(dict, py_key, py_val) < 0) {
                Py_DECREF(py_key);
                Py_DECREF(py_val);
                Py_DECREF(dict);
                return NULL;
            }
            
            Py_DECREF(py_key);
            Py_DECREF(py_val);
        }
        return dict;
    }
    
    Py_RETURN_NONE;
}

static PyObject* DFTracerJSON_getitem(DFTracerJSONObject* self, PyObject* key) {
    if (!DFTracerJSON_ensure_parsed(self)) {
        return NULL;
    }
    
    if (!PyUnicode_Check(key)) {
        PyErr_SetString(PyExc_TypeError, "Key must be a string");
        return NULL;
    }
    
    const char* key_str = PyUnicode_AsUTF8(key);
    if (!key_str) {
        return NULL;
    }
    
    yyjson_val* root = yyjson_doc_get_root(self->doc);
    if (!yyjson_is_obj(root)) {
        PyErr_SetString(PyExc_TypeError, "JSON root is not an object");
        return NULL;
    }
    
    yyjson_val* val = yyjson_obj_get(root, key_str);
    if (!val) {
        PyErr_SetString(PyExc_KeyError, key_str);
        return NULL;
    }
    
    return yyjson_val_to_python(val);
}

static PyObject* DFTracerJSON_keys(DFTracerJSONObject* self, PyObject* Py_UNUSED(ignored)) {
    if (!DFTracerJSON_ensure_parsed(self)) {
        return NULL;
    }
    
    yyjson_val* root = yyjson_doc_get_root(self->doc);
    if (!yyjson_is_obj(root)) {
        return PyList_New(0);
    }
    
    PyObject* keys = PyList_New(0);
    if (!keys) return NULL;
    
    size_t idx, max;
    yyjson_val* key_val, *val_val;
    yyjson_obj_foreach(root, idx, max, key_val, val_val) {
        const char* key_str = yyjson_get_str(key_val);
        PyObject* py_key = PyUnicode_FromString(key_str);
        if (!py_key) {
            Py_DECREF(keys);
            return NULL;
        }
        if (PyList_Append(keys, py_key) < 0) {
            Py_DECREF(py_key);
            Py_DECREF(keys);
            return NULL;
        }
        Py_DECREF(py_key);
    }
    
    return keys;
}

static PyObject* DFTracerJSON_get(DFTracerJSONObject* self, PyObject* args) {
    PyObject* key;
    PyObject* default_value = Py_None;
    
    if (!PyArg_ParseTuple(args, "O|O", &key, &default_value)) {
        return NULL;
    }
    
    if (!DFTracerJSON_ensure_parsed(self)) {
        return NULL;
    }
    
    if (!PyUnicode_Check(key)) {
        PyErr_SetString(PyExc_TypeError, "Key must be a string");
        return NULL;
    }
    
    const char* key_str = PyUnicode_AsUTF8(key);
    if (!key_str) {
        return NULL;
    }
    
    yyjson_val* root = yyjson_doc_get_root(self->doc);
    if (!yyjson_is_obj(root)) {
        Py_INCREF(default_value);
        return default_value;
    }
    
    yyjson_val* val = yyjson_obj_get(root, key_str);
    if (!val) {
        Py_INCREF(default_value);
        return default_value;
    }
    
    return yyjson_val_to_python(val);
}

static PyObject* DFTracerJSON_str(DFTracerJSONObject* self) {
    if (self->json_string) {
        return PyUnicode_FromString(self->json_string->c_str());
    }
    return PyUnicode_FromString("{}");
}

static PyObject* DFTracerJSON_repr(DFTracerJSONObject* self) {
    if (self->json_string) {
        return PyUnicode_FromFormat("DFTracerJSON(%s)", self->json_string->c_str());
    }
    return PyUnicode_FromString("DFTracerJSON({})");
}

PyMethodDef DFTracerJSON_methods[] = {
    {"__contains__", (PyCFunction)DFTracerJSON_contains, METH_O,
     "Check if key exists in JSON object"},
    {"keys", (PyCFunction)DFTracerJSON_keys, METH_NOARGS,
     "Get all keys from JSON object"},
    {"get", (PyCFunction)DFTracerJSON_get, METH_VARARGS,
     "Get value by key with optional default"},
    {NULL}
};

PySequenceMethods DFTracerJSON_as_sequence = {
    .sq_contains = DFTracerJSON_contains_sq,
};

PyMappingMethods DFTracerJSON_as_mapping = {
    .mp_subscript = (binaryfunc)DFTracerJSON_getitem,
};

PyTypeObject DFTracerJSONType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "json.DFTracerJSON",                       /* tp_name */
    sizeof(DFTracerJSONObject),                 /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)DFTracerJSON_dealloc,           /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)DFTracerJSON_repr,                /* tp_repr */
    0,                                          /* tp_as_number */
    &DFTracerJSON_as_sequence,                  /* tp_as_sequence */
    &DFTracerJSON_as_mapping,                   /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    (reprfunc)DFTracerJSON_str,                 /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "Lazy JSON object that parses on demand",   /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    DFTracerJSON_methods,                       /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)DFTracerJSON_init,                /* tp_init */
    0,                                          /* tp_alloc */
    DFTracerJSON_new,                           /* tp_new */
};

static PyModuleDef jsonmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "json",
    .m_doc = "Lazy JSON parsing module using yyjson",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit_json(void) {
    PyObject* m;
    
    if (PyType_Ready(&DFTracerJSONType) < 0)
        return NULL;
    
    m = PyModule_Create(&jsonmodule);
    if (m == NULL)
        return NULL;
    
    Py_INCREF(&DFTracerJSONType);
    if (PyModule_AddObject(m, "DFTracerJSON", (PyObject*)&DFTracerJSONType) < 0) {
        Py_DECREF(&DFTracerJSONType);
        Py_DECREF(m);
        return NULL;
    }
    
    return m;
}