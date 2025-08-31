#ifndef DFTRACER_UTILS_PYTHON_JSON_H
#define DFTRACER_UTILS_PYTHON_JSON_H

#include <Python.h>
#include <string>
#include <memory>
#include <yyjson.h>

typedef struct {
    PyObject_HEAD
    std::string* json_string;
    mutable yyjson_doc* doc;
    mutable bool parsed;
} DFTracerJSONObject;

extern PyTypeObject DFTracerJSONType;

static void DFTracerJSON_dealloc(DFTracerJSONObject* self);
static PyObject* DFTracerJSON_new(PyTypeObject* type, PyObject* args, PyObject* kwds);
static int DFTracerJSON_init(DFTracerJSONObject* self, PyObject* args, PyObject* kwds);

static PyObject* DFTracerJSON_contains(DFTracerJSONObject* self, PyObject* key);
static PyObject* DFTracerJSON_getitem(DFTracerJSONObject* self, PyObject* key);
static int DFTracerJSON_contains_sq(PyObject* self, PyObject* key);

static PyObject* DFTracerJSON_keys(DFTracerJSONObject* self, PyObject* Py_UNUSED(ignored));
static PyObject* DFTracerJSON_get(DFTracerJSONObject* self, PyObject* args);
static PyObject* DFTracerJSON_str(DFTracerJSONObject* self);
static PyObject* DFTracerJSON_repr(DFTracerJSONObject* self);

extern PyMethodDef DFTracerJSON_methods[];
extern PySequenceMethods DFTracerJSON_as_sequence;
extern PyMappingMethods DFTracerJSON_as_mapping;

#endif // DFTRACER_UTILS_PYTHON_JSON_H