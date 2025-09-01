#ifndef DFTRACER_UTILS_PYTHON_JSON_H
#define DFTRACER_UTILS_PYTHON_JSON_H

#include <Python.h>
#include <yyjson.h>

#include <cstdint>
#include <memory>
#include <string>

typedef struct {
    PyObject_HEAD mutable yyjson_doc* doc;
    mutable bool parsed;
    std::size_t json_length;
    char json_data[];  // Flexible array member - data stored inline
} JSONObject;

extern PyTypeObject JSONType;

extern PyMethodDef JSON_methods[];
extern PySequenceMethods JSON_as_sequence;
extern PyMappingMethods JSON_as_mapping;

int init_json(PyObject* m);

PyObject* JSON_from_data(const char* data, size_t length);

#endif  // DFTRACER_UTILS_PYTHON_JSON_H
