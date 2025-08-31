#ifndef DFTRACER_UTILS_PYTHON_JSON_LINE_PROCESSOR_H
#define DFTRACER_UTILS_PYTHON_JSON_LINE_PROCESSOR_H

#include <Python.h>
#include <dftracer/utils/reader/line_processor.h>
#include <simdjson.h>

#include <cstdio>

class JsonLineProcessor : public dftracer::utils::LineProcessor {
   private:
    PyObject* py_list_;
    simdjson::ondemand::parser parser_;

   public:
    JsonLineProcessor() : py_list_(PyList_New(0)) {
        if (!py_list_) {
            throw std::runtime_error("Failed to create Python list");
        }
    }

    ~JsonLineProcessor() { Py_XDECREF(py_list_); }

    bool process(const char* data, std::size_t length) override {
        try {
            // Parse JSON using simdjson
            simdjson::padded_string json_str(data, length);
            simdjson::ondemand::document doc = parser_.iterate(json_str);

            // Convert simdjson document to Python dict
            PyObject* py_dict = convert_to_python_dict(doc);
            if (!py_dict) {
                return false;
            }

            int result = PyList_Append(py_list_, py_dict);
            Py_DECREF(py_dict);
            return result == 0;

        } catch (const simdjson::simdjson_error& e) {
            // Fallback: if JSON parsing fails, store as string
            PyObject* py_line = PyUnicode_FromStringAndSize(data, length);
            if (!py_line) {
                return false;
            }
            int result = PyList_Append(py_list_, py_line);
            Py_DECREF(py_line);
            return result == 0;
        }
    }

    PyObject* get_result() {
        if (!py_list_) {
            Py_RETURN_NONE;
        }
        Py_INCREF(py_list_);
        return py_list_;
    }

    std::size_t size() const { return py_list_ ? PyList_Size(py_list_) : 0; }

   private:
    PyObject* convert_to_python_dict(simdjson::ondemand::document& doc) {
        try {
            simdjson::ondemand::object obj = doc.get_object();
            PyObject* py_dict = PyDict_New();
            if (!py_dict) {
                return NULL;
            }

            for (auto field : obj) {
                std::string_view key = field.unescaped_key();
                PyObject* py_key =
                    PyUnicode_FromStringAndSize(key.data(), key.size());
                if (!py_key) {
                    Py_DECREF(py_dict);
                    return NULL;
                }

                PyObject* py_value = convert_json_value(field.value());
                if (!py_value) {
                    Py_DECREF(py_key);
                    Py_DECREF(py_dict);
                    return NULL;
                }

                if (PyDict_SetItem(py_dict, py_key, py_value) < 0) {
                    Py_DECREF(py_key);
                    Py_DECREF(py_value);
                    Py_DECREF(py_dict);
                    return NULL;
                }

                Py_DECREF(py_key);
                Py_DECREF(py_value);
            }

            return py_dict;
        } catch (const simdjson::simdjson_error& e) {
            return NULL;
        }
    }

    PyObject* convert_json_value(simdjson::ondemand::value value) {
        try {
            switch (value.type()) {
                case simdjson::ondemand::json_type::null:
                    Py_RETURN_NONE;

                case simdjson::ondemand::json_type::boolean: {
                    bool b = value.get_bool();
                    return PyBool_FromLong(b ? 1 : 0);
                }

                case simdjson::ondemand::json_type::number: {
                    // Try int64 first, then double
                    simdjson::ondemand::number num = value.get_number();
                    if (num.is_integer()) {
                        int64_t i = num.get_int64();
                        return PyLong_FromLongLong(i);
                    } else {
                        double d = num.get_double();
                        return PyFloat_FromDouble(d);
                    }
                }

                case simdjson::ondemand::json_type::string: {
                    std::string_view str = value.get_string();
                    return PyUnicode_FromStringAndSize(str.data(), str.size());
                }

                case simdjson::ondemand::json_type::array: {
                    simdjson::ondemand::array arr = value.get_array();
                    PyObject* py_list = PyList_New(0);
                    if (!py_list) {
                        return NULL;
                    }

                    for (auto element : arr) {
                        PyObject* py_element = convert_json_value(element);
                        if (!py_element) {
                            Py_DECREF(py_list);
                            return NULL;
                        }

                        if (PyList_Append(py_list, py_element) < 0) {
                            Py_DECREF(py_element);
                            Py_DECREF(py_list);
                            return NULL;
                        }

                        Py_DECREF(py_element);
                    }

                    return py_list;
                }

                case simdjson::ondemand::json_type::object: {
                    simdjson::ondemand::object obj = value.get_object();
                    PyObject* py_dict = PyDict_New();
                    if (!py_dict) {
                        return NULL;
                    }

                    for (auto field : obj) {
                        std::string_view key = field.unescaped_key();
                        PyObject* py_key =
                            PyUnicode_FromStringAndSize(key.data(), key.size());
                        if (!py_key) {
                            Py_DECREF(py_dict);
                            return NULL;
                        }

                        PyObject* py_val = convert_json_value(field.value());
                        if (!py_val) {
                            Py_DECREF(py_key);
                            Py_DECREF(py_dict);
                            return NULL;
                        }

                        if (PyDict_SetItem(py_dict, py_key, py_val) < 0) {
                            Py_DECREF(py_key);
                            Py_DECREF(py_val);
                            Py_DECREF(py_dict);
                            return NULL;
                        }

                        Py_DECREF(py_key);
                        Py_DECREF(py_val);
                    }

                    return py_dict;
                }

                default:
                    Py_RETURN_NONE;
            }
        } catch (const simdjson::simdjson_error& e) {
            return NULL;
        }
    }
};

#endif  // DFTRACER_UTILS_PYTHON_JSON_LINE_PROCESSOR_H
