/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_PYFLEXIBLE_TYPE_HPP
#define GRAPHLAB_LAMBDA_PYFLEXIBLE_TYPE_HPP

#include <cmath>
#include <boost/python.hpp>
#include <typeinfo>
#include <flexible_type/flexible_type.hpp>
#include <exceptions/error_types.hpp>
#include <lambda/python_import_modules.hpp>

namespace graphlab {

namespace lambda {

/**
 * \ingroup lambda
 *
 * Provide functions that convert between python object
 * and flexible_types.
 *
 * Supported flexible_types are: flex_int, flex_float, flex_string, flex_vec, flex_dict
 * and flex_undefined.
 *
 * To convert from flexible_type to a python object:
 * \code
 * python::object py_int_val = PyObject_FromFlex(3);
 * python::object py_float_val = PyObject_FromFlex(3.0);
 * python::object py_str_val = PyObject_FromFlex("foo");
 * python::object py_vec_val = PyObject_FromFlex(array.array('i', [1,2,3]));
 * python::object py_recursive_val = PyObject_FromFlex([1,2,3]);
 * python::object py_dict_val = PyObject_FromFlex([{'a':1}, {1: 1.0}]);
 * python::object py_none = PyObject_FromFlex(flex_undefined());
 * \endcode
 *
 * To convert from python object to flexible_type:
 * \code
 * flexible_type int_flex_val = PyObject_AsFlex(py_int_val);
 * flexible_type float_flex_val = PyObject_AsFlex(py_float_val);
 * flexible_type str_flex_val = PyObject_AsFlex(py_str_val);
 * flexible_type vec_flex_val = PyObject_AsFlex(py_vec_val);
 * flexible_type recursive_flex_val = PyObject_AsFlex(py_vec_val);
 * flexible_type dict_flex_val = PyObject_AsFlex(py_dict_val);
 * flexible_type undef_flex_val = PyObject_AsFlex(py_none);
 * \endcode
 *
 * Or calling the faster overload avoiding allocation.
 * \code
 * flexible_type out;
 * PyObject_AsFlex(py_int_val, out);
 * PyObject_AsFlex(py_float_val, out);
 * PyObject_AsFlex(py_str_val, out);
 * PyObject_AsFlex(py_vec_val, out);
 * PyObject_AsFlex(py_vec_val, out);
 * PyObject_AsFlex(py_dict_val, out);
 * PyObject_AsFlex(py_none, out);
 * \endcode
 *
 * To convert from a list of flexible_type, use \ref PyObject_FromFlexList().
 */

namespace python = boost::python;


////////////////// FLEXIBLE TYPE TO PYTHON OBJECT /////////////////////// 

python::object PyObject_FromFlex(const flexible_type& flex_value);

/**
 * Converting basic flexible types into boost python object.
 *
 * Throws graphlab::bad_cast on type failure.
 */
struct PyObjectVisitor {

  inline python::object operator()(const flex_int& i) const {

#if PY_MAJOR_VERSION < 3
    return python::object(python::handle<>(PyInt_FromSsize_t(i)));
#else
    return python::object(python::handle<>(PyLong_FromSsize_t(i)));
#endif
  }

  inline python::object operator()(const flex_float& i) const {
    return python::object(python::handle<>(PyFloat_FromDouble(i)));
  }

  inline python::object operator()(const flex_string& i) const {
#if PY_MAJOR_VERSION < 3
    return python::object(python::handle<>(PyString_FromString(i.c_str())));
#else
    return python::object(python::handle<>(PyBytes_FromString(i.c_str())));
#endif
  }
  inline python::object operator()(const flex_date_time& i) const {
    boost::python::dict epoch_kwargs;
    boost::python::list emptyList;

#if PY_MAJOR_VERSION < 3
    epoch_kwargs["year"] = python::object(python::handle<>(PyInt_FromSsize_t((ssize_t)1970)));
    epoch_kwargs["month"] = python::object(python::handle<>(PyInt_FromSsize_t((ssize_t)1)));
    epoch_kwargs["day"] = python::object(python::handle<>(PyInt_FromSsize_t((ssize_t)1)));
#else
    epoch_kwargs["year"] = python::object(python::handle<>(PyLong_FromSsize_t((ssize_t)1970)));
    epoch_kwargs["month"] = python::object(python::handle<>(PyLong_FromSsize_t((ssize_t)1)));
    epoch_kwargs["day"] = python::object(python::handle<>(PyLong_FromSsize_t((ssize_t)1)));
#endif

    python::object utc = py_datetime(*python::tuple(emptyList), **epoch_kwargs);

    boost::python::dict delta_kwargs;
#if PY_MAJOR_VERSION < 3
    delta_kwargs["seconds"] = python::object(python::handle<>(PyInt_FromSsize_t(i.posix_timestamp())));
    delta_kwargs["microseconds"] = python::object(python::handle<>(PyInt_FromSsize_t(i.microsecond())));
#else
    delta_kwargs["seconds"] = python::object(python::handle<>(PyLong_FromSsize_t(i.posix_timestamp())));
    delta_kwargs["microseconds"] = python::object(python::handle<>(PyLong_FromSsize_t(i.microsecond())));
#endif

    utc += py_datetime_module.attr("timedelta")(*python::tuple(emptyList), **delta_kwargs);

    if (i.time_zone_offset() != flex_date_time::EMPTY_TIMEZONE) {
      python::object GMT = py_gl_timezone.attr("GMT");
      python::object to_zone = 
          GMT(
              python::object(
                  python::handle<>(
                      PyFloat_FromDouble(i.time_zone_offset() * 
                                         flex_date_time::TIMEZONE_RESOLUTION_IN_HOURS))));
      boost::python::dict kwargs;
      kwargs["tzinfo"] = GMT(python::object(python::handle<>(PyFloat_FromDouble(0))));
      utc = utc.attr("replace")(*python::tuple(emptyList),**kwargs);
      return utc.attr("astimezone")(to_zone);
    } else {
      return utc;
    }

  } 
  inline python::object operator()(const flex_vec& i) const {

    // construct an array object
    python::object array = py_array.attr("array")(python::object("d"));

    for (const auto& v: i) {
      array.attr("append")(v);
    }
    return array;
  }

  inline python::object operator()(const flex_list& i) const {
    python::list l;
    for (const auto& v: i) l.append(lambda::PyObject_FromFlex(v));
    return l;
  }

  inline python::object operator()(const flex_dict& i) const {
    python::dict d;
    for (const auto& v: i) {
      d[lambda::PyObject_FromFlex(v.first)] = lambda::PyObject_FromFlex(v.second);
    }
    return d;
  }

  inline python::object operator()(const flex_undefined& i) const {
    return python::object();
  }

  inline python::object operator()(const flexible_type& t) const {
    throw bad_cast("Cannot convert flexible_type " +
                   std::string(flex_type_enum_to_name(t.get_type())) +
                   " to python object.");
  }

  inline python::object operator()(const flex_image& i) const {

    const unsigned char* c_image_data = i.get_image_data();

    if (c_image_data == NULL){
        logstream(LOG_WARNING) << "Trying to apply lambda to flex_image with NULL data pointer" << std::endl;
    }
    PyObject* bytearray_ptr = PyByteArray_FromStringAndSize((const char*)c_image_data,i.m_image_data_size);

    boost::python::list arguments;

    boost::python::dict kwargs;
    kwargs["_image_data"] =  boost::python::handle<>(bytearray_ptr);
    kwargs["_height"] = i.m_height;
    kwargs["_width"] = i.m_width;
    kwargs["_channels"] = i.m_channels;
    kwargs["_image_data_size"] = i.m_image_data_size;
    kwargs["_version"] = static_cast<int>(i.m_version);
    kwargs["_format_enum"] = static_cast<int>(i.m_format);


    python::object img = py_gl_image_class(*python::tuple(arguments), **kwargs);
    return img;
  }

  template<typename T>
  inline python::object operator()(const T& t) const {
    throw bad_cast("Cannot convert non-flexible_type to python object.");
  }
};

/**
 * \ingroup lambda
 *
 * Convert basic flexible types into boost python object.
 * Throws exception on failure.
 */
inline python::object PyObject_FromFlex(const flexible_type& flex_value) {
  return flex_value.apply_visitor(PyObjectVisitor());
}

/*
 * \ingroup lambda
 *
 * Convert vector of flexible types into boost python list object.
 * Throws exception on failure.
 */
inline python::object PyObject_FromFlexList(const std::vector<flexible_type>& flex_list) {
  python::list l;
  for (const auto& v: flex_list) {
    l.append(PyObject_FromFlex(v));
  }
  return l;
}

/**
 * Update the given dictionary with given key and value vectors.
 */
inline python::dict& PyDict_UpdateFromFlex(python::dict& d,
                                           const std::vector<std::string>& keys,
                                           const std::vector<graphlab::flexible_type>& values,
                                           bool erase_existing_keys = true) {
  DASSERT_EQ(keys.size(), values.size());
  if (erase_existing_keys)
    d.clear();
  for (size_t i = 0; i < values.size(); ++i) {
    d[keys[i]] = PyObject_FromFlex(values[i]);
  }
  return d;
}

/**
 * Update the given list with the value vectors. Assuming the length are same.
 */
inline python::list& PyList_UpdateFromFlex(python::list& ls,
                                           const std::vector<graphlab::flexible_type>& values) {
  if (values.size() == 0) {
    return ls;
  }

  DASSERT_EQ(values.size(), python::len(ls));
  for (size_t i = 0; i < values.size(); ++i) {
    ls[i] = PyObject_FromFlex(values[i]);
  }
  return ls;
}


////////////////// PYTHON OBJECT TO FLEXIBLE TYPE /////////////////////// 


/// Converter for simple types: int, float, string, and undefined
inline bool Simple_PyObject_AsFlex(PyObject* objectptr,
                                   flexible_type& out) {
  PyTypeObject* ob_type = objectptr->ob_type;

  bool success = false;

  // Int, Long, Bool -> flex_int 
#if PY_MAJOR_VERSION < 3
  if (PyInt_Check(objectptr)) {
    out = PyInt_AsLong(objectptr);
#else
  if (PyLong_Check(objectptr)) {
    out = PyLong_AsLong(objectptr);
#endif

    success = true;
  } else if (PyLong_Check(objectptr)) {
    out = PyLong_AsLong(objectptr);
    success = true;
  // Float -> flex_float
  } else if (PyFloat_Check(objectptr)) {
    out = PyFloat_AsDouble(objectptr);
    success = true;
  // String -> flex_string
  } 

#if PY_MAJOR_VERSION < 3
  else if (ob_type == &PyString_Type) {
    char* c; Py_ssize_t len;
    PyString_AsStringAndSize(objectptr, &c, &len);
#else
    else if (ob_type == &PyBytes_Type) {
      char* c; Py_ssize_t len;
      PyBytes_AsStringAndSize(objectptr, &c, &len);
#endif

    if (out.get_type() != flex_type_enum::STRING)
      out = flexible_type(flex_type_enum::STRING);
    out.mutable_get<flex_string>() = std::string(c, len);
    success = true;
  // None -> flex_undefined
  } else if (objectptr == Py_None) {
    out = FLEX_UNDEFINED;
    success = true;
  }
  return success;
}


/////////////////// List of Type Converters in cpp ///////////////////
// Fast pass for converting simple types: None, Int, Float, String and Unicode
bool Simple_PyObject_AsFlex(PyObject* objectptr, flexible_type& out);

// Convert dict
bool PyDict_AsFlex(const python::object& object, flexible_type& out);

// Convert tuple
bool PyTuple_AsFlex(const python::object& object, flexible_type& out);

// Convert list
bool PyList_AsFlex(const python::object& object, flexible_type& out, bool _as_array = false);

// Convert array
bool PyArray_AsFlex(const python::object& object, flexible_type& out);

// Convert list like object (ndarray)
bool PyOtherList_AsFlex(const python::object& object, flexible_type& out);

// Convert Image
bool PyImage_AsFlex(const python::object& object, flexible_type& out);

// Convert Datetime
bool PyDateTime_AsFlex(const python::object& object, flexible_type& out);

// Convert Unicode
bool PyUnicode_AsFlex(const python::object& object, flexible_type& out);

// Old logic as fall back
bool _Old_PyObject_AsFlex(const python::object& object, flexible_type& out);

/**
 * \ingroup lambda
 *
 * Extract flexible types from boost python objects.
 * Throws exception on failure.
 */
inline void PyObject_AsFlex(const python::object& object, flexible_type& out) {
  if (Simple_PyObject_AsFlex(object.ptr(), out)) {}
  else if (PyList_AsFlex(object, out)) {}
  else if (PyDict_AsFlex(object, out)) {}
  else if (PyTuple_AsFlex(object, out)) {}
  else if (PyUnicode_AsFlex(object, out)) {}
  else if (PyImage_AsFlex(object, out)) {}
  else if (PyDateTime_AsFlex(object, out)) {}
  else if (PyArray_AsFlex(object, out)) {}
  else if (PyOtherList_AsFlex(object, out)) {}
  else if (_Old_PyObject_AsFlex(object, out)) {}
  else {
    std::string tname = python::extract<std::string>(object.attr("__class__").attr("__name__"));
    throw bad_cast("Cannot convert python object " + tname + " to flexible_type.");
  }
}

/**
 * \overload
 */
inline flexible_type PyObject_AsFlex(const python::object& object) {
  flexible_type ret;
  PyObject_AsFlex(object, ret);
  return ret;
}


} // end lambda
} // end graphlab

// workaround python bug http://bugs.python.org/issue10910
#undef isspace
#undef isupper
#undef toupper
#endif
