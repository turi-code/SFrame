/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/pyflexible_type.hpp>
#include <datetime.h>

namespace graphlab {
namespace lambda {

// Convert dict
bool PyDict_AsFlex(const python::object& object, flexible_type& out) {
  if (PyDict_Check(object.ptr())) {
    python::dict dict_val(object);
    python::list keys = dict_val.keys();

    if (out.get_type() != flex_type_enum::DICT) {
      out = flexible_type(flex_type_enum::DICT);
    }
    flex_dict& x = out.mutable_get<flex_dict>();
    x.resize(len(keys));

    for(size_t i = 0; i < (size_t)len(keys); i++) {
      PyObject_AsFlex(keys[i], x[i].first);
      PyObject_AsFlex(dict_val[keys[i]], x[i].second);
    }
    return true;
  }
  return false;
}

// Convert tuple
bool PyTuple_AsFlex(const python::object& object, flexible_type& out) {
  if (PyTuple_Check(object.ptr())) { 
    python::tuple tuple_val(object);
    if (out.get_type() != flex_type_enum::LIST) {
      out = flexible_type(flex_type_enum::LIST);
    }
    flex_list& x = out.mutable_get<flex_list>();
    x.resize(len(tuple_val));
    for(size_t i = 0; i < (size_t)len(tuple_val); i++) {
      PyObject_AsFlex(tuple_val[i], x[i]);
    }
    return true;
  }
  return false;
}

// Convert list
// TODO: Optimize
bool PyList_AsFlex(const python::object& object, flexible_type& out, bool _as_array) {
  auto objectptr = object.ptr();
  if (PyList_Check(objectptr)) {
    // We try hard to convert return list to vector(numeric), only when we encounter the first
    // non-numeric value, would we convert the return value to recursive type.

    // Base case: empty list, return empty list (not empty array)
    size_t list_size = PyList_Size(object.ptr());
    if (list_size == 0) {
      out = flexible_type(flex_type_enum::LIST);
      return true;
    }

    bool all_numeric = true;

    flex_list ret_recursive;
    flex_vec ret_vector;

    for (size_t i = 0; i < list_size; ++i) {
      PyObject* a = PyList_GetItem(objectptr, i);
      if (a == NULL) {
        throw bad_cast("Unable to read resultant Python list");
      }

      // first time encounter non-numeric value, move from flex_vec to flex_list
      if (!_as_array && all_numeric && !PyNumber_Check(a)) {
        all_numeric = false;

        // move all items from ret_vector to ret_recursive
        for (size_t j = 0; j < ret_vector.size(); ++j) {
          PyObject* ret_vector_read_obj = PyList_GetItem(objectptr, j); 
          ret_recursive.push_back(PyObject_AsFlex(python::object(python::handle<>(python::borrowed(ret_vector_read_obj)))));
        }
        ret_vector.clear();
      }

      if (all_numeric) {
        ret_vector.push_back((flex_float)PyObject_AsFlex(python::object(python::handle<>(python::borrowed(a)))));
      } else {
        ret_recursive.push_back(PyObject_AsFlex(python::object(python::handle<>(python::borrowed(a)))));
      }
    }

    if (all_numeric) {
      out = std::move(ret_vector);
    } else {
      out = std::move(ret_recursive);
    }
    return true;
  }
  return false;
}

// Convert array
// TODO: optimize
bool PyArray_AsFlex(const python::object& object, flexible_type& out) {
  python::object array_type_obj = py_array.attr("ArrayType");
  PyTypeObject* array_type = (PyTypeObject*)(array_type_obj.ptr());
  if (object.ptr()->ob_type == array_type) {
    // this is not the most efficient way, but its should be the most robust.
    PyList_AsFlex(object.attr("tolist")(), out, true /* _as_array */);
    return true;
  }
  return false;
}

// Convert other list like object (numpy array)
// TODO: optimize
bool PyOtherList_AsFlex(const python::object& object, flexible_type& out) {
  if (PyObject_HasAttrString(object.ptr(), "tolist")) {
    // this is not the most efficient way, but its should be the most robust.
    PyList_AsFlex(object.attr("tolist")(), out);
    return true;
  }
  return false;
}

// Convert Image
bool PyImage_AsFlex(const python::object& object, flexible_type& out) {
  if (PyObject_HasAttrString(object.ptr(), "_image_data")) {
    if (out.get_type() != flex_type_enum::IMAGE) {
      out = flexible_type(flex_type_enum::IMAGE);
    }
    flex_image& img = out.mutable_get<flex_image>();
    int format_enum;
    img.m_image_data_size = python::extract<size_t>(object.attr("_image_data_size"));
    if (img.m_image_data_size > 0){
      python::object o = object.attr("_image_data");         
      char* buf = new char[img.m_image_data_size];
      memcpy(buf, PyByteArray_AsString(o.ptr()), img.m_image_data_size);
      img.m_image_data.reset(buf);
    }
    img.m_height = python::extract<size_t>(object.attr("_height"));
    img.m_width = python::extract<size_t>(object.attr("_width"));
    img.m_channels = python::extract<size_t>(object.attr("_channels"));
    img.m_version = python::extract<int>(object.attr("_version"));
    format_enum = python::extract<int>(object.attr("_format_enum"));
    img.m_format = static_cast<Format>(format_enum);
    return true;
  }
  return false;
}

// Convert Datetime
bool PyDateTime_AsFlex(const python::object& object, flexible_type& out) {
  // we need to import this macro for PyDateTime_Check()
  PyDateTime_IMPORT;
  auto objectptr = object.ptr();
  if (PyDateTime_Check(objectptr)) {
    if(PyDateTime_GET_YEAR(objectptr) < 1400 or PyDateTime_GET_YEAR(objectptr) > 10000) { 
      throw("Year is out of valid range: 1400..10000");
    }
    if(object.attr("tzinfo") != python::object()) {
      //store timezone offset at the granularity of half an hour 
      int offset = (int)python::extract<flex_float>(object.attr("tzinfo").attr("utcoffset")(object).attr("total_seconds")()) / flex_date_time::TIMEZONE_RESOLUTION_IN_SECONDS;
      out = flex_date_time(python::extract<int64_t>(py_timegm(object.attr("utctimetuple")())),offset, python::extract<int64_t>(object.attr("microsecond")));
    } else {
      out = flex_date_time(python::extract<int64_t>(py_timegm(object.attr("utctimetuple")())),flex_date_time::EMPTY_TIMEZONE, python::extract<int64_t>(object.attr("microsecond")));
    }
    return true;
  }
  return false;
}

// convert unicode
bool PyUnicode_AsFlex(const python::object& object, flexible_type& out) {;
  if (PyUnicode_Check(object.ptr())) {
    if (out.get_type() != flex_type_enum::STRING)
      out = flexible_type(flex_type_enum::STRING);
    out.mutable_get<flex_string>() = python::extract<flex_string>(object.attr("encode")("utf-8"));
    return true;
  }
  return false;
}

/**
 * Old legacy logic as fall back in case everything else failed
 */
bool _Old_PyObject_AsFlex(const python::object& object, flexible_type& out) {
  PyDateTime_IMPORT; //we need to import this macro for PyDateTime_Check()
  PyObject* objectptr = object.ptr();

#if PY_MAJOR_VERSION < 3
  if (PyInt_Check(objectptr) || PyLong_Check(objectptr)) {
    out = (flex_int)(python::extract<flex_int>(object));
  } 
#else
  if (PyLong_Check(objectptr)) {
    out = (flex_int)(python::extract<flex_int>(object));
  } 
#endif

  else if (PyFloat_Check(objectptr)) {
    out = (flex_float)(python::extract<flex_float>(object));
  } 

#if PY_MAJOR_VERSION < 3
  else if (PyString_Check(objectptr)) {
    out = python::extract<flex_string>(object);
  } 
#endif

else if (PyUnicode_Check(objectptr)) {
    out = python::extract<flex_string>(object.attr("encode")("utf-8"));
  } else if (PyDateTime_Check(objectptr)) { 
    if(PyDateTime_GET_YEAR(objectptr) < 1400 or PyDateTime_GET_YEAR(objectptr) > 10000) { 
         throw("Year is out of valid range: 1400..10000");
    }
    if(object.attr("tzinfo") != python::object()) {
         //store timezone offset at the granularity of half an hour 
         int offset = (int)python::extract<flex_float>(object.attr("tzinfo").attr("utcoffset")(object).attr("total_seconds")()) / flex_date_time::TIMEZONE_RESOLUTION_IN_SECONDS;
         out = flex_date_time(
             python::extract<int64_t>(python::import("calendar").attr("timegm")(object.attr("utctimetuple")())),
             offset, 
             python::extract<int64_t>(object.attr("microsecond")));
    } else {
      out = flex_date_time(
          python::extract<int64_t>(python::import("calendar").attr("timegm")(object.attr("utctimetuple")())),
          flex_date_time::EMPTY_TIMEZONE, 
          python::extract<int64_t>(object.attr("microsecond")));
    }
  } else if (PyTuple_Check(objectptr)) { 
    python::tuple tuple_val(object);
      
    flex_list ret_recursive;
    for(size_t i = 0; i < (size_t)len(tuple_val); i++) {
      ret_recursive.push_back(PyObject_AsFlex(tuple_val[i]));
    }
    out = ret_recursive;
  } else if (PyDict_Check(objectptr)) {
    python::dict dict_val(object);
    python::list keys = dict_val.keys();
    flex_dict x;
    x.reserve(len(keys));
    for(size_t i = 0; i < (size_t)len(keys); i++) {
      x.push_back(std::make_pair(
        PyObject_AsFlex(keys[i]),
        PyObject_AsFlex(dict_val[keys[i]])
      ));
    }
    out = x;
  } else if (PyObject_HasAttrString(objectptr, "_image_data")) {
    flex_image img;
    int format_enum;
    img.m_image_data_size = python::extract<size_t>(object.attr("_image_data_size"));
    if (img.m_image_data_size > 0){
      python::object o = object.attr("_image_data");         
      char* buf = new char[img.m_image_data_size];
      memcpy(buf, PyByteArray_AsString(o.ptr()), img.m_image_data_size);
      img.m_image_data.reset(buf);
    }
    img.m_height = python::extract<size_t>(object.attr("_height"));
    img.m_width = python::extract<size_t>(object.attr("_width"));
    img.m_channels = python::extract<size_t>(object.attr("_channels"));
    img.m_version = python::extract<int>(object.attr("_version"));
    format_enum = python::extract<int>(object.attr("_format_enum"));
    img.m_format = static_cast<Format>(format_enum);
    out = img;
  } else if (PyObject_HasAttrString(objectptr, "tolist")) {
    // array.array type
    // this is not the most efficient way, but its should be the most robust.
    out = PyObject_AsFlex(object.attr("tolist")());
  } else if (PyList_Check(objectptr)) {
    // We try hard to convert return list to vector(numeric), only when we encounter the first
    // non-numeric value, would we convert the return value to recursive type.

    size_t list_size = PyList_Size(objectptr);

    bool all_numeric = true;
    flex_list ret_recursive;
    flex_vec ret_vector;

    for (size_t i = 0; i < list_size; ++i) {
      PyObject* a = PyList_GetItem(objectptr, i);
      if (a == NULL) {
        throw bad_cast("Unable to read resultant Python list");
      }

      // first time encounter non-numeric value, move from flex_vec to flex_list
      if (all_numeric && !PyNumber_Check(a)) {
        all_numeric = false;

        // move all items from ret_vector to ret_recursive
        for (size_t j = 0; j < ret_vector.size(); ++j) {
          PyObject* ret_vector_read_obj = PyList_GetItem(objectptr, j); 
          ret_recursive.push_back(PyObject_AsFlex(python::object(python::handle<>(python::borrowed(ret_vector_read_obj)))));
        }
        ret_vector.clear();

      }

      if (all_numeric) {
        ret_vector.push_back((flex_float)PyObject_AsFlex(python::object(python::handle<>(python::borrowed(a)))));
      } else {
        ret_recursive.push_back(PyObject_AsFlex(python::object(python::handle<>(python::borrowed(a)))));
      }
    }

    if (all_numeric) {
      out = ret_vector;
    } else {
      out = ret_recursive;
    }
  } else if (objectptr == python::object().ptr()) {
    out = flexible_type(flex_type_enum::UNDEFINED);
  } else {
    return false;
  }
  return true;
}

} // end lambda
} // end graphlab
