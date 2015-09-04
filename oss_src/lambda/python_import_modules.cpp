/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/python_import_modules.hpp>
namespace graphlab {

namespace lambda {

namespace python = boost::python;

/**
 * Global modules that have been imported on init_python
 */
// graphlab 
python::object py_graphlab;
// gl_pickle
python::object py_gl_pickle;
// graphlab.data_structures.image
python::object py_gl_image_class;
// graphlab.util.timezone
python::object py_gl_timezone;
// gc, garbage collector
python::object py_gc;
// pickle
python::object py_pickle;
// datetime.datetime
python::object py_datetime_module;
// datetime.datetime
python::object py_datetime;
// calendar.timegm
python::object py_timegm;
// array
python::object py_array;

/**
 * Initialize global modules needed by python_api.hpp,
 * and pyflexible_type.hpp
 */
void import_modules(const std::string& sframe_module_name) {
  if(!sframe_module_name.empty()) { 
    // GraphLab modules
    py_graphlab = boost::python::import(sframe_module_name.c_str());
    auto image_module_name = sframe_module_name + ".data_structures.image";
    py_gl_image_class = python::import(image_module_name.c_str()).attr("Image");
    auto timezone_module_name = sframe_module_name + ".util.timezone";
    py_gl_timezone = python::import(timezone_module_name.c_str());
    py_gl_pickle = py_graphlab.attr("_gl_pickle");
  }
  // Other python modules
  py_gc = python::import("gc");
  py_pickle = python::import("pickle");
  py_array = python::import("array");

  // Datetime modules 
  py_datetime_module = python::import("datetime");
  py_datetime = python::import("datetime").attr("datetime");
  py_timegm = python::import("calendar").attr("timegm");
}

} // end lambda 

} // end graphlab
