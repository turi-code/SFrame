/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_PYTHON_IMPORT_MODULES_HPP
#define GRAPHLAB_LAMBDA_PYTHON_IMPORT_MODULES_HPP
#include <cmath>
#include <boost/python.hpp>

namespace graphlab {

namespace lambda {

namespace python = boost::python;

/**
 * Global modules that have been imported on init_python
 */
// graphlab 
extern python::object py_graphlab;
// gl_pickle
extern python::object py_gl_pickle;
// graphlab.data_structures.image
extern python::object py_gl_image_class;
// graphlab.util.timezone
extern python::object py_gl_timezone;
// gc, garbage collector
extern python::object py_gc;
// pickle
extern python::object py_pickle;
// datetime.datetime
extern python::object py_datetime_module;
// datetime.datetime
extern python::object py_datetime;
// calendar.timegm
extern python::object py_timegm;
// array.array
extern python::object py_array;

/**
 * Initialize global modules needed by python_api.hpp,
 * and pyflexible_type.hpp
 *
 * If sframe_module_name is non-empty, this will attempt to import sframe data
 * structures from that module.
 */
void import_modules(const std::string& sframe_module_name="");

} // end lambda 

} // end graphlab
#endif
