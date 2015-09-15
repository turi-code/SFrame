/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
// Needs to be before Python.h (on Windows at least) because Python.h redefines hypot
#include <cmath>
#include <Python.h>
#include <boost/python.hpp>
#include <lambda/python_api.hpp>
#include <lambda/python_import_modules.hpp>
#include <lambda/python_thread_guard.hpp>
#include <boost/filesystem.hpp>
#include <iostream>

namespace graphlab {

namespace lambda {

namespace python = boost::python;
namespace fs = boost::filesystem;
/**
 * Enforce the sys path to be the same as the GLC client's sys path.
 */
void set_gl_sys_path() {
  try {
    python::object sys_path = python::import("sys").attr("path");
    python::object os_environ = python::import("os").attr("environ");
    python::object sys_path_str = os_environ.attr("get")("__GL_SYS_PATH__");

    // If the __GL_SYS_PATH__ isn't present, then just keep the
    // regular sys.path.
    
    if(sys_path_str != python::object() ) {  
      // Simply doing "sys_path = sys_path_str.attr("split")(":");"
      // does not work as expected.
      // Have to manually clear the list and append with __GL_SYS_PATH__.
      while(len(sys_path))
        sys_path.attr("pop")();
#ifdef _WIN32
      sys_path.attr("extend")(sys_path_str.attr("split")(";"));
#else
      sys_path.attr("extend")(sys_path_str.attr("split")(":"));
#endif
    }
  } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      std::cerr << error_string << std::endl;
      throw(error_string);
  } catch (...) {
    std::cerr << "Warning: Error setting sys.path from __GL_SYS_PATH__" << std::endl;
  }
}

void init_python(int argc, char** argv) {
  Py_Initialize();

  PySys_SetArgvEx(argc, argv, 0);

  set_gl_sys_path();

  PyEval_InitThreads();
  PyEval_SaveThread(); // release the GIL

  std::string module_name;
  try {
    // I need to find my module name
    fs::path curpath = argv[0];
    // we expect to be located in sframe/pylambda_worker or
    // graphlab/pylambda_worker
    curpath = fs::canonical(curpath);
    module_name = curpath.parent_path().filename().string();

    logstream(LOG_INFO) << "Module Name is " << module_name << std::endl;
    if (module_name != "graphlab" && module_name != "sframe") {
      module_name = "";
      logstream(LOG_ERROR)
          << "Not in graphlab subdirectory nor sframe subdirectory. "
          << "Module import disabled"
          << std::endl;
    }
  } catch (...) {
    logstream(LOG_ERROR) << "Failed to obtain module name." << std::endl;
    module_name = "";
  }

  {
    python_thread_guard py_thread_guard;
    try {
      import_modules(module_name);
    } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      std::cerr << error_string << std::endl;
      throw(error_string);
    } catch (...) {
      throw(std::string("Unknown error when import graphlab"));
    }
  }
}

void py_set_random_seed(size_t seed) {
  python::import("random").attr("seed")(python::object(seed));
}

/**
 * Extract the exception error string from the python interpreter.
 *
 * This function assumes the gil is acquired. See \ref python_thread_guard.
 *
 * Code adapted from http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
 */
std::string parse_python_error() {
  PyObject *exc,*val,*tb;
  PyErr_Fetch(&exc,&val,&tb);
  PyErr_NormalizeException(&exc,&val,&tb);
  python::handle<> hexc(exc),hval(python::allow_null(val)),htb(python::allow_null(tb));
  if(!hval) {
    return python::extract<std::string>(python::str(hexc));
  }
  else {
    python::object traceback(python::import("traceback"));
    python::object format_exception(traceback.attr("format_exception"));
    python::object formatted_list(format_exception(hexc,hval,htb));
    python::object formatted(python::str("").join(formatted_list));
    return python::extract<std::string>(formatted);
  }
}

} // end lambda 
} // end graphlab
