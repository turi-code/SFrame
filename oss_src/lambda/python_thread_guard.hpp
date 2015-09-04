/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_PYTHON_THREAD_GUARD_HPP
#define GRAPHLAB_LAMBDA_PYTHON_THREAD_GUARD_HPP

#include <parallel/mutex.hpp>
#include <Python.h>
#include <boost/python.hpp>

namespace graphlab {

namespace lambda {

/**
 * \ingroup lambda
 *
 * The global mutex provide re-entrant access to the python intepreter.
 */
static graphlab::mutex py_gil;


/**
 * \ingroup lambda
 *
 * An RAII object for guarding multi-thread calls into the python intepreter.
 */
struct python_thread_guard {
  python_thread_guard() : guard(py_gil) {
    thread_state = PyGILState_Ensure();
  }

  ~python_thread_guard() {
    PyGILState_Release(thread_state);
  }

  PyGILState_STATE thread_state;
  std::lock_guard<graphlab::mutex> guard;
};

}
}
#endif
