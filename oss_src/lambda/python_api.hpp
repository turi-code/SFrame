/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_PYTHON_API_HPP
#define GRAPHLAB_LAMBDA_PYTHON_API_HPP
#include <string>
#include <cstddef>

namespace graphlab {

namespace lambda {



/**
 * \ingroup lambda
 *
 * Initialize the python environment, and import global graphlab modules and classes.
 * Must be called before any python functionality is used.
 *
 * Throws string exception on error.
 */
void init_python(int argc, char** argv); 

/**
 * Extract the exception error string from the python interpreter.
 *
 * This function assumes the gil is acquired. See \ref python_thread_guard.
 *
 * Code adapted from http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
 */
std::string parse_python_error();

/**
 * Set the random seed for the interpreter.
 *
 * This function assumes the gil is acquired. See \ref python_thread_guard.
 */
void py_set_random_seed(size_t seed);

} // end lambda
} // end graphlab
#endif
