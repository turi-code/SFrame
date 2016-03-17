/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <vector>
#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/gl_sframe.hpp>
#include <unity/lib/toolkit_function_macros.hpp>
#include "additional_sframe_utilities.hpp"
using namespace graphlab;
typedef int(*sarray_callback_type)(const graphlab::flexible_type*, void*);
typedef int(*sframe_callback_type)(const graphlab::flexible_type*, size_t, void*);


void sarray_callback(graphlab::gl_sarray input, size_t callback_addr, size_t callback_data_addr,
                     size_t begin, size_t end) {
  sarray_callback_type callback_fun = (sarray_callback_type)(callback_addr);
  auto ra = input.range_iterator(begin, end);
  auto iter = ra.begin();
  while (iter != ra.end()) {
    int retcode = callback_fun(&(*iter), (void*)(callback_data_addr));
    if (retcode != 0) log_and_throw("Error applying callback. Error code " + std::to_string(retcode));
    ++iter;
  }
}

void sframe_callback(graphlab::gl_sframe input, size_t callback_addr, size_t callback_data_addr,
                     size_t begin, size_t end) {
  ASSERT_MSG(input.num_columns() > 0, "SFrame has no column");
  sframe_callback_type callback_fun = (sframe_callback_type)(callback_addr);
  auto ra = input.range_iterator(begin, end);
  std::vector<flexible_type> row_vec;
  auto iter = ra.begin();
  while (iter != ra.end()) {
    row_vec = (*iter);
    int retcode = callback_fun(&(row_vec[0]), row_vec.size(), (void*)(callback_data_addr));
    if (retcode != 0) log_and_throw("Error applying callback. Error code " + std::to_string(retcode));
    ++iter;
  }
}

BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(sarray_callback, "input", "callback_addr", "callback_data", "begin", "end");
REGISTER_FUNCTION(sframe_callback, "input", "callback_addr", "callback_data", "begin", "end");
END_FUNCTION_REGISTRATION
