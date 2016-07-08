/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <vector>
#include <parallel/pthread_tools.hpp>
#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/gl_sframe.hpp>
#include <unity/lib/toolkit_function_macros.hpp>
#include <sframe/sframe_config.hpp>
#include "additional_sframe_utilities.hpp"
using namespace graphlab;
typedef int(*sarray_callback_type)(const graphlab::flexible_type*, void*);
typedef int(*sframe_callback_type)(const graphlab::flexible_type*, size_t, void*);

template<typename InputType>
void _fetch_to_buffer(InputType& input,
                  std::vector<std::vector<std::vector<flexible_type>>>& rows_by_segment_id,
                  size_t nthreads) {
  // materialize in parallel to rows_by_segment_id
  rows_by_segment_id.clear();
  rows_by_segment_id.resize(nthreads);
  auto copy_out_callback = [&](size_t segment_id,
                               const std::shared_ptr<sframe_rows>& data) {
    for (auto& row: (*data)) {
      rows_by_segment_id[segment_id].push_back(row);
    }
    return false;
  };
  input.materialize_to_callback(copy_out_callback, nthreads);
}


void sarray_callback(graphlab::gl_sarray input, size_t callback_addr, size_t callback_data_addr,
                     size_t begin, size_t end) {

  sarray_callback_type callback_fun = (sarray_callback_type)(callback_addr);
  size_t nthreads = thread::cpu_count();
  size_t buffer_size = sframe_config::SFRAME_READ_BATCH_SIZE;
  std::vector<std::vector<std::vector<flexible_type>>> rows_by_segment_id;
  for (size_t i = begin; i < end; i += buffer_size) {
    size_t lbegin = i;
    size_t lend = std::min<size_t>(i + buffer_size, end);
    gl_sarray sliced_input = input[{(int64_t)lbegin, (int64_t)lend}];
    _fetch_to_buffer(sliced_input, rows_by_segment_id, nthreads);
    for (auto& rows: rows_by_segment_id) {
      for (auto& row: rows) {
        int retcode = callback_fun(&(row[0]), (void*)(callback_data_addr));
        if (retcode != 0) log_and_throw("Error applying callback. Error code " + std::to_string(retcode));
      }
    }
  }
}

void sframe_callback(graphlab::gl_sframe input, size_t callback_addr, size_t callback_data_addr,
                     size_t begin, size_t end) {
  ASSERT_MSG(input.num_columns() > 0, "SFrame has no column");

  sframe_callback_type callback_fun = (sframe_callback_type)(callback_addr);
  size_t nthreads = thread::cpu_count();
  size_t buffer_size = sframe_config::SFRAME_READ_BATCH_SIZE;
  std::vector<std::vector<std::vector<flexible_type>>> rows_by_segment_id;
  for (size_t i = begin; i < end; i += buffer_size) {
    size_t lbegin = i;
    size_t lend = std::min<size_t>(i + buffer_size, end);
    gl_sframe sliced_input = input[{(int64_t)lbegin, (int64_t)lend}];
    _fetch_to_buffer(sliced_input, rows_by_segment_id, nthreads);
    for (auto& rows: rows_by_segment_id) {
      for (auto& row: rows) {
        int retcode = callback_fun(&(row[0]), row.size(), (void*)(callback_data_addr));
        if (retcode != 0) log_and_throw("Error applying callback. Error code " + std::to_string(retcode));
      }
    }
  }
}

BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(sarray_callback, "input", "callback_addr", "callback_data", "begin", "end");
REGISTER_FUNCTION(sframe_callback, "input", "callback_addr", "callback_data", "begin", "end");
END_FUNCTION_REGISTRATION
