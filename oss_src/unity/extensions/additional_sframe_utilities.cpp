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
#include <unity/lib/toolkit_function_macros.hpp>
using namespace graphlab;

bool is_undefined_or_integer(flexible_type val) {
  return (val.get_type() == flex_type_enum::INTEGER ||
          val.get_type() == flex_type_enum::UNDEFINED);
}


struct slicer_impl {
  int64_t m_start = 0;
  bool has_start = false;
  int64_t m_step = 1;
  bool has_stop = false;
  int64_t m_stop = 0;
  
  template <typename T>
  T slice(const T& s) const {
    T ret;
    int64_t real_start;
    if (has_start) {
      if (m_start < 0) real_start = s.size() + m_start;
      else real_start = m_start;
    } else {
      // default values
      if (m_step > 0) real_start = 0;
      else if (m_step < 0) real_start = s.size() - 1;
    }

    int64_t real_stop;
    if (has_stop) {
      if (m_stop < 0) real_stop = s.size() + m_stop;
      else real_stop = m_stop;
    } else {
      // default values
      if (m_step > 0) real_stop = s.size();
      else if (m_step < 0) real_stop = -1;
    }

    if (m_step > 0 && real_start < real_stop) {
      real_start = std::max<int64_t>(0, real_start);
      real_stop = std::min<int64_t>(s.size(), real_stop);
      for (int64_t i = real_start; i < real_stop; i += m_step) {
        ret.push_back(s[i]);
      }
    } else if (m_step < 0 && real_start > real_stop) {
      real_start = std::min<int64_t>(s.size() - 1, real_start);
      real_stop = std::max<int64_t>(-1, real_stop);
      for (int64_t i = real_start; i > real_stop; i += m_step) {
        ret.push_back(s[i]);
      }
    }
    return ret;
  }
};

gl_sarray sarray_subslice(gl_sarray input, flexible_type start, 
                          flexible_type step, flexible_type stop) {
  // some quick type checking
  if (!(is_undefined_or_integer(start) && 
        is_undefined_or_integer(step) && 
        is_undefined_or_integer(stop))) {
    log_and_throw("Start, stop and end values must be integral.");
  }
  auto dtype = input.dtype();
  if (dtype != flex_type_enum::STRING &&
      dtype != flex_type_enum::VECTOR &&
      dtype != flex_type_enum::LIST) {
    log_and_throw("SArray must contain strings, arrays or lists");
  }

  slicer_impl slicer;
  if (start.get_type() == flex_type_enum::INTEGER) {
    slicer.m_start = start.get<flex_int>();
    slicer.has_start = true;
  }
  if (step.get_type() == flex_type_enum::INTEGER) {
    slicer.m_step = step.get<flex_int>();
    if (slicer.m_step == 0) slicer.m_step = 1;
  }
  if (stop.get_type() == flex_type_enum::INTEGER) {
    slicer.m_stop = stop.get<flex_int>();
    slicer.has_stop = true;
  }

  return input.apply(
      [=](const flexible_type& f) -> flexible_type {
        if (f.get_type() == flex_type_enum::STRING) {
          return slicer.slice(f.get<flex_string>());
        } else if (f.get_type() == flex_type_enum::VECTOR) {
          return slicer.slice(f.get<flex_vec>());
        } else if (f.get_type() == flex_type_enum::LIST) {
          return slicer.slice(f.get<flex_list>());
        } else {
          return flex_undefined();
        }
      }, dtype);
}

BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(sarray_subslice, "input", "start", "step", "stop");
END_FUNCTION_REGISTRATION

