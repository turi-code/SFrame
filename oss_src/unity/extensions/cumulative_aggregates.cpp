/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/toolkit_function_macros.hpp>
#include <unity/extensions/cumulative_aggregates.hpp>

namespace graphlab {
namespace cumulative_aggregates {

template <typename AggregateFunctionType, 
          typename AccumulatorType> 
gl_sarray cumulative_aggregate(const gl_sarray& in,
                     AggregateFunctionType aggregate_fn, 
                     AccumulatorType init,
                     flex_type_enum output_type) {
  
  // Empty case.  
  size_t m_size = in.size();
  if (m_size == 0) {
    return gl_sarray({});
  } 
  
  gl_sarray_writer writer(output_type, 1);
  AccumulatorType y = init; 
  for (const auto& v: in.range_iterator()) {
    writer.write(aggregate_fn(v, y), 0); // z[t+1], y[t+1] = f(x[t], y[y]) 
  }
  return writer.close();
}


gl_sarray _sarray_cumulative_sum(gl_sarray in) {

  // Check types.
  flex_type_enum dt = in.dtype();
  if (dt != flex_type_enum::INTEGER && 
      dt != flex_type_enum::FLOAT &&
      dt != flex_type_enum::VECTOR) {
    log_and_throw("SArray must be of type int, float, or array.");
  }
 
  flexible_type start_val = FLEX_UNDEFINED;
  switch (dt) {

    case flex_type_enum::INTEGER:
    case flex_type_enum::FLOAT:
    {
      auto aggregate_fn = []
        (const flexible_type& v, flexible_type& y) -> flexible_type {
          if (v != FLEX_UNDEFINED) {
            if (y == FLEX_UNDEFINED) {
              y = v;
            } else {
              y += v;
            }
          }
          return y;
        };
      return cumulative_aggregate(in, aggregate_fn, start_val, dt); 
    }
    break;

    case flex_type_enum::VECTOR:
    {
      auto aggregate_fn = []
        (const flexible_type& v, flexible_type& y) -> flexible_type {
          if (v != FLEX_UNDEFINED) {
            if (y == FLEX_UNDEFINED) {
              y = v;
            } else {
              if (v.size() != y.size()) {
                log_and_throw(
   "Cannot perform cumulative_sum on SArray with vectors of different lengths.");
              }
              y += v;
            }
          }
          return y;
        };
      return cumulative_aggregate(in, aggregate_fn, start_val, dt); 
    }
    break;

    // This should never happen. 
    default:
      DASSERT_TRUE(false);
      break;
  }
}


BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(_sarray_cumulative_sum, "in");
END_FUNCTION_REGISTRATION

}
}
