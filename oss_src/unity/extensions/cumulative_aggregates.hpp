/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef EXTENSIONS_CUMULATIVE_AGGREGATES_HPP
#define EXTENSIONS_CUMULATIVE_AGGREGATES_HPP
#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/toolkit_function_specification.hpp>

namespace graphlab {
namespace cumulative_aggregates {

std::vector<toolkit_function_specification> 
                       get_toolkit_function_registration();

/**
 *
 *  An abstraction to perform cumulative aggregates.
 *    y <- x.cumulative_aggregate(f, w_0)
 *
 *  The abstraction is as follows:
 *    y[i+1], w[i+1] = func(x[i], w[i])
 *
 *  where w[i] is some arbitary state.
 *
 *  You can implement cumulative_sum as follows.
 *
 *   auto cumsum_agg = []
 *     (const flexible_type& v, flexible_type& y) -> flexible_type {
 *      y += v;
 *      return y
 *   };
 *   out_sa = in_sa.cumulative_aggregate(agg_fn, 0);
 *
 * \param[in] Function to perform the aggregate to keep track of state.
 * \return SArray 
 */
 template <typename AggregateFunctionType, 
           typename AccumulatorType>
 gl_sarray cumulative_aggregate(
                const gl_sarray& in,
                AggregateFunctionType aggregate_fn, 
                AccumulatorType init,
                flex_type_enum output_type);

/**
 *
 *  Functional form of the pre-built cumulative aggregates.
 *
 *  \param[in] Input SArray
 *  \return SArray of cumulative aggregate of the input SArray.
 */
gl_sarray _sarray_cumulative_sum(gl_sarray in);

}
}
#endif
