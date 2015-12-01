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
 *  Functional form of the pre-built cumulative aggregates. These functions 
 *  are exposed to the user as extensions.
 *
 *  \param[in] Input SArray
 *  \return SArray of cumulative aggregate of the input SArray.
 */
gl_sarray _sarray_cumulative_sum(const gl_sarray& in);

}
}
#endif
