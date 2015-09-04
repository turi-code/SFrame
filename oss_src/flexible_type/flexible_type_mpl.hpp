/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FLEXIBLE_TYPE_MPL_HPP
#define GRAPHLAB_FLEXIBLE_TYPE_MPL_HPP
#include <boost/mpl/vector.hpp>
#include <flexible_type/flexible_type_base_types.hpp>
namespace graphlab {
typedef boost::mpl::vector<flex_int, flex_float, flex_string, flex_vec, flex_list, flex_dict, flex_date_time> flexible_type_types;
}
#endif
