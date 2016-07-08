/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_OPTIONS_MAP_HPP
#define GRAPHLAB_UNITY_OPTIONS_MAP_HPP
namespace graphlab {
/**
 * \ingroup unity
 * An options map is a map from string to a flexible_type.
 * Also see variant_map_type 
 */
typedef std::map<std::string, flexible_type> options_map_t;
}
#endif
