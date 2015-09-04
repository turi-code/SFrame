/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_TOOLKIT_UTIL_HPP

#define GRAPHLAB_UNITY_TOOLKIT_UTIL_HPP
#include <vector>
#include <utility>
#include <unity/lib/variant.hpp>

/*
 * This contains a collection of useful utility function for toolkit 
 * developement
 */
namespace graphlab {

template <typename T>
T safe_varmap_get(const variant_map_type& kv, std::string key) {
  if (kv.count(key) == 0) {
    log_and_throw("Required Key " + key + " not found");
  } else {
    return variant_get_value<T>(kv.at(key));
  }
  __builtin_unreachable();
}

/**
 * Extract all flexible_type values from the varmap into a std::map<std::String, flexible_type>.
 * All other value types will be ignored.
 */
inline std::map<std::string, flexible_type> varmap_to_flexmap(const variant_map_type& map) {
  std::map<std::string, flexible_type> ret;
  for (const auto& kv : map) {
    if (kv.second.type() == typeid(flexible_type)) {
      ret[kv.first] = boost::get<flexible_type>(kv.second);
    }
  }
  return ret;
}

/**
 * Cast each flexible type to variant type. 
 */
inline std::map<std::string, variant_type> flexmap_to_varmap(const std::map<std::string, flexible_type>& map) {
  std::map<std::string, variant_type> ret;
  for (const auto& kv : map) {
    ret[kv.first] = (variant_type) kv.second; 
  }
  return ret;
}


} // namespace graphlab
#endif
