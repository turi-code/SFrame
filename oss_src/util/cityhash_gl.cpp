/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <util/cityhash_gl.hpp>
#include <flexible_type/flexible_type.hpp> 
#include <vector>

namespace graphlab {

uint128_t hash128(const flexible_type& v) {
  return v.hash128(); 
}

uint64_t hash64(const flexible_type& v) {
  return v.hash(); 
}

uint128_t hash128(const std::vector<flexible_type>& v) {
  uint128_t h = hash128(v.size());

  for(const flexible_type& x : v)
    h = hash128_combine(h, x.hash128());

  return h;
}

uint64_t hash64(const std::vector<flexible_type>& v) {
  return hash64(hash128(v));
}


}; 
