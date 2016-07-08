/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#ifndef BLOOM_FILTER_HPP
#define BLOOM_FILTER_HPP
#include <util/dense_bitset.hpp>

template <size_t len, size_t probes>
class fixed_bloom_filter {
 private:
  fixed_dense_bitset<len> bits;
 public:
  inline fixed_bloom_filter() { }
  
  inline void clear() {
    bits.clear();
  }
  
  inline void insert(uint64_t i) {
    for (size_t i = 0;i < probes; ++i) {
      bits.set_bit_unsync(i % len);
      i = i * 0x9e3779b97f4a7c13LL;
    }
  }
  
  inline bool may_contain(size_t i) {
    for (size_t i = 0;i < probes; ++i) {
      if (bits.get_bit_unsync(i % len) == false) return false;
      i = i * 0x9e3779b97f4a7c13LL;
    }
    return true;
  }

};

#endif
