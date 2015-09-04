/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**  
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
#ifndef GRAPHLAB_GRAPH_HASH_UTIL
#define GRAPHLAB_GRAPH_HASH_UTIL

#include <boost/function.hpp>
#include <boost/functional/hash.hpp>
#include <graphlab/util/integer_mix.hpp>

namespace graphlab {
  namespace graph_hash {
    /** \brief Returns the hashed value of a vertex. */
    inline static size_t hash_vertex (const vertex_id_type vid) { 
      return integer_mix(vid);
    }

    /** \brief Returns the hashed value of an edge. */
    inline static size_t hash_edge (const std::pair<vertex_id_type, vertex_id_type>& e, const uint32_t seed = 5) {
      // a bunch of random numbers
#if (__SIZEOF_PTRDIFF_T__ == 8)
      static const size_t a[8] = {0x6306AA9DFC13C8E7,
        0xA8CD7FBCA2A9FFD4,
        0x40D341EB597ECDDC,
        0x99CFA1168AF8DA7E,
        0x7C55BCC3AF531D42,
        0x1BC49DB0842A21DD,
        0x2181F03B1DEE299F,
        0xD524D92CBFEC63E9};
#else
      static const size_t a[8] = {0xFC13C8E7,
        0xA2A9FFD4,
        0x597ECDDC,
        0x8AF8DA7E,
        0xAF531D42,
        0x842A21DD,
        0x1DEE299F,
        0xBFEC63E9};
#endif
      vertex_id_type src = e.first;
      vertex_id_type dst = e.second;
      return (integer_mix(src^a[seed%8]))^(integer_mix(dst^a[(seed+1)%8]));
    }
  } // end of graph_hash namespace
} // end of graphlab namespace
#endif
