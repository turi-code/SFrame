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


/**
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved.  
 *
 * Contributed under the iCLA for:
 *    Joseph Gonzalez (jegonzal@yahoo-inc.com) 
 *
 */


#include <boost/unordered_map.hpp>
#include <rpc/delta_dht.hpp>


#include <graphlab/macros_def.hpp>
namespace graphlab {
  namespace delta_dht_impl {

    typedef boost::unordered_map<const void*, icache*> cache_map_type;
  
    void destroy_tls_data(void* ptr) {
      cache_map_type* cache_map_ptr = static_cast<cache_map_type*>(ptr);
      if(cache_map_ptr != NULL) { 
        cache_map_type& cache_map = *cache_map_ptr;
        typedef cache_map_type::value_type pair_type;
        foreach(pair_type& pair, cache_map) {
          if(pair.second != NULL) { 
            delete pair.second; 
            pair.second = NULL;
          }
        }
        delete cache_map_ptr;
      }
    }
    struct tls_key_creator {
      pthread_key_t TLS_KEY;
      tls_key_creator() : TLS_KEY(0) {
        pthread_key_create(&TLS_KEY, destroy_tls_data);
      }
    }; 
    const tls_key_creator key; 
    
    icache*& get_icache_ptr(const void* dht_ptr) {
      cache_map_type* cache_map_ptr = static_cast<cache_map_type*> 
        (pthread_getspecific(key.TLS_KEY));
      if(cache_map_ptr == NULL) {
        cache_map_ptr = new cache_map_type();
        pthread_setspecific(key.TLS_KEY, cache_map_ptr);
      }
      ASSERT_NE(cache_map_ptr, NULL);
      ASSERT_NE(dht_ptr, NULL);
      return (*cache_map_ptr)[dht_ptr];
    }

    

    
  }; // end of delta dht impl
}; // end of graphlab namespace









