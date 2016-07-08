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


#ifndef LOCK_FREE_POOL_HPP
#define LOCK_FREE_POOL_HPP
#include <stdint.h>
#include <vector>
#include <logger/assertions.hpp>
#include <parallel/atomic.hpp>
#include <graphlab/util/lock_free_internal.hpp>
#include <util/branch_hints.hpp>

namespace graphlab {
  template <typename T, typename index_type = uint32_t>
  class lock_free_pool{
  private:
    std::vector<T> data;
    T* lower_ptrlimit;
    T* upper_ptrlimit;
    // freelist[i] points to the next free list element
    // if freelist[i] == index_type(-1), then it is the last element
    // allocated entries are set to index_type(0), though
    // note that there is no way to disambiguate between allocated
    // and non-allocated entries by simply looking at the freelist
    std::vector<index_type> freelist;
    typedef lock_free_internal::reference_with_counter<index_type> queue_ref_type;
    volatile queue_ref_type freelisthead;

  public:
    lock_free_pool(size_t poolsize = 0) { reset_pool(poolsize); }
  
  
    void reset_pool(size_t poolsize) {
      if (poolsize == 0) {
        data.clear();
        freelist.clear();
        lower_ptrlimit = NULL;
        upper_ptrlimit = NULL;
      } else {
        data.resize(poolsize);
        freelist.resize(poolsize);
        for (index_type i = 0;i < freelist.size(); ++i) {
          freelist[i] = i + 1;
        }
        freelist[freelist.size() - 1] = index_type(-1);
        lower_ptrlimit = &(data[0]);
        upper_ptrlimit = &(data[data.size() - 1]);
      }
      freelisthead.q.val = 0;
      freelisthead.q.counter = 0;
    }
  
    std::vector<T>& unsafe_get_pool_ref() { return data; }
  
    T* alloc() {
      // I need to atomically advance freelisthead to the freelist[head]
      queue_ref_type oldhead;
      queue_ref_type newhead;
      do {
        oldhead.combined = freelisthead.combined;
        if (oldhead.q.val == index_type(-1)) return new T; // ran out of pool elements
        newhead.q.val = freelist[oldhead.q.val];
        newhead.q.counter = oldhead.q.counter + 1;
      } while(!atomic_compare_and_swap(freelisthead.combined, 
                                       oldhead.combined, 
                                       newhead.combined));
      freelist[oldhead.q.val] = index_type(-1);
      return &(data[oldhead.q.val]);
    }
  
    void free(T* p) {
      // is this from the pool?
      // if it is below the pointer limits
      if (__unlikely__(p < lower_ptrlimit || p > upper_ptrlimit)) {
        delete p;
        return;
      }
    
      index_type cur = index_type(p - &(data[0]));

      // prepare for free list insertion
      // I need to atomically set freelisthead == cur
      // and freelist[cur] = freelisthead
      queue_ref_type oldhead;
      queue_ref_type newhead;
      do{
        oldhead.combined = freelisthead.combined;
        freelist[cur] = oldhead.q.val;
        newhead.q.val = cur;
        newhead.q.counter = oldhead.q.counter + 1;
        // now try to atomically move freelisthead
      } while(!atomic_compare_and_swap(freelisthead.combined, 
                                       oldhead.combined, 
                                       newhead.combined));
    }
  }; // end of lock free pool

}; // end of graphlab namespace
#endif
