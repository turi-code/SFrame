/**
 * Copyright (C) 2015 Dato, Inc.
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


#ifndef GRAPHLAB_PARALLEL_LOCKFREE_PUSHBACK_HPP
#define GRAPHLAB_PARALLEL_LOCKFREE_PUSHBACK_HPP
#include <parallel/atomic.hpp>

namespace graphlab {

namespace lockfree_push_back_impl {
  struct idx_ref {
    idx_ref(): reference_count(0), idx(0) { }
    idx_ref(size_t idx): reference_count(0), idx(idx) { }
    
    volatile int reference_count;
    atomic<size_t> idx;
    enum {
      MAX_REF = 65536
    };
    
    inline void inc_ref() {
      while (1) {
        int curref = reference_count;
        if ((curref & MAX_REF) == 0 &&
            atomic_compare_and_swap(reference_count, curref, curref + 1)) {
          break;
        }
      }      
    }

    inline void wait_till_no_ref() {
      while((reference_count & (MAX_REF - 1)) != 0);
    }
    
    inline void dec_ref() {
      __sync_fetch_and_sub(&reference_count, 1);
    }

    inline void flag_ref() {
      __sync_fetch_and_xor(&reference_count, MAX_REF);
    }
    
    inline size_t inc_idx() {
      return idx.inc_ret_last();
    }

    inline size_t inc_idx(size_t n) {
      return idx.inc_ret_last(n);
    }
  };
} // lockfree_push_back_impl
  
/**
 * Provides a lock free way to insert elements to the end
 * of a container. Container must provide 3 functions.
 *  - T& operator[](size_t idx)
 *  - void resize(size_t len)
 *  - size_t size()
 *
 * resize(n) must guarantee that size() >= n.
 * T& operator[](size_t idx) must succeed for idx < size() and must be 
 * safely executeable in parallel.
 * size() must be safely executeable in parallel with resize().
 */
template <typename Container, typename T = typename Container::value_type>
class lockfree_push_back {
  private:
    Container& container;
    lockfree_push_back_impl::idx_ref cur;
    mutex mut;
    float scalefactor;
  public:
    lockfree_push_back(Container& container, size_t startidx, float scalefactor = 2):
                            container(container),cur(startidx), scalefactor(scalefactor) { }

    size_t size() const {
      return cur.idx.value;
    }

    void set_size(size_t s) {
      cur.idx.value = s;
    }

    template <typename Iterator>
    size_t push_back(Iterator begin, Iterator end) {
      size_t numel = std::distance(begin, end);
      size_t putpos = cur.inc_idx(numel);
      size_t endidx = putpos + numel;
      while(1) {
        cur.inc_ref();
        if (endidx <= container.size()) {
          while(putpos < endidx) {
            container[putpos] = (*begin);
            ++putpos; ++begin;
          }
          cur.dec_ref();
          break;
        }
        else {
          cur.dec_ref();

          if (mut.try_lock()) {
            // ok. we need to resize
            // flag the reference and wait till there are no more references
            cur.flag_ref();
            cur.wait_till_no_ref();
            // we are exclusive here. resize
            if (endidx > container.size()) {
              container.resize(std::max<size_t>(endidx, container.size() * scalefactor));
            }
            while(putpos < endidx) {
              container[putpos] = (*begin);
              ++putpos; ++begin;
            }
            cur.flag_ref();
            mut.unlock();
            break;
          }
        }
      }
      return putpos;
    }
    
    bool query(size_t item, T& value) {
      bool ret = false;
      cur.inc_ref();
      if (item < cur.idx) {
        value = container[item];
        ret = true;
      }
      cur.dec_ref();
      return ret;
    }

    T* query(size_t item) {
      T* ret = NULL;
      cur.inc_ref();
      if (item < cur.idx) {
        ret = &(container[item]);
      }
      cur.dec_ref();
      return ret;
    }

    bool query_unsafe(size_t item, T& value) {
      bool ret = false;
      if (item < cur.idx) {
        value = container[item];
        ret = true;
      }
      return ret;
    }

    T* query_unsafe(size_t item) {
      T* ret = NULL;
      if (item < cur.idx) {
        ret = &(container[item]);
      }
      return ret;
    }


    size_t push_back(const T& t) {
      size_t putpos = cur.inc_idx();
      while(1) {
        cur.inc_ref();
        if (putpos < container.size()) {
          container[putpos] = t;
          cur.dec_ref();
          break;
        }
        else {
          cur.dec_ref();

          if (mut.try_lock()) {
            // ok. we need to resize
            // flag the reference and wait till there are no more references
            cur.flag_ref();
            cur.wait_till_no_ref();
            // we are exclusive here. resize
            if (putpos >= container.size()) {
              container.resize(std::max<size_t>(putpos + 1, container.size() * scalefactor));
            }
            container[putpos] = t;
            cur.flag_ref();
            mut.unlock();
            break;
          }
        }
      }
      return putpos;
    }
};

} // namespace graphlab
#endif
