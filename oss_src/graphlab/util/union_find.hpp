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


#ifndef GRAPHLAB_UTIL_UNION_FIND_HPP
#define GRAPHLAB_UTIL_UNION_FIND_HPP
#include <vector>
#include <utility>
#include <parallel/atomic.hpp>

namespace graphlab {
// IDType must be an integer type and its maximum 
// value must be larger than the length of the sequence
template <typename IDType, typename RankType>
class union_find {
  private:
    std::vector<std::pair<IDType, RankType> > setid;

    bool is_root(IDType i) {
      return setid[i].first == (IDType)i;
    }
    
  public:
    union_find() { }
    void init(IDType s) {
      setid.resize((size_t)s);
      for (size_t i = 0; i < setid.size() ;++i) {
        setid[i].first = (IDType)(i);
        setid[i].second = 0;
      }
    }
    
    void merge(IDType i, IDType j) {
      IDType iroot = find(i);
      IDType jroot = find(j);
      if (iroot == jroot) return;
      else if (setid[iroot].second < setid[jroot].second) {
        setid[iroot].first = jroot;
      }
      else if (setid[iroot].second > setid[jroot].second) {
        setid[jroot].first = iroot;
      }
      else {
        setid[jroot].first = iroot;
        // make sure we don't overflow
        if (setid[iroot].second + 1 > setid[iroot].second) {
          setid[iroot].second = setid[iroot].second + 1;
        }
      }
    }

    IDType find(IDType i) {
      IDType root = i;
      if (is_root(root)) return root;
      
      // get the id of the root element
      while (!is_root(root)) { root = setid[root].first; }
      
      // update the parents and ranks all the way up
      IDType cur = i;
      while (!is_root(cur)) {
        IDType parent = setid[cur].first;
        setid[cur].first = root;
        cur = parent;
      }
      
      return setid[i].first;
    }
};


class concurrent_union_find {
  private:
    union elem{
      struct {
        uint32_t next;
        uint32_t rank;
      } d;
      uint64_t val;
    };
    
    std::vector<elem> setid;

    bool is_root(uint32_t i) {
      return setid[i].d.next == i;
    }

    bool updateroot(uint32_t x, uint32_t oldrank,
                    uint32_t y, uint32_t newrank) {
      elem old; old.d.next = x; old.d.rank = oldrank;
      elem newval; newval.d.next = y; newval.d.rank = newrank;
      return atomic_compare_and_swap(setid[x].val, old.val, newval.val);
    }
    
  public:
    concurrent_union_find() { }
    void init(uint32_t s) {
      setid.resize((size_t)s);
      for (size_t i = 0; i < setid.size() ;++i) {
        setid[i].d.next = (uint32_t)(i);
        setid[i].d.rank = 0;
      }
    }

    void merge(uint32_t x, uint32_t y) {

      uint32_t xr, yr;
      while(1) {
        x = find(x);
        y = find(y);
        if (x == y) return;
        xr = setid[x].d.rank;
        yr = setid[y].d.rank;

        if (xr > yr || (xr == yr && x > y)) {
          std::swap(x,y); std::swap(xr, yr);
        }

        if (updateroot(x, xr, y, xr)) break;
      }
      if (xr == yr) {
        __sync_add_and_fetch(&(setid[y].d.rank), 1);
      }
    }

    uint32_t find(uint32_t x) {
      if (is_root(x)) return x;
      
      uint32_t y = x;
      // get the id of the root element
      while (!is_root(x)) { x = setid[x].d.next; }

      // update the parents and ranks all the way up
      while (setid[y].d.rank < setid[x].d.rank) {
        uint32_t t = setid[y].d.next;
        atomic_compare_and_swap(setid[y].d.next, t, x);
        y = setid[t].d.next;
      }
      return x;
    }
};
}
#endif
