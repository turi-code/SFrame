/**
 * Copyright (C) 2016 Turi
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


#ifndef GRAPHLAB_INPLACE_SHUFFLE_HPP
#define GRAPHLAB_INPLACE_SHUFFLE_HPP
#include <algorithm>
#include <vector>
#include <cassert>
#include <iterator>

#ifndef __NO_OPENMP__
#include <omp.h>
#endif



namespace graphlab {
/**
 * Shuffles a random access container inplace such at
 * newcont[i] = cont[targets[i]]
 * targets must be the same size as the container
 * Both the container and the targets vector will be modified.
 */
template <typename Iterator, typename sizetype>
void inplace_shuffle(Iterator begin,
                     Iterator end, 
                     std::vector<sizetype> &targets) {
  size_t len = std::distance(begin, end);
  assert(len == targets.size());
  
  for (size_t i = 0;i < len; ++i) {
    // begin the permutation cycle
    if (i != targets[i]) {
      typename std::iterator_traits<Iterator>::value_type v = *(begin + i);
      size_t j = i;
      while(j != targets[j]) {
        size_t next = targets[j];
        if (next != i) {
          *(begin + j) = *(begin + next);
          targets[j] = j;
          j = next;
        } else {
          // end of cycle
          *(begin + j) = v;
          targets[j] = j;
          break;
        }
      }
    }
  }
}

/**
 * Shuffles a random access container inplace such at
 * newcont[i] = cont[targets[i]]
 * targets must be the same size as the container
 */
template <typename Container, typename sizetype>
void outofplace_shuffle(Container &c,
                        const std::vector<sizetype> &targets) {  
  Container result(targets.size());
#ifdef _OPENMP
#pragma omp parallel for
#endif
  for (ssize_t i = 0;i < ssize_t(targets.size()); ++i) {
    result[i] = c[targets[i]];
  }
  std::swap(c, result);
}

}
#endif
