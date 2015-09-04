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
#ifndef GRAPHLAB_COUNTING_SORT
#define GRAPHLAB_COUNTING_SORT

#ifndef __NO_OPENMP__
#include <omp.h>
#endif

#include <vector>
#include <parallel/atomic.hpp>

namespace graphlab {
    /**
     *  Count the value_vec.
     *  Generate permute_index for value_vec in ascending order and 
     *  optionally fill in the prefix array of the counts. 
     **/
    template <typename valuetype, typename sizetype>
    void counting_sort(const std::vector<valuetype>& value_vec,
                       std::vector<sizetype>& permute_index,
                       std::vector<sizetype>* prefix_array = NULL) {
      if(value_vec.size() == 0) return;

      valuetype maxval = *std::max_element(value_vec.begin(), value_vec.end());
      std::vector< atomic<size_t> > counter_array(maxval+1);
      permute_index.resize(value_vec.size(), 0);
      permute_index.assign(value_vec.size(), 0);
#ifdef _OPENMP
#pragma omp parallel for
#endif
      for (ssize_t i = 0; i < ssize_t(value_vec.size()); ++i) {
        size_t val = value_vec[i];
        counter_array[val].inc();
      }

      for (size_t i = 1; i < counter_array.size(); ++i) {
        counter_array[i] += counter_array[i-1];
      }

#ifdef _OPENMP
#pragma omp parallel for
#endif
      for (ssize_t i = 0; i < ssize_t(value_vec.size()); ++i) {
        size_t val = value_vec[i];
        permute_index[counter_array[val].dec()] = i;
      }

      if (prefix_array != NULL) {
        prefix_array->resize(counter_array.size());
#ifdef _OPENMP
#pragma omp parallel for
#endif
        for (ssize_t i = 0; i < ssize_t(counter_array.size()); ++i) {
          (*prefix_array)[i] = counter_array[i];
        }
      }
    }
} // end of graphlab

#endif
