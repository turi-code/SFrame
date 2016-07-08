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

#ifndef GRAPHLAB_VECTOR_ZIP_HPP
#define GRAPHLAB_VECTOR_ZIP_HPP

#ifndef __NO_OPENMP__
#include <omp.h>
#endif

#include <vector>

namespace graphlab {
  template<typename v1, typename v2>
  std::vector<std::pair<v1, v2> > 
    vector_zip(std::vector<v1>& vec1, std::vector<v2>& vec2) {

      assert(vec1.size() == vec2.size());
      size_t length = vec1.size();

      std::vector<std::pair<v1, v2> >  out;
      out.reserve(length);
      out.resize(length);

#ifdef _OPENMP
#pragma omp parallel for
#endif
    for (ssize_t i = 0; i < ssize_t(length); ++i) {
      out[i] = (std::pair<v1, v2>(vec1[i], vec2[i]));
    }
    std::vector<v1>().swap(vec1);
    std::vector<v2>().swap(vec2);
    return out;
  }
} // end of graphlab
#endif
