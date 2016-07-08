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

/**
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved.  
 *
 * Contributed under the iCLA for:
 *    Joseph Gonzalez (jegonzal@yahoo-inc.com) 
 *
 */





#ifndef GRAPHLAB_CACHE_LINE_PAD
#define GRAPHLAB_CACHE_LINE_PAD

namespace graphlab {
    /**
     * Used to prevent false cache sharing by padding T
     */
    template <typename T> struct cache_line_pad  {
      T value;
      char pad[64 - (sizeof(T) % 64)];      
      cache_line_pad(const T& value = T()) : value(value) { }
      T& operator=(const T& other) { return value = other; }      
      operator T() const { return value; }
    }; // end of cache_line_pad

}; // end of namespace





#endif
