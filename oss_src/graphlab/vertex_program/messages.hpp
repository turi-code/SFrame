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

#ifndef GRAPHLAB_MESSAGES_HPP
#define GRAPHLAB_MESSAGES_HPP

#include <serialization/serialization_includes.hpp>

namespace graphlab {

  namespace messages {

    /**
     * The priority of two messages is the sum
     */
    struct sum_priority : public graphlab::IS_POD_TYPE {
      double value;
      sum_priority(const double value = 0) : value(value) { }
      double priority() const { return value; }
      sum_priority& operator+=(const sum_priority& other) {
        value += other.value;
        return *this;
      }
    }; // end of sum_priority message

    /**
     * The priority of two messages is the max
     */
    struct max_priority : public graphlab::IS_POD_TYPE {
      double value;
      max_priority(const double value = 0) : value(value) { }
      double priority() const { return value; }
      max_priority& operator+=(const max_priority& other) {
        value = std::max(value, other.value);
        return *this;
      }
    }; // end of max_priority message


  }; // end of messages namespace


}; // end of graphlab namespace
#endif
