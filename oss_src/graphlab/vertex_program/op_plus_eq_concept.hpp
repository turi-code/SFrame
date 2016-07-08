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


#ifndef GRAPHLAB_OP_PLUS_EQ_CONCEPT
#define GRAPHLAB_OP_PLUS_EQ_CONCEPT

#include <boost/concept/assert.hpp>
#include <boost/concept/requires.hpp>
#include <boost/concept_check.hpp>
#include <sstream>
#include <serialization/serialize.hpp>


namespace graphlab {

  /**
   * \brief Concept checks if a type T supports operator+=
   *
   * This is a concept checking class for boost::concept and can be
   * used to enforce that a type T is "additive."  In particular many
   * types in GraphLab (e.g., messages, gather_type, as well as
   * aggregation types) must support operator+=.  To achieve this the
   * class should implement:
   *
   * \code
   * class gather_type {
   *   int member1;
   * public:
   *   gather_type& operator+=(const gather_type& other) {
   *     member1 += other.member1;
   *     return *this;
   *   } // end of operator+=
   * };
   * \endcode
   *
   * \tparam T The type to test for additivity
   */
  template <typename T>
  class OpPlusEq :  boost::Assignable<T>, public boost::DefaultConstructible<T> {
   public:
    BOOST_CONCEPT_USAGE(OpPlusEq) {
      T t1 = T();
      const T t2 = T();
      // A compiler error on these lines implies that your type does
      // not support operator+= when this is required (e.g.,
      // gather_type or aggregator types)
      t1 += t2;
    }
  };
} // namespace graphlab
#endif

