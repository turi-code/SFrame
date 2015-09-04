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


#ifndef GRAPHLAB_SERIALIZABLE
#define GRAPHLAB_SERIALIZABLE
#include <boost/concept/assert.hpp>
#include <boost/concept/requires.hpp>
#include <boost/concept_check.hpp>
#include <sstream>
#include <serialization/serialize.hpp>
namespace graphlab {

  /**
   * \brief Concept checks if a type T is serializable.
   *
   * This is a concept checking class for boost::concept and can be 
   * used to enforce that a type T is \ref sec_serializable, assignable and 
   * default constructible. 
   *
   * \tparam T The type to test for serializability.
   */
  template <typename T>
  class Serializable : boost::Assignable<T>, boost::DefaultConstructible<T> {
   public:
    BOOST_CONCEPT_USAGE(Serializable) {
      std::stringstream strm;
      oarchive oarc(strm);
      iarchive iarc(strm);
      const T const_t = T();
      T t = T();
      // A compiler error on these lines implies that your type is not
      // serializable.  See the documentaiton on how to make
      // serializable type.
      oarc << const_t;
      iarc >> t;
    }
  };

} // namespace graphlab
#endif

