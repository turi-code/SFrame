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


#ifndef GRAPHLAB_SERIALIZE_UNORDERED_MAP_HPP
#define GRAPHLAB_SERIALIZE_UNORDERED_MAP_HPP

#include <boost/unordered_map.hpp>
#include <serialization/iarchive.hpp>
#include <serialization/oarchive.hpp>
#include <serialization/iterator.hpp>

namespace graphlab {

namespace archive_detail {
  /** Serializes a map */
  template <typename OutArcType, typename T, typename U>
  struct serialize_impl<OutArcType, boost::unordered_map<T,U>, false > {
  static void exec(OutArcType& oarc, 
                   const boost::unordered_map<T,U>& vec){
    serialize_iterator(oarc, 
                       vec.begin(), vec.end(), vec.size());
  }
  };

  /** deserializes a map  */
      
  template <typename InArcType, typename T, typename U>
  struct deserialize_impl<InArcType, boost::unordered_map<T,U>, false > {
  static void exec(InArcType& iarc, boost::unordered_map<T,U>& vec){
    vec.clear();
    // get the number of elements to deserialize
    size_t length = 0;
    iarc >> length;    
    // iterate through and send to the output iterator
    for (size_t x = 0; x < length ; ++x){
      std::pair<T, U> v;
      iarc >> v;
      vec[v.first] = v.second;
    }
  }
  };

} // archive_detail  
} // graphlab



#if defined(__cplusplus) && __cplusplus >= 201103L
#include <unordered_map>
namespace graphlab {

namespace archive_detail {
  /** Serializes a map */
  template <typename OutArcType, typename T, typename U>
  struct serialize_impl<OutArcType, std::unordered_map<T,U>, false > {
  static void exec(OutArcType& oarc, 
                   const std::unordered_map<T,U>& vec){
    serialize_iterator(oarc, 
                       vec.begin(), vec.end(), vec.size());
  }
  };

  /** deserializes a map  */
      
  template <typename InArcType, typename T, typename U>
  struct deserialize_impl<InArcType, std::unordered_map<T,U>, false > {
  static void exec(InArcType& iarc, std::unordered_map<T,U>& vec){
    vec.clear();
    // get the number of elements to deserialize
    size_t length = 0;
    iarc >> length;    
    // iterate through and send to the output iterator
    for (size_t x = 0; x < length ; ++x){
      std::pair<T, U> v;
      iarc >> v;
      vec[v.first] = v.second;
    }
  }
  };

} // archive_detail  
} // graphlab

#endif


#endif 

