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


#ifndef GRAPHLAB_ROBUST_CAST_HPP
#define GRAPHLAB_ROBUST_CAST_HPP

#include <boost/utility.hpp>
#include <boost/type_traits/is_convertible.hpp>
namespace graphlab {
  /** robust_cast performs a static cast from type A to type B
      if a cast can be done. Return B() otherwise */
  
  template <typename Target, typename Source>
  typename boost::disable_if_c<boost::is_convertible<Source, Target>::value, 
                               Target>::type
                               robust_cast(const Source &h) {
    return Target();
  }
  
  template <typename Target, typename Source>
  typename boost::enable_if_c<boost::is_convertible<Source, Target>::value, 
                              Target>::type
                              robust_cast(const Source &h) {
    return (Target)h;
  }
}

#endif


