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


#ifndef GRAPHLAB_IS_POD_HPP
#define GRAPHLAB_IS_POD_HPP
#include <type_traits>

namespace graphlab {

  /** \ingroup group_serialization
    \brief Inheriting from this type will force the serializer
    to treat the derived type as a POD type.
    */
  struct IS_POD_TYPE { };

  /**
   * \ingroup group_serialization
   *
   * \brief Tests if T is a POD type
   *
   * gl_is_pod<T>::value is true if T is a POD type (as determined by
   * boost::is_pod) or if T inherits from IS_POD_TYPE. gl_is_pod<T>::value
   * is false otherwise.
   */
  template <typename T>
  struct gl_is_pod{
    // it is a pod and is not an integer since we have special handlings for integers
    static constexpr bool value =  std::is_scalar<T>::value || 
        std::is_base_of<IS_POD_TYPE, T>::value;
    /*
     * BOOST_STATIC_CONSTANT(bool, value = (boost::type_traits::ice_or<
     *                                         boost::is_scalar<T>::value,
     *                                         boost::is_base_of<IS_POD_TYPE, T>::value
     *                                       >::value));
     */

    // standard POD detection is no good because things which contain pointers
    // are POD, but are not serializable
    // (T is POD and  T is not an integer of size >= 2)
    /*BOOST_STATIC_CONSTANT(bool, value =
                          (
                           boost::type_traits::ice_and<
                             boost::is_pod<T>::value,
                             boost::type_traits::ice_not<
                               boost::type_traits::ice_and<
                                 boost::is_integral<T>::value,
                                 sizeof(T) >= 2
                                 >::value
                               >::value
                             >::value
                          ));*/

  };
  
  /// \internal

  template <typename T>
  struct gl_is_pod_or_scaler{
    static constexpr bool value =  std::is_scalar<T>::value || gl_is_pod<T>::value;
  };
}

#endif



