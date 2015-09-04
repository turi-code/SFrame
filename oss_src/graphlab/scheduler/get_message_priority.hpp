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

#ifndef GRAPHLAB_SCHEDULER_GET_MESSAGE_PRIORITY_HPP
#define GRAPHLAB_SCHEDULER_GET_MESSAGE_PRIORITY_HPP

#include <boost/type_traits.hpp>
#include <typeinfo>

namespace graphlab {
  
namespace scheduler_impl {

  template <typename T>
  struct implements_priority_member {
    template<typename U, double (U::*)() const> struct SFINAE {};
    template <typename U> static char test(SFINAE<U, &U::priority>*);
    template <typename U> static int test(...);
    static const bool value = (sizeof(test<T>(0)) == sizeof(char));
  };

  template <typename MessageType>
  typename boost::enable_if_c<implements_priority_member<MessageType>::value,
                              double>::type
  get_message_priority(const MessageType &m) {
    return m.priority();
  }

  template <typename MessageType>
  typename boost::disable_if_c<implements_priority_member<MessageType>::value,
                                double>::type
  get_message_priority(const MessageType &m) {
    return 1.0;
  }

} //namespace scheduler_impl
} //namespace graphlab


#endif
