/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

/*  
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




#ifndef HAS_SAVE_HPP
#define HAS_SAVE_HPP
#include <typeinfo>
#include <type_traits>
namespace graphlab {
namespace archive_detail {

  /** SFINAE method to detect if a class T 
   * implements a function void T::save(ArcType&) const
   * 
   * If T implements the method, has_save_method<ArcType,T>::value will be 
   * true. Otherwise it will be false
   */
  template<typename ArcType, typename T>
  struct has_save_method
  {
	  template<typename U, void (U::*)(ArcType&) const> struct SFINAE {};
	  template<typename U> static char Test(SFINAE<U, &U::save>*);
	  template<typename U> static int Test(...);
	  static const bool value = sizeof(Test<T>(0)) == sizeof(char);
  };

  /**
   *  save_or_fail<ArcType, T>(arc, t)
   *  will call this version of the function if
   *  T implements void T::save(ArcType&) const.
   *  
   * save_or_fail<ArcType, T>(arc, t) will therefore save the class successfully
   * if T implements the save function correctly. Otherwise, calling 
   * save_or_fail will print an error message.
   */
  template <typename ArcType, typename ValueType>
  typename std::enable_if<has_save_method<ArcType, ValueType>::value, void>::type 
  save_or_fail(ArcType& o, const ValueType &t) { 
    t.save(o);
  }
 
  /**
   *  save_or_fail<ArcType, T>(arc, t)
   *  will call this version of the function if
   *  
   * save_or_fail<ArcType, T>(arc, t) will therefore save the class successfully
   * if T implements the save function correctly. Otherwise, calling 
   * save_or_fail will print an error message.
   * T does not implement void T::save(ArcType&) const.
   */
  template <typename ArcType, typename ValueType>
  typename std::enable_if<!has_save_method<ArcType, ValueType>::value, void>::type 
  save_or_fail(ArcType& o, const ValueType &t) { 
    ASSERT_MSG(false,"Trying to serializable type %s without valid save method.", typeid(ValueType).name()); 
  }
 
}  // archive_detail
}  // graphlab

#endif

