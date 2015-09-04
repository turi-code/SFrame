/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_IS_SWRITER_BASE_HPP
#define GRAPHLAB_UNITY_IS_SWRITER_BASE_HPP


#include <iterator>
#include <type_traits>
#include <sframe/swriter_base.hpp>
namespace graphlab {
namespace sframe_impl {


/**
 * is_swriter_base<T>::value is true if T inherits from swriter_base
 */
template <typename T, 
          typename DecayedT = typename std::decay<T>::type,
          typename Iterator = typename DecayedT::iterator> 
struct is_swriter_base {
  static constexpr bool value = 
      std::is_base_of<graphlab::swriter_base<Iterator>, DecayedT>::value;
};


template <typename T>
struct is_swriter_base<T,void,void> {
  static constexpr bool value = false;
};



} // sframe_impl
} // graphlab

#endif
