/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_IS_SITERABLE_HPP
#define GRAPHLAB_UNITY_IS_SITERABLE_HPP

#include <iterator>
#include <type_traits>
#include <sframe/siterable.hpp>
namespace graphlab {
namespace sframe_impl {


/**
 * is_siterable<T>::value is true if T inherits from siterable
 */
template <typename T, 
          typename DecayedT = typename std::decay<T>::type,
          typename Iterator = typename DecayedT::iterator> 
struct is_siterable {
  static constexpr bool value = 
      std::is_base_of<graphlab::siterable<Iterator>, DecayedT>::value;
};


template <typename T>
struct is_siterable<T,void,void> {
  static constexpr bool value = false;
};



} // sframe_impl
} // graphlab

#endif
