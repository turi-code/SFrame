/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

// this file should not be included directly
// due to circular include issues with variant.hpp
// instead. we redirect to variant.hpp which will then include this again.
#include <unity/lib/variant.hpp>
#ifndef GRAPHLAB_UNITY_LIB_API_FUNCTION_CLOSURE_INFO_HPP
#define GRAPHLAB_UNITY_LIB_API_FUNCTION_CLOSURE_INFO_HPP
#include <string>
#include <vector>
#include <utility>
#include <memory>
#include <serialization/serialization_includes.hpp>
namespace graphlab {
/**
 * Describes a function capture closure. 
 * 
 *  Defines a closure class describing a lambda closure. Contains 2 fields:
 *
 *  \param native_fn_name The toolkit native function name
 *
 *  \param arguments An array of the same length as the toolkit native function.
 *      Each array element is an array of 2 elements [is_capture, value]
 *  \code{.py}
 *      If is_capture == 1: 
 *          value contains the captured value
 *      If is_capture == 0:
 *          value contains a number denoting the lambda argument position.
 *  \endcode
 *
 *  Example:
 *  \code{.py}
 *      lambda x, y: fn(10, x, x, y)
 *  \endcode
 *
 *  Then arguments will be 
 *  
 *      [1, 10], -->  is captured value. has value 10
 *      [0, 0],  -->  is not captured value. is argument 0 of the lambda. 
 *      [0, 0],  -->  is not captured value. is argument 0 of the lambda. 
 *      [0, 1]   -->  is not captured value. is argument 1 of the lambda. 
 *  \endcode
 */
struct function_closure_info {
  std::string native_fn_name;
  std::vector<std::pair<size_t, std::shared_ptr<variant_type>>> arguments;

  enum argument_type {
    CAPTURED_VALUE = 1,
    PARAMETER = 0
  };

  void save(oarchive& oarc) const;
  void load(iarchive& iarc);
};

} // namespace graphlab
#endif // GRAPHLAB_UNITY_LIB_API_FUNCTION_CLOSURE_INFO_HPP
