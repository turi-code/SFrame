/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_TOOLKIT_RESPONSE_TYPE_HPP
#define GRAPHLAB_UNITY_TOOLKIT_RESPONSE_TYPE_HPP
#include <string>
#include <unity/lib/variant.hpp>
#include <serialization/serialization_includes.hpp>

namespace graphlab {

/**
 * \ingroup unity
 * The response from a toolkit
 */
struct toolkit_function_response_type {
  /**
   * Whether the toolkit was executed successfully
   */
  bool success = true;

  /**
   * Any other messages to be printed.
   */
  std::string message;

  /**
   * The returned parameters. (Details will vary from toolkit to toolkit)
   */
  variant_map_type params;

  void save(oarchive& oarc) const {
    log_func_entry();
    oarc << success << message << params;
  }

  void load(iarchive& iarc) {
    log_func_entry();
    iarc >> success >> message >> params;
  }
};



} // graphlab

#endif // GRAPHLAB_UNITY_TOOLKIT_RESPONSE_TYPE_HPP
