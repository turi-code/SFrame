/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_TOOLKIT_INVOCATION_HPP
#define GRAPHLAB_UNITY_TOOLKIT_INVOCATION_HPP
#include <string>
#include <functional>
#include <unity/lib/variant.hpp>
#include <unity/lib/toolkit_class_registry.hpp>
namespace graphlab {

/**
 * \ingroup unity
 * The arguments used to invoke the toolkit execution.
 * See \ref toolkit_function_specification for details.
 */ 
struct toolkit_function_invocation {
  toolkit_function_invocation() {
    progress = [=](std::string s) {
      logstream(LOG_INFO) << "PROGRESS: " << s << std::endl;
    };
  }
  /**
   * The parameters passed to the toolkit from the user.
   * The options set will be cleaned: every option in 
   * \ref toolkit_function_specification::default_options will show appear here,
   * and there will not be extraneous options.
   */
  variant_map_type params;

  /**
   * A pointer to a function which prints execution progress.
   */
  std::function<void(std::string)> progress;

  /**
   * A pointer to the class registry.
   */
  toolkit_class_registry* classes;
};

}

#endif
