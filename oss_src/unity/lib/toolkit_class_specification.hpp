/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_LIB_TOOLKIT_CLASS_SPECIFICATION_HPP
#define GRAPHLAB_UNITY_LIB_TOOLKIT_CLASS_SPECIFICATION_HPP

#include <string>
#include <flexible_type/flexible_type.hpp>
namespace graphlab {
class model_base;
/**
 * \ingroup unity
 * Each model is specified by filling in \ref toolkit_model_specification struct.
 * The contents of the struct describe user-facing documentation and default
 * options, as well as a callback to actual toolkit execution.
 */
struct toolkit_class_specification {
  /**
   * A short name used to identify this toolkit. For instance, 
   * LDA, or PageRank.
   */
  std::string name;

  /**
   * Model properties.
   * The following keys are recognized.
   *  - "functions": A dictionary with key: function name, and value,
   *                     a list of input parameters.
   *  - "get_properties": The list of all readable properties of the model
   *  - "set_properties": The list of all writable properties of the model
   *  - "file": The file which the toolkit was loaded from
   *  - "documentation": A documentation string
   */
  std::map<std::string, flexible_type> description;

  /**
   * A callback function to call to construct a model
   */
  model_base* (*constructor)();
};

} // namespace graphlab
#endif // GRAPHLAB_UNITY_LIB_TOOLKIT_MODEL_SPECIFICATION_HPP
