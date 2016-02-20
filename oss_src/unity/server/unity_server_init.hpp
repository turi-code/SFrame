/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SERVER_UNITY_SERVER_INIT_HPP
#define GRAPHLAB_UNITY_SERVER_UNITY_SERVER_INIT_HPP

#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/toolkit_function_registry.hpp>
#include <unity/lib/api/model_interface.hpp>

namespace graphlab {

/// A helper function to automatically add an entry to the model
/// lookup table with the proper information
template <typename Model> 
static void register_model_helper(graphlab::toolkit_class_registry& g_toolkit_classes) {
  Model m;
  std::string name = (dynamic_cast<graphlab::model_base*>(&m))->name();
  g_toolkit_classes.register_toolkit_class(name, [](){return new Model;});
}

class EXPORT unity_server_initializer {
 public:
  /**
   * Fill the registry of internal toolkits
   */
  virtual void init_toolkits(toolkit_function_registry&) const;

  /**
   * Fill the registry of internal models
   */
  virtual void init_models(toolkit_class_registry&) const;

  /**
   * Load external extensions into unity global
   */
  virtual void init_extensions(std::string root_path, std::shared_ptr<unity_global> unity_global_ptr) const;

  /**
   * Register basic datastructure classes
   */
  virtual void register_base_classes(cppipc::comm_server* server,
                                     std::shared_ptr<unity_global> unity_global_ptr) const;
};

}
#endif
