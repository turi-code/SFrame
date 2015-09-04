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

graphlab::toolkit_function_registry* init_toolkits();

graphlab::toolkit_class_registry* init_models();

/// A helper function to automatically add an entry to the model
///// lookup table with the proper information.
template <typename Model> 
static void register_model_helper(graphlab::toolkit_class_registry* g_toolkit_classes) {
  Model m;
  std::string name = (dynamic_cast<graphlab::model_base*>(&m))->name();
  g_toolkit_classes->register_toolkit_class(name, [](){return new Model;});
}


#endif
