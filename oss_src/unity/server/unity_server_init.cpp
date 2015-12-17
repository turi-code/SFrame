/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/simple_model.hpp>
#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/toolkit_function_registry.hpp>
#include <unity/lib/unity_odbc_connection.hpp>
#include <unity/server/unity_server_init.hpp>
#include <unity/toolkits/image/image_fn_export.hpp>

graphlab::toolkit_function_registry* init_toolkits() {
  graphlab::toolkit_function_registry* g_toolkit_functions = new graphlab::toolkit_function_registry();
  g_toolkit_functions->register_toolkit_function(graphlab::image_util::get_toolkit_function_registration());
  return g_toolkit_functions;
}

graphlab::toolkit_class_registry* init_models() {
  graphlab::toolkit_class_registry* g_toolkit_classes = new graphlab::toolkit_class_registry();
  // legacy model
  register_model_helper<graphlab::simple_model>(g_toolkit_classes);
  g_toolkit_classes->register_toolkit_class(graphlab::odbc_connection::get_toolkit_class_registration(), "_odbc_connection");
  return g_toolkit_classes;
}
