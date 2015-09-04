/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/unity_global.hpp>

namespace graphlab {

std::shared_ptr<unity_global> unity_global_ptr;

void create_unity_global_singleton(toolkit_function_registry* _toolkit_functions,
                                   toolkit_class_registry* _classes,
                                   cppipc::comm_server* server) {
    unity_global_ptr = std::make_shared<unity_global>(_toolkit_functions, _classes, server);
}

std::shared_ptr<unity_global> get_unity_global_singleton() {
  if (unity_global_ptr == nullptr) {
    ASSERT_MSG(false, "Unity Global has not been created");
  }
  return unity_global_ptr;
}

} // namespace graphlab
