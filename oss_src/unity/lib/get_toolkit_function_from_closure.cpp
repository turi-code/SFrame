/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/variant.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/unity_global.hpp>

namespace graphlab {
namespace variant_converter_impl {
std::function<variant_type(const std::vector<variant_type>&)> 
    get_toolkit_function_from_closure(const function_closure_info& closure) {
  auto native_execute_function = get_unity_global_singleton()
      ->get_toolkit_function_registry()
      ->get_native_function(closure);

  return native_execute_function;
}
} // variant_converter_impl
} // ngraphlab
