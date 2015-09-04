/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_SDK_REGISTRATION_FUNCTION_TYPES_HPP
#define GRAPHLAB_UNITY_SDK_REGISTRATION_FUNCTION_TYPES_HPP
#include <vector>
#include <unity/lib/toolkit_function_specification.hpp>
#include <unity/lib/toolkit_class_specification.hpp>

namespace graphlab {

typedef std::vector<toolkit_function_specification> (*get_toolkit_function_registration_type)();
typedef std::vector<toolkit_class_specification> (*get_toolkit_class_registration_type)();

} // graphlab
#endif // GRAPHLAB_UNITY_SDK_REGISTRATION_FUNCTION_TYPES_HPP
