/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef UNITY_LIB_UNITY_SARRAY_BINARY_OPERATIONS_HPP
#define UNITY_LIB_UNITY_SARRAY_BINARY_OPERATIONS_HPP
#include <string>
#include <functional>
#include <flexible_type/flexible_type.hpp>
namespace graphlab {
namespace unity_sarray_binary_operations {

/**
 * Internal function to check if two flexible type can perform a binary operation
 * against each other. Throws a string message on encountering infeasibility.
 */
void check_operation_feasibility(flex_type_enum left,
                                 flex_type_enum right,
                                 std::string op);


/**
 * Given a binary operation type, and the input type, returns the output type.
 * The operation must be one of the following: "+", "-", "*", "/", "<", ">",
 * "<=", ">=", "==", "!=". The type of the new array is dependent on the 
 * semantics of the operation. check_operation_feasibility is assumed to be true.
 *  - comparison operators always return integers
 *  - +,-,* of integer against integers always return integers
 *  - / of integer against integer always returns floats
 *  - +,-,*,/ of floats against floats always return floats
 *  - +,-,*,/ of integer against floats or floats against integers
 *            always return floats.
 */
flex_type_enum get_output_type(flex_type_enum left, 
                               flex_type_enum right,
                               std::string op);

/**
 * Given a binary operation type, returns a lambda function which computes the 
 * function on a pair of flexible_types.
 * The operation must be one of the following: "+", "-", "*", "/", "<", ">",
 * "<=", ">=", "==", "!=". The type of the new array is dependent on the 
 * semantics of the operation. check_operation_feasibility is assumed to be true.
 *  - comparison operators always return integers
 *  - +,-,* of integer against integers always return integers
 *  - / of integer against integer always returns floats
 *  - +,-,*,/ of floats against floats always return floats
 *  - +,-,*,/ of integer against floats or floats against integers
 *            always return floats.
 */
std::function<flexible_type(const flexible_type&, const flexible_type&)> 
get_binary_operator(flex_type_enum left, flex_type_enum right, std::string op);
} // namespace unity_sarray_binary_operations
} // namespace graphlab
#endif

