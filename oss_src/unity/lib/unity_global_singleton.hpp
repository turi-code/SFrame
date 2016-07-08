/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_GLOBAL_SINGLETON_HPP
#define GRAPHLAB_UNITY_GLOBAL_SINGLETON_HPP
#include <memory>
#include <unity/lib/variant.hpp>
namespace cppipc {
class comm_server;
} // namespace cppipc

namespace graphlab {
class toolkit_function_registry;
class toolkit_class_registry;
class unity_global;


/**
 * Creates the unity_global singleton, passing in the arguments into the 
 * unity_global constructor
 */
void create_unity_global_singleton(toolkit_function_registry* _toolkit_functions,
                                   toolkit_class_registry* _classes,
                                   cppipc::comm_server* server);

/**
 * Gets a pointer to the unity global singleton
 */
std::shared_ptr<unity_global> get_unity_global_singleton();


}
#endif // GRAPHLAB_UNITY_GLOBAL_SINGLETON_HPP

