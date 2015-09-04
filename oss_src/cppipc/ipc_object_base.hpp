/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_IPC_OBJECT_BASE_HPP
#define CPPIPC_IPC_OBJECT_BASE_HPP
#include <memory>
#include <export.hpp>
/**
 * All exported base classes must inherit from this class.
 */
namespace cppipc {

class EXPORT ipc_object_base: public std::enable_shared_from_this<ipc_object_base> { };

} // cppipc



#endif
