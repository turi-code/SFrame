/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_SERVER_DISPATCH_HPP
#define CPPIPC_SERVER_DISPATCH_HPP
#include <serialization/serialization_includes.hpp>

namespace cppipc {
class comm_server;
/**
 * \internal
 * \ingroup cppipc
 * The base class for the function dispatch object.
 * The function dispatch object wraps a member function pointer and the 
 * execute call then calls the member function pointer using the objectptr as 
 * the object, and the remaining arguments are deserialized from the iarchive.
 * The result of the call are serialized in the response archive.
 *
 * The restrictions are the the function call must not take any 
 * arguments by reference.
 */
struct dispatch {
  virtual void execute(void* objectptr, 
                       comm_server* server,
                       graphlab::iarchive& msg, 
                       graphlab::oarchive& response) = 0;
  virtual ~dispatch() { }
};





} // cppipc

#endif
