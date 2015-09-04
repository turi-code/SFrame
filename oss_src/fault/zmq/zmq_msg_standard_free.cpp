/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cstdlib>
#include <fault/zmq/zmq_msg_standard_free.hpp>
#include <export.hpp>
namespace libfault {

EXPORT void zmq_msg_standard_free(void* buf, void* hint) {
  free(buf);
}

} // libfault
