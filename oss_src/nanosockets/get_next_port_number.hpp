/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef FAULT_SOCKETS_GET_NEXT_PORT_NUMBER_HPP
#define FAULT_SOCKETS_GET_NEXT_PORT_NUMBER_HPP
#include <stdint.h>
#include <cstdlib>
namespace graphlab {
namespace nanosockets {

size_t get_next_port_number();

} // namespace nanosockets
}
#endif

