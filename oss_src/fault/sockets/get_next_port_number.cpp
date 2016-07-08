/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <fault/sockets/get_next_port_number.hpp>
#include <cstdlib>
#include <export.hpp>
namespace libfault {
#define ZSOCKET_DYNFROM     0xc000
#define ZSOCKET_DYNTO       0xffff
static size_t cur_port = ZSOCKET_DYNFROM;

EXPORT size_t get_next_port_number() {
  size_t ret = cur_port;
  cur_port = (cur_port + 1) <= ZSOCKET_DYNTO ? 
                                (cur_port + 1) : ZSOCKET_DYNFROM;
  return ret;
}

} // libfault
