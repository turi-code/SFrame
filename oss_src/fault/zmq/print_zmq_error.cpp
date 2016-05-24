/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <fault/zmq/print_zmq_error.hpp>
#include <logger/logger.hpp>
#include <iostream>
namespace libfault {

void print_zmq_error(const char* prefix) {
  logstream(LOG_ERROR) << prefix << ": Unexpected socket error(" << zmq_errno()
            << ") = " << zmq_strerror(zmq_errno()) << std::endl;
}

}
