/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <nanosockets/print_zmq_error.hpp>
#include <logger/logger.hpp>
#include <iostream>

extern "C" {
#include <nanomsg/nn.h>
}
namespace graphlab {
namespace nanosockets {

void print_zmq_error(const char* prefix) {
  logstream(LOG_ERROR) << prefix << ": Unexpected socket error(" << nn_errno()
            << ") = " << nn_strerror(nn_errno()) << std::endl;
}

}
}
