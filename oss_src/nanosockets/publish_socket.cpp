/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cassert>
#include <nanosockets/socket_errors.hpp>
#include <nanosockets/socket_config.hpp>
#include <nanosockets/publish_socket.hpp>
#include <nanosockets/print_zmq_error.hpp>
#include <mutex>
#include <serialization/oarchive.hpp>
extern "C" {
#include <nanomsg/nn.h>
#include <nanomsg/pubsub.h>
}
namespace graphlab {
namespace nanosockets {
publish_socket::publish_socket(std::string bind_address) {
  // create a socket
  z_socket = nn_socket(AF_SP, NN_PUB);
  set_conservative_socket_parameters(z_socket);
  local_address = normalize_address(bind_address);
  int rc = nn_bind(z_socket, local_address.c_str());
  if (rc == -1) {
    print_zmq_error("publish_socket construction: ");
    assert(rc == 0);
  }
}

publish_socket::~publish_socket() {
  close();
}

void publish_socket::close() {
  if (z_socket != -1) nn_close(z_socket);
  z_socket = -1;
}


void publish_socket::send(const std::string& msg) {
  std::lock_guard<mutex> lock(z_mutex);
  nn_send(z_socket, msg.c_str(), msg.length(), 0);
}

std::string publish_socket::get_bound_address() {
  return local_address;
}


} // namespace nanosockets
}
