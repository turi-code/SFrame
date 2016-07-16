/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cassert>
#include <parallel/atomic.hpp>
#include <logger/logger.hpp>
#include <nanosockets/print_zmq_error.hpp>
#include <nanosockets/socket_errors.hpp>
#include <nanosockets/socket_config.hpp>
#include <nanosockets/async_request_socket.hpp>

extern "C" {
#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>
}

static graphlab::atomic<size_t> ASYNC_SOCKET_CTR;
namespace graphlab {
namespace nanosockets {



async_request_socket::async_request_socket(std::string target_connection,
                                           size_t num_connections) {
  server = target_connection;
  sockets.resize(num_connections);
  for (size_t i = 0;i < sockets.size(); ++i) {
    available.push_back(i);
  }

}

void async_request_socket::close() {
  // closes all sockets
  for (size_t i = 0;i < sockets.size(); ++i) {
    if (sockets[i].z_socket != -1) {
      nn_close(sockets[i].z_socket);
      sockets[i].z_socket = -1;
    }
  }
}


async_request_socket::~async_request_socket() {
  std::unique_lock<mutex> lock(global_lock);
  sockets.clear();
  available.clear();
  cvar.signal();
  lock.unlock();
  close();
}


int async_request_socket::request_master(zmq_msg_vector& msgs,
                                         zmq_msg_vector& ret) {
  // find a free target to lock
  std::unique_lock<mutex> lock(global_lock);
  while(available.size() == 0 && sockets.size() > 0) {
    cvar.wait(lock);
  }
  if (sockets.size() == 0) {
    // no sockets available!
    return -1;
  }
  size_t wait_socket = available[available.size() - 1];
  available.pop_back();
  lock.unlock();
  create_socket(wait_socket);
  int rc = msgs.send(sockets[wait_socket].z_socket, SEND_TIMEOUT);
  if (rc == 0) {
    rc = ret.recv(sockets[wait_socket].z_socket);
  }

  // restore available socket
  lock.lock();
  available.push_back(wait_socket);
  cvar.signal();
  lock.unlock();

  // return
  if (rc != 0) {
    print_zmq_error("Unexpected error on sending");
    return rc;
  } else {
    return 0;
  }
}

int async_request_socket::create_socket(size_t i) {
  if (sockets[i].z_socket == -1) {
    sockets[i].z_socket = nn_socket(AF_SP, NN_REQ);
    set_conservative_socket_parameters(sockets[i].z_socket);
    int rc = nn_connect(sockets[i].z_socket, server.c_str());
    if (rc == -1) {
      print_zmq_error("Unexpected error on connection");
      return rc;
    }
  } 
  return 0;
}

} // namespace nanosockets
}
