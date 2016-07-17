/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef NANOSOCKETS_SOCKETS_ASYNC_REQUEST_SOCKET_HPP
#define NANOSOCKETS_SOCKETS_ASYNC_REQUEST_SOCKET_HPP
#include <string>
#include <vector>
#include <parallel/mutex.hpp>
#include <parallel/pthread_tools.hpp>
#include <nanosockets/zmq_msg_vector.hpp>
#include <export.hpp>
namespace graphlab { 

namespace nanosockets {



/**
 * \ingroup fault
 * Constructs a nanomsg asynchronous request socket.
 * Will automatically retry sockets.
 * This object is multi-threaded. Calls can be made from any thread.
 */
class EXPORT async_request_socket {
 public:
   /**
   * Constructs a request socket.
   * The request will be sent to the current owners of the key
   *
   * \param zmq_ctx A zeroMQ Context
   * \param masterkey The master target where requests
   */
  async_request_socket(std::string target_address, size_t num_connections=2);


  /**
   * Closes this socket. Once closed, the socket cannot be used again.
   */
  void close();

  ~async_request_socket();

  /**
   * Sends a request to the server.
   * Returns 0 on success, an error number on failure
   */
  int request_master(zmq_msg_vector& msgs,
                     zmq_msg_vector& ret,
                     size_t timeout = 0);

  // on receiving data, this function will be polled once per second. 
  // If this function returns false, the receive polling will quit.
  // This can be used for instance, to quit a receive if the remote is no
  // longer alive.
  void set_receive_poller(boost::function<bool()>);
 private:

  struct socket_data {

    inline socket_data() { }

    // fake copy constructor
    inline socket_data(const socket_data& other) { }
    // fake operator=
    inline void operator=(const socket_data& other) { }

    // The actual zmq socket
    int z_socket = -1;
  };
  // queue of available sockets
  mutex global_lock;
  conditional cvar;
  std::vector<size_t> available;

  std::string server;
  std::vector<socket_data> sockets;

  boost::function<bool()> receive_poller;
  // create a socket for socket i
  // returns 0 on success, errno on failure.
  int create_socket(size_t i);
};


} 
}
#endif
