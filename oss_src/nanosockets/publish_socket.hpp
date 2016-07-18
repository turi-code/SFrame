/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef FAULT_SOCKETS_PUBLISH_SOCKET_HPP
#define FAULT_SOCKETS_PUBLISH_SOCKET_HPP
#include <string>
#include <vector>
#include <set>
#include <nanosockets/zmq_msg_vector.hpp>
#include <parallel/mutex.hpp>
#include <export.hpp>

namespace graphlab {
namespace nanosockets {

/**
 * \ingroup fault
 * Constructs a zookeeper backed publish socket.
 * This object is very much single threaded.
 * Sends to this socket will be received by all machines
 * subscribed to the socket.
 * 
 * \code
 * publish_socket pubsock(listen_addr);
 * pubsock.send(...);
 * \endcode
 */
class EXPORT publish_socket {
 public:
  /**
   * Constructs a publish socket.
   * The request will be sent to the current owners of the key
   *
   * \param bind_address this will be address to bind to.
   */
  publish_socket(std::string bind_address);

  /**
   * Closes this socket. Once closed, the socket cannot be used again.
   */
  void close();


  /**
   * Sends a message. All subscribers which match the message (by prefix)
   * will receive a copy.
   */
  void send(const std::string& message);

  ~publish_socket();

  /**
   * Returns the address the socket is bound to
   */
  std::string get_bound_address();

 private:
  int z_socket;
  mutex z_mutex;
  std::string local_address;
};


} // namespace fault
}
#endif
