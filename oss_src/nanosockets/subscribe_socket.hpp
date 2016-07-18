/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef FAULT_SOCKETS_SUBSCRIBE_SOCKET_HPP
#define FAULT_SOCKETS_SUBSCRIBE_SOCKET_HPP
#include <string>
#include <vector>
#include <set>
#include <boost/function.hpp>
#include <parallel/pthread_tools.hpp>
#include <nanosockets/zmq_msg_vector.hpp>
#include <export.hpp>
namespace graphlab { 

namespace nanosockets {
/**
 * \ingroup fault
 * Constructs a zookeeper backed subscribe socket.
 *
 * This object works together with the socket_receive_pollset().
 * The general construction is to
 *  - Create a subscribe_socket
 *  - Create a socket_receive_pollset
 *  - start the pollset ( socket_receive_pollset::start_poll_thread()
 *  - subscribe to a prefix. (Can be the empty string). It is important to at 
 *    least subscribe to the empty string, or nothing will ever be received.
 *
 * \code
 * subscribe_socket subsock(zmq_ctx, NULL, callback);
 * socket_receive_pollset pollset;
 * subsock.add_to_pollset(&pollset);
 * pollset.start_poll_thread();
 * subsock.connect(pub_server);
 * subsock.subscribe("");
 * \endcode
 */
class EXPORT subscribe_socket {
 public:

   typedef boost::function<void(const std::string& message)> callback_type;

  /**
   * Constructs a subscribe socket.
   * \param callback The function used to process replies.
   *
   * keyval can be NULL in which case all "connect/disconnect" calls 
   * must refer to a ZeroMQ endpoints.
   */
  subscribe_socket(callback_type callback);

  /**
   * Closes the socket. Once closed. It cannot be opened again
   */
  void close();

  /**
   * the argument must be a ZeroMQ endpoint to connect to.
   */
  void connect(std::string objectkey);

  /**
   * Disconnects from a given endpoint. 
   */
  void disconnect(std::string objectkey);

  /**
   * Subscribes to a topic. A topic is any message prefix.
   */
  void subscribe(std::string topic);

  /**
   * Unsubscribes from a topic. A topic is any message prefix.
   */
  void unsubscribe(std::string topic);

  bool unsubscribe_all();

  ~subscribe_socket();

 private:
  int z_socket = -1;
  volatile bool shutting_down = false;

  std::map<std::string, size_t> publishers;

  callback_type callback;
  std::set<std::string> topics;
  mutex lock;
  thread thr;


  void thread_function();
};

} // nanosockets
}
#endif
