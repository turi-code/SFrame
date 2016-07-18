/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef NANOSOCKETS_SOCKETS_ASYNC_REPLY_SOCKET_HPP
#define NANOSOCKETS_SOCKETS_ASYNC_REPLY_SOCKET_HPP
#include <string>
#include <vector>
#include <set>
#include <queue>
#include <boost/function.hpp>
#include <parallel/pthread_tools.hpp>
#include <nanosockets/zmq_msg_vector.hpp>
#include <export.hpp>
namespace graphlab { 

namespace nanosockets {
/**
 * Constructs a nanomsg asynchronous reply socket.
 */
class EXPORT async_reply_socket {
 public:

  /**
   * Returns true if there are contents to reply.
   * Returns false otherwise.
   * If the reply socket is connected to a request socket,
   * this must always return true.
   * \note There is no provided way to figure out if a reply is necessary.
   * This must be managed on a higher protocol layer.
   */
   typedef boost::function<bool (zmq_msg_vector& recv,
                                 zmq_msg_vector& reply)> callback_type;

  /**
   * Constructs a reply socket.
   * \param callback The function used to process replies. Multiple
   *                 threads may call the callback simultaneously
   * \param nthreads The maximum number of threads to use
   * \param alternate_bind_address If set, this will be address to bind to.
   *                               Otherwise, binds to a free tcp address.
   */
  async_reply_socket(callback_type callback,
                     size_t nthreads = 4,
                     std::string bind_address = "");

  void start_polling();
  void stop_polling();

  /**
   * Closes the socket. Once closed. It cannot be opened again
   */
  void close();

  /**
   * Returns the address the socket is bound to
   */
  std::string get_bound_address();

  ~async_reply_socket();

 private:
  struct job {
    char* data = nullptr;
    size_t datalen = 0;
    void* control = nullptr;
  };
  mutex socketlock;
  int z_socket = -1;
  std::string local_address;
  callback_type callback;

  std::queue<job> jobqueue;
  mutex queuelock;
  conditional queuecond;
  bool queue_terminate = false; // false initially. If true, all threads die.

  void thread_function();
  void poll_function();

  void process_job(job j);
  thread_group threads;
  thread_group poll_thread;
};

} // nanosockets
}
#endif
