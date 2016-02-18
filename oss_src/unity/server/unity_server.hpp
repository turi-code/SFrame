/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef SFRAME_UNITY_SERVER_HPP
#define SFRAME_UNITY_SERVER_HPP

#include <parallel/pthread_tools.hpp>
#include <util/blocking_queue.hpp>
#include "unity_server_options.hpp"
#include "unity_server_init.hpp"

namespace cppipc {
class comm_server;
}

namespace graphlab {
// Forward declaration of classes
class toolkit_function_registry;
class toolkit_class_registry;

class unity_server {
 public:
  typedef void(*progress_callback_type)(const std::string&);

  /**
   * Constructor
   */
  unity_server(unity_server_options options);

  /**
   * Start the server object
   */
  void start(const unity_server_initializer& server_initializer);

  /**
   * Stop the server and cleanup state
   */
  void stop();

  /**
   * Enable or disable log progress stream.
   */
  void set_log_progress(bool enable);

  void set_log_progress_callback(progress_callback_type callback);

  inline cppipc::comm_server* get_comm_server() { return server; }

  inline std::string get_comm_server_address() { return options.server_address; }

 protected:
  /**
   * Parse the server_address and return the parsed address.
   *
   * \param server_address can begin with different protocols: ipc, tcp or inproc
   */
  std::string parse_server_address(std::string server_address);

 protected:
  unity_server_options options;
  cppipc::comm_server* server;
  toolkit_function_registry* toolkit_functions;
  toolkit_class_registry* toolkit_classes;

 private:
  volatile progress_callback_type log_progress_callback = nullptr;
  graphlab::thread log_thread;
  blocking_queue<std::string> log_queue;

}; // end of class usenity_server
} // end of namespace graphlab

#endif
