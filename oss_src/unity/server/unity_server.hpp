/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef SFRAME_UNITY_SERVER_HPP
#define SFRAME_UNITY_SERVER_HPP

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

  inline cppipc::comm_server* get_comm_server() { return server; }

  inline std::string get_comm_server_address() { return options.server_address; }

 protected:
  /**
   * Include the authentication method if the auth token is provided
   */
  void set_auth_token();

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
}; // end of class usenity_server
} // end of namespace graphlab

#endif
