/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cppipc/client/comm_client.hpp>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <unity/server/unity_server.hpp>

namespace graphlab {

// Global embeded server and client object
static unity_server* SERVER = nullptr;
static cppipc::comm_client* CLIENT = nullptr;

EXPORT void start_server(const unity_server_options& server_options) {
  ASSERT_MSG(boost::starts_with(server_options.server_address, "inproc://"),
             "Server address must starts with inproc://");

  namespace fs = boost::filesystem;
  global_logger().set_log_level(LOG_INFO);
  global_logger().set_log_to_console(false);

  unity_server_initializer server_initializer;
  SERVER = new unity_server(server_options);
  SERVER->start(server_initializer);

  auto zmq_ctx = SERVER->get_comm_server()->get_zmq_context();
  
  // we are going to leak this pointer 
  CLIENT = new cppipc::comm_client(SERVER->get_comm_server_address(),zmq_ctx);
  CLIENT->start();
}

EXPORT void* get_client() {
  return (void*)CLIENT;
}

EXPORT void stop_server() {
  logstream(LOG_EMPH) << "Stopping server" << std::endl;
  if (SERVER) {
    SERVER->stop();
    delete SERVER;
    SERVER = NULL;
  }
}

} // end of graphlab

