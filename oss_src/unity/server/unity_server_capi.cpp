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

#include "unity_server.hpp"

namespace graphlab {

// Global embeded server and client object
static unity_server* SERVER = nullptr;
static cppipc::comm_client* CLIENT = nullptr;

void start_server(const unity_server_options& server_options) {
  unity_server_initializer server_initializer;
  SERVER = new unity_server(server_options);
  SERVER->start(server_initializer);

  auto zmq_ctx = SERVER->get_comm_server()->get_zmq_context();
  // we are going to leak this pointer 
  CLIENT = new cppipc::comm_client(SERVER->get_comm_server_address(),zmq_ctx);
  CLIENT->start();
}

void* get_client() {
  return (void*)CLIENT;
}

void stop_server() {
  logstream(LOG_EMPH) << "Stopping server" << std::endl;
  if (SERVER) {
    SERVER->stop();
    delete SERVER;
    SERVER = NULL;
  }
}

} // end of graphlab


extern "C" {
/**
 * Starts the server in the same process.
 *
 * \param root_path directory of the graphlab installation
 * \param server_address the inproc address of the server, could be anything like "inproc://test_server"
 * \param log_file local file for logging
 */
EXPORT void start_server(const char* root_path,
                         const char* server_address,
                         const char* log_file,
                         size_t log_rotation_interval,
                         size_t log_rotation_truncate) {

  ASSERT_MSG(boost::starts_with(std::string(server_address), "inproc://"), "Server address must starts with inproc://");

  namespace fs = boost::filesystem;
  global_logger().set_log_level(LOG_INFO);
  global_logger().set_log_to_console(false);

  graphlab::unity_server_options server_options;
  // Example: "inproc://graphlab_server";
  server_options.server_address = server_address;
  // Example: "/tmp/sframe.log";
  server_options.log_file = log_file;
  // Example: "/home/jay/virtualenv/lib/python2.7/site-packages/sframe"
  server_options.root_path = fs::path(root_path).string();
  // Example: "log_rotation_interval = 86400", "log_rotation_truncate = 8"
  server_options.log_rotation_interval = log_rotation_interval;
  server_options.log_rotation_truncate = log_rotation_truncate;

  graphlab::start_server(server_options);
}

/**
 * Return the comm client associated with the embeded server. Require calling
 * start_server first.
 */
EXPORT void* get_client() {
  return graphlab::get_client();
}

/**
 * Shutdown the server, and cleanup all the resourcese
 */
EXPORT void stop_server() {
  graphlab::stop_server();
}

/**
 * Enable or disable log progress stream.
 */
EXPORT void set_log_progress(bool enable) {
  graphlab::unity_server::set_log_progress(enable);
}

EXPORT void set_log_progress_callback(void* callback) {
  graphlab::unity_server::set_log_progress_callback(reinterpret_cast<void(*)(std::string)>(callback));
}
}
 // end of extern "C"
