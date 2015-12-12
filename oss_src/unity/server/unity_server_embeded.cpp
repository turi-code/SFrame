/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <logger/logger.hpp>
#include <logger/assertions.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <startup_teardown/startup_teardown.hpp>
#include "unity_server_options.hpp"

void exported_symbols();

namespace graphlab {
int parse_program_options(int argc, char** argv, unity_server_options& option);
void start_embeded_server(const unity_server_options& server_options);
void* get_embeded_client();
void stop_embeded_server();
}

extern "C" {

/**
 * Starts the server in the same process.
 *
 * \param root_path directory of the graphlab installation
 * \param server_address the inproc address of the server, could be anything like "inproc://test_server"
 * \param log_file local file for logging
 */
EXPORT void start_embeded_server(const char* root_path,
                                 const char* server_address,
                                 const char* log_file) {
  namespace fs = boost::filesystem;
  global_logger().set_log_level(LOG_INFO);
  graphlab::unity_server_options server_options;

  ASSERT_MSG(boost::starts_with(std::string(server_address), "inproc://"), "Server address must starts with inproc://");
  // Example: "inproc://graphlab_server";
  server_options.server_address = server_address;
  // Example: "/tmp/sframe.log";
  server_options.log_file = log_file;
  // Example: "/home/jay/virtualenv/lib/python2.7/site-packages/sframe"
  server_options.root_path = fs::path(root_path).parent_path().string();

  graphlab::configure_global_environment(server_options.root_path);
  graphlab::global_startup::get_instance().perform_startup();
  graphlab::start_embeded_server(server_options);
}

/**
 * Return the comm client associated with the embeded server. Require calling
 * start_embeded_server first.
 */
EXPORT void* get_embeded_client() {
  return graphlab::get_embeded_client();
}

/**
 * Shutdown the server, and cleanup all the resourcese
 */
EXPORT void stop_embeded_server() {
  graphlab::stop_embeded_server();
  graphlab::global_teardown::get_instance().perform_teardown();
}

}
