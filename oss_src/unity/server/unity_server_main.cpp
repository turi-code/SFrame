/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <logger/logger.hpp>
#include <boost/filesystem.hpp>
#include <startup_teardown/startup_teardown.hpp>
#include "unity_server_options.hpp"

void exported_symbols();

namespace graphlab {
int parse_program_options(int argc, char** argv, unity_server_options& option);
void start_standalone_unity_server(const unity_server_options& options);
}

int main(int argc, char** argv) {
  namespace fs = boost::filesystem;
  std::string program_name = argv[0];

  // parse options, temporarily disable excessive logging
#ifndef NDEBUG
  global_logger().set_log_level(LOG_DEBUG);
#endif
  graphlab::unity_server_options server_options;
  int retcode = graphlab::parse_program_options(argc, argv, server_options);
  if (retcode == 2) { 
    return 0;
  } else if (retcode == 1) {
    return 1;
  }

  global_logger().set_log_level(LOG_INFO);

  // start the server
  std::string root_path = fs::absolute(fs::path(program_name)).parent_path().string();
  graphlab::configure_global_environment(root_path);
  graphlab::global_startup::get_instance().perform_startup();
  graphlab::start_standalone_unity_server(server_options);
  graphlab::global_teardown::get_instance().perform_teardown();

#ifdef _WIN32
  // windows appears to kill threads then call DLLUnload (which calls the
  // global object destructors). This is problematic since all of our
  // state is in DLLs. We can do more cleanup here or we can just die.
  // (this is going to be a problem when we eliminate the unity_server
  // subprocess)
  TerminateProcess(GetCurrentProcess(), 0);
#endif
}
