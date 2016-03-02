/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <cstdlib>
#include <time.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <cppipc/cppipc.hpp>
#include <cppipc/common/authentication_token_method.hpp>
#include <minipsutil/minipsutil.h>
#include <logger/logger.hpp>
#include <logger/log_rotate.hpp>
#include <unity/lib/unity_global.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/toolkit_function_registry.hpp>
#include <startup_teardown/startup_teardown.hpp>

#include <lambda/lambda_master.hpp>

#include "unity_server.hpp"

namespace graphlab {

unity_server::unity_server(unity_server_options options) : options(options) {
  toolkit_functions = new toolkit_function_registry();
  toolkit_classes = new toolkit_class_registry();
}

void unity_server::start(const unity_server_initializer& server_initializer) {

  // log files
  if (!options.log_file.empty()) {
    if (options.log_rotation_interval) {
      graphlab::begin_log_rotation(options.log_file,
                                   options.log_rotation_interval,
                                   options.log_rotation_truncate);
    } else {
      global_logger().set_log_file(options.log_file);
    }
  }

  graphlab::configure_global_environment(options.root_path);
  graphlab::global_startup::get_instance().perform_startup();

  // server address
  options.server_address = parse_server_address(options.server_address);

  // construct the server
  server = new cppipc::comm_server(std::vector<std::string>(), "", 
                                   options.server_address,
                                   options.control_address,
                                   options.publish_address,
                                   options.secret_key);

  // initialize built-in data structures, toolkits and models, defined in unity_server_init.cpp
  server_initializer.init_toolkits(*toolkit_functions);
  server_initializer.init_models(*toolkit_classes);
  create_unity_global_singleton(toolkit_functions,
                                toolkit_classes,
                                server);
  auto unity_global_ptr = get_unity_global_singleton();
  server_initializer.register_base_classes(server, unity_global_ptr);

  // initialize extension modules and lambda workers
  server_initializer.init_extensions(options.root_path, unity_global_ptr);
  lambda::set_pylambda_worker_binary_from_environment_variables();

  // start the cppipc server
  server->start();
  logstream(LOG_EMPH) << "Unity server listening on: " <<  options.server_address << std::endl;
  logstream(LOG_EMPH) << "Total System Memory Detected: " << total_mem() << std::endl;
  log_thread.launch([=]() {
                      do {
                        std::pair<std::string, bool> queueelem = this->log_queue.dequeue();
                        if (queueelem.second == false) {
                          break;
                        } else {
                          // we need to read it before trying to do the callback
                          // Otherwise we might accidentally call a null pointer
                          volatile progress_callback_type cback = this->log_progress_callback;
                          if (cback != nullptr) cback(queueelem.first);
                        }
                      } while(1);
                    });
}

/**
 * Cleanup the server state
 */
void unity_server::stop() {
  delete server;
  server = nullptr;
  set_log_progress(false);
  log_queue.stop_blocking();
  graphlab::global_teardown::get_instance().perform_teardown();
}

/**
 * Parse the server_address and return the parsed address.
 *
 * \param server_address can begin with different protocols: ipc, tcp or inproc
 */
std::string unity_server::parse_server_address(std::string server_address) {
  namespace fs = boost::filesystem;
  // Prevent multiple server listen on the same ipc device.
  if (boost::starts_with(server_address, "ipc://") &&
      fs::exists(fs::path(server_address.substr(6)))) {
    logstream(LOG_FATAL) << "Cannot start graphlab server at " 
                         << server_address<< ". File already exists" << "\n";
    exit(-1);
  }
  // Form default server address using process_id and client's timestamp:
  // ipc://graphlab_server-$pid-$timestamp
  if (boost::starts_with(server_address, "default")) {
    std::string path = "/tmp/graphlab_server-" + std::to_string(getpid());
    { // parse server address: "default-$timestamp"
      // append timestamp to the address
      std::vector<std::string> _tmp;
      boost::split(_tmp, server_address, boost::is_any_of("-"));
      if (_tmp.size() == 2)
        path += "-" + _tmp[1];
    }
    server_address = "ipc://" + path;
    if (fs::exists(fs::path(path))) {
      // It could be a leftover of a previous crashed process, try to delete the file
      if (remove(path.c_str()) != 0) {
        logstream(LOG_FATAL) << "Cannot start graphlab server at "
                             << server_address 
                             << ". File already exists, and cannot be deleted." << "\n";
        exit(-1);
      }
    }
  }
  return server_address;
}

EXPORT void unity_server::set_log_progress(bool enable) {
  global_logger().add_observer(LOG_PROGRESS, NULL);
  if (enable == true) {
    // set the progress observer
    global_logger().add_observer(
        LOG_PROGRESS,
        [=](int lineloglevel, const char* buf, size_t len){
          std::cout << "PROGRESS: " << std::string(buf, len);
        });
  }
}

void unity_server::set_log_progress_callback(progress_callback_type callback) {
  if (callback == nullptr) {
    log_progress_callback = nullptr;
    global_logger().add_observer(LOG_PROGRESS, NULL);
  } else {
    log_progress_callback = callback;
    global_logger().add_observer(
        LOG_PROGRESS,
        [=](int lineloglevel, const char* buf, size_t len){
          this->log_queue.enqueue(std::string(buf, len));
        });
  }
}

} // end of graphlab
