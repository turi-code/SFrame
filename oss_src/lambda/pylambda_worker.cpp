/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <boost/program_options.hpp>
#include <cppipc/server/comm_server.hpp>
#include <lambda/pylambda.hpp>
#include <lambda/python_api.hpp>
#include <shmipc/shmipc.hpp>
#include <lambda/graph_pylambda.hpp>
#include <logger/logger.hpp>
#include <process/process_util.hpp>
#include <boost/python.hpp>
#include <util/try_finally.hpp>

namespace po = boost::program_options;

using namespace graphlab;

/** The main function to be called from the python ctypes library to
 *  create a pylambda worker process.
 *
 *  Different error routes produce different error codes of 101 and
 *  above.
 */
int _pylambda_worker_main(const char* _root_path, const char* _server_address) {
  // Options
  std::string server_address = _server_address;
  std::string root_path = _root_path;

  size_t debug_mode = (server_address == "debug");
  char* debug_mode_str = getenv("GRAPHLAB_LAMBDA_WORKER_DEBUG_MODE");

  if(debug_mode_str != NULL) {
    debug_mode = true;
  }

  size_t parent_pid = get_parent_pid();
  size_t this_pid = get_my_pid();

  if(debug_mode) {
    global_logger().set_log_level(LOG_DEBUG);
    global_logger().set_log_to_console(true, true /* stderr */);
  } else {
    global_logger().set_log_level(LOG_INFO);
    char* log_file_prefix = getenv("GRAPHLAB_LAMBDA_WORKER_LOG_PREFIX");
    if (log_file_prefix != NULL) {
      // Write logs to file ands disable console log
      std::string log_file = std::string(log_file_prefix) + "-" + std::to_string(this_pid) + ".log";
      global_logger().set_log_file(log_file);
      global_logger().set_log_to_console(false);
    }
  }
  global_logger().set_pid(get_my_pid());

  LOG_DEBUG_WITH_PID("root_path = '" << root_path << "'");
  LOG_DEBUG_WITH_PID("server_address = '" << server_address << "'");
  LOG_DEBUG_WITH_PID("parend pid = " << parent_pid);

  try {

    LOG_DEBUG_WITH_PID("Library function entered successfully.");

    // Whenever this is set, it must be restored upon return to python. 
    PyThreadState *python_gil_thread_state = nullptr;
    scoped_finally gil_restorer([&](){
        if(python_gil_thread_state != nullptr) {
          LOG_DEBUG_WITH_PID("Restoring GIL thread state.");
          PyEval_RestoreThread(python_gil_thread_state);
          LOG_DEBUG_WITH_PID("GIL thread state restored.");
          python_gil_thread_state = nullptr;
        }
      });
    
    try {

      LOG_DEBUG_WITH_PID("Attempting to initialize python.");
      graphlab::lambda::init_python(root_path);
      LOG_DEBUG_WITH_PID("Python initialized successfully.");
    
    } catch (const std::string& error) {
      logstream(LOG_ERROR) << this_pid << ": "
                           << "Failed to initialize python (internal exception): " << error << std::endl;
      return 101;
    } catch (const std::exception& e) {
      logstream(LOG_ERROR) << this_pid << ": "
                           << "Failed to initialize python: " << e.what() << std::endl;
      return 102;
    }

    if(server_address == "debug") {
      logstream(LOG_INFO) << "Exiting simulation mode ." << std::endl;
      return 1; 
    }

    // Now, release the gil and continue. 
    python_gil_thread_state = PyEval_SaveThread();

    LOG_DEBUG_WITH_PID("Python GIL released.");
    
    graphlab::shmipc::server shm_comm_server;
    bool has_shm = shm_comm_server.bind();

    LOG_DEBUG_WITH_PID("shm_comm_server bind: has_shm=" << has_shm);

    // construct the server
    cppipc::comm_server server(std::vector<std::string>(), "", server_address);

    server.register_type<graphlab::lambda::lambda_evaluator_interface>([&](){
        if (has_shm) {
          auto n = new graphlab::lambda::pylambda_evaluator(&shm_comm_server);
          LOG_DEBUG_WITH_PID("creation of pylambda_evaluator with SHM complete.");
          return n;
        } else {
          auto n = new graphlab::lambda::pylambda_evaluator();
          LOG_DEBUG_WITH_PID("creation of pylambda_evaluator without SHM complete.");
          return n;
        }
      });
    server.register_type<graphlab::lambda::graph_lambda_evaluator_interface>([&](){
        auto n = new graphlab::lambda::graph_pylambda_evaluator();
        LOG_DEBUG_WITH_PID("creation of graph_pylambda_evaluator complete.");
        return n;
      });

    LOG_DEBUG_WITH_PID("Starting server.");
    server.start();

    wait_for_parent_exit(parent_pid);

    return 0;

    /** Any exceptions happening?
     */
  } catch (const std::string& error) {
    logstream(LOG_ERROR) << "Internal PyLambda Error: " << error << std::endl;
    return 103;
  } catch (const std::exception& error) {
    logstream(LOG_ERROR) << "PyLambda C++ Error: " << error.what() << std::endl;
    return 104;
  } catch (...) {
    logstream(LOG_ERROR) << "Unknown PyLambda Error." << std::endl;
    return 105;
  }
}

// This one has to be accessible from python's ctypes.  
extern "C" {
  int EXPORT pylambda_worker_main(const char* _root_path, const char* _server_address) {
    return _pylambda_worker_main(_root_path, _server_address);
  }
}

