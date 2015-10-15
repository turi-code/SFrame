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

#ifdef HAS_TCMALLOC
#include <google/malloc_extension.h>
#endif

namespace po = boost::program_options;

using namespace graphlab;

#ifdef HAS_TCMALLOC
/**
 *  If TCMalloc is available, we try to release memory back to the
 *  system every 15 seconds or so. TCMalloc is otherwise somewhat...
 *  aggressive about keeping memory around.
 */
static bool stop_memory_release_thread = false;
graphlab::mutex memory_release_lock;
graphlab::conditional memory_release_cond;

void memory_release_loop() {
  memory_release_lock.lock();
  while (!stop_memory_release_thread) {
    memory_release_cond.timedwait(memory_release_lock, 15);
    MallocExtension::instance()->ReleaseFreeMemory();
  }
  memory_release_lock.unlock();
}
#endif

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
    
  try {
   
    size_t parent_pid = get_parent_pid();
  
    if(debug_mode) {
      std::cerr << "INFO: Library function entered successfully." << std::endl;
    }

    // Whenever this is set, it must be restored upon return to python. 
    PyThreadState *python_gil_thread_state = nullptr;
    scoped_finally gil_restorer([&](){
        if(python_gil_thread_state != nullptr) {
          PyEval_RestoreThread(python_gil_thread_state);
          python_gil_thread_state = nullptr;
        }
      });
    
    try {
      if(debug_mode) {
        std::cerr << "INFO: Attempting to initialize python." << std::endl;
      }
    
      graphlab::lambda::init_python(root_path, debug_mode);
    
      if(debug_mode) {
        std::cerr << "INFO: Python initialized successfully." << std::endl;
      }
    
    } catch (const std::string& error) {

      logstream(LOG_ERROR) << "Failed to initialize python (internal exception): " << error << std::endl;
    
      if(debug_mode)
        std::cerr << "ERROR: (internal exception) Failed to initialize python: " << error << std::endl;
    
      return 101;
    } catch (const std::exception& e) {

      if(debug_mode)
        std::cerr << "ERROR: Failed to initialize python: " << e.what() << std::endl;

      logstream(LOG_ERROR) << "Failed to initialize python: " << e.what() << std::endl;
    
      return 102;
    }

    if(debug_mode) {
      std::cerr << "INFO: No valid server address, exiting. \n"
                << "   Example: ipc:///tmp/pylambda_worker\n"
                << "   Example: tcp://127.0.0.1:10020\n"
                << "   Example: tcp://*:10020\n"
                << "   Example: tcp://127.0.0.1:10020 tcp://127.0.0.1:10021\n"
                << "   Example: ipc:///tmp/unity_test_server --auth_token=secretkey\n"
                << "   Example: ipc:///tmp/unity_test_server ipc:///tmp/unity_status secretkey\n"
                << std::endl;
      return 1; 
    }

    // Now, release the gil and continue. 
    python_gil_thread_state = PyEval_SaveThread();
    
    graphlab::shmipc::server shm_comm_server;
    bool has_shm = shm_comm_server.bind();
    // construct the server
    cppipc::comm_server server(std::vector<std::string>(), "", server_address);

    server.register_type<graphlab::lambda::lambda_evaluator_interface>([&](){
        if (has_shm) {
          return new graphlab::lambda::pylambda_evaluator(&shm_comm_server);
        } else {
          return new graphlab::lambda::pylambda_evaluator();
        }
      });
    server.register_type<graphlab::lambda::graph_lambda_evaluator_interface>([](){
        return new graphlab::lambda::graph_pylambda_evaluator();
      });

    server.start();

#ifdef HAS_TCMALLOC
    graphlab::thread memory_release_thread;
    memory_release_thread.launch(memory_release_loop);
#endif

    wait_for_parent_exit(parent_pid);

#ifdef HAS_TCMALLOC
    stop_memory_release_thread = true;
    memory_release_cond.signal();
    memory_release_thread.join();
#endif

    return 0;

    /** Any exceptions happening?
     */
  } catch (const std::string& error) {
    logstream(LOG_ERROR) << "Internal PyLambda Error: " << error << std::endl;
    if(debug_mode)
      std::cerr << "Internal PyLambda Error: " << error << std::endl;
    return 103;
  } catch (const std::exception& error) {
    logstream(LOG_ERROR) << "PyLambda C++ Error: " << error.what() << std::endl;
    if(debug_mode)
      std::cerr << "PyLambda C++ Error: " << error.what() << std::endl;
    return 104;
  } catch (...) {
    logstream(LOG_ERROR) << "Unknown PyLambda Error." << std::endl;
    if(debug_mode)
      std::cerr << "Unknown PyLambda Error." << std::endl;
    return 105;
  }
}

// This one has to be accessible from python's ctypes.  
extern "C" {
  int EXPORT pylambda_worker_main(const char* _root_path, const char* _server_address) {
    return _pylambda_worker_main(_root_path, _server_address);
  }
}

