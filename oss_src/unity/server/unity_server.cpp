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

#include "unity_server_options.hpp"
#include "unity_server_init.hpp"

namespace graphlab {

// Forward declaration of classes
class toolkit_function_registry;
class toolkit_class_registry;
// Forward declaration of functions
void init_pylambda_worker();

class unity_server {
 public:
  unity_server(unity_server_options options) : options(options) { }
  /**
   * Start the server object with options
   */
  void start() {
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

    // server address
    options.server_address = parse_server_address(options.server_address);

    // construct the server
    server = new cppipc::comm_server(std::vector<std::string>(), "", 
                                     options.server_address,
                                     options.control_address,
                                     options.publish_address,
                                     options.secret_key);
    // set the progress observer
    global_logger().add_observer(
        LOG_PROGRESS,
        [=](int lineloglevel, const char* buf, size_t len){
          server->report_status("progress", std::string(buf, len));
        });

    // initialize built-in data structures, toolkits and models, defined in unity_server_init.cpp
    register_base_classes(server);
    toolkit_functions = init_toolkits();
    toolkit_classes = init_models();

    // create and register unity global singleton
    create_unity_global_singleton(toolkit_functions, toolkit_classes, server);
    server->register_type<graphlab::unity_global_base>(
      []()->std::shared_ptr<graphlab::unity_global_base> {
        auto unity_shared_ptr = graphlab::get_unity_global_singleton();
        return std::dynamic_pointer_cast<graphlab::unity_global_base>(unity_shared_ptr);
      });

    server->start();
    init_extensions(options.root_path);
    init_pylambda_worker();

    logstream(LOG_EMPH) << "Unity server listening on: " <<  options.server_address << std::endl;
    logstream(LOG_EMPH) << "Total System Memory Detected: " << total_mem() << std::endl;
  }

  /**
   * Wait for messages or termination signal.
   */
  void wait_on_stdin() {
    // make a copy of the gtdin file handle since annoyingly some imported
    // libraries via dlopen might close stdin. (i am looking at you...
    // scipy/optimize/minpack2.so as distributed by anaconda)
    auto stdin_clone_fd = dup(STDIN_FILENO);
    auto stdin_clone_file = fdopen(stdin_clone_fd, "r");
    if (options.daemon) {
      while(1) {
        sleep(1000000);
      }
    } else {
      // lldb when breaking and continuing may
      // interrupt the getchar syscall  making this return
      // a failure. (it will return -1 (eof) and set eagain).
      // however, of course eof is a valid value for getchar() to return really.
      // so we have to double check with feof.
      while(1) {
        int c = fgetc(stdin_clone_file);
        // returned eof, but not actually eof. this is an interrupted syscall.
        // continue looping.
        if (c == -1 && !feof(stdin_clone_file)) continue;
        logstream(LOG_EMPH) << "quiting with received character: "
                            << int(c)
                            << " feof = " << feof(stdin_clone_file) << std::endl;
        // quit in all other cases.
          break;
      }
    }
  }

  /**
   * Cleanup the server state
   */
  void cleanup() {
    delete server;
    delete toolkit_functions;
    delete toolkit_classes;
    global_logger().add_observer(LOG_PROGRESS, NULL);
  }

  cppipc::comm_server* get_server() {
    return server;
  }

  std::string get_address() {
    return options.server_address;
  }

 protected:
  /**
   * Include the authentication method if the auth token is provided
   */
  void set_auth_token() {
    if (!options.auth_token.empty()) {
      logstream(LOG_EMPH) << "authentication method: authentication_token applied" << std::endl;
      server->add_auth_method(std::make_shared<cppipc::authentication_token_method>(options.auth_token));
    } else {
      logstream(LOG_EMPH) << "no authentication method." << std::endl;
    }
  }

  /**
   * Parse and check the server_address.
   */
  std::string parse_server_address(std::string server_address) {
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

 protected:
  unity_server_options options;
  cppipc::comm_server* server;
  toolkit_function_registry* toolkit_functions;
  toolkit_class_registry*  toolkit_classes;
};


void start_standalone_unity_server(const unity_server_options& server_options) {
  unity_server server(server_options);
  server.start();
  server.wait_on_stdin();
  server.cleanup();
}

// Global embeded server and client object
static unity_server* EMBEDED_SERVER;
static cppipc::comm_client* EMBEDED_CLIENT;

void start_embeded_server(const unity_server_options& server_options) {
  EMBEDED_SERVER = new unity_server(server_options);
  EMBEDED_SERVER->start();

  auto zmq_ctx = EMBEDED_SERVER->get_server()->get_zmq_context();
  // we are going to leak this pointer 
  EMBEDED_CLIENT = new cppipc::comm_client(EMBEDED_SERVER->get_address(), zmq_ctx);
  EMBEDED_CLIENT->start();
}

void* get_embeded_client() {
  return (void*)EMBEDED_CLIENT;
}

void stop_embeded_server() {
  logstream(LOG_EMPH) << "Stopping server" << std::endl;
  if (EMBEDED_SERVER) {
    EMBEDED_SERVER->cleanup();
    delete EMBEDED_SERVER;
    EMBEDED_SERVER = NULL;
  }
}

} // end of graphlab
