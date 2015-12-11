/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <unity/lib/version.hpp>
#include "unity_server_options.hpp"

namespace graphlab {

namespace po = boost::program_options;
void print_help(std::string program_name, po::options_description& desc) {
  std::cerr << "Unity Server version: " << UNITY_VERSION << "\n"
    << desc << "\n"
    << "Example: " << program_name << " ipc:///tmp/unity_test_server\n"
    << "Example: " << program_name << " tcp://127.0.0.1:10020\n"
    << "Example: " << program_name << " tcp://*:10020\n"
    << "Example: " << program_name << " tcp://127.0.0.1:10020 tcp://127.0.0.1:10021\n"
    << "Example: " << program_name << " ipc:///tmp/unity_test_server --auth_token=auth_token_value\n"
    << "Example: " << program_name << " ipc:///tmp/unity_test_server ipc:///tmp/unity_status auth_token_value\n";
}


int parse_program_options(int argc, char** argv, unity_server_options& option) {
  namespace fs = boost::filesystem;
  std::string program_name = std::string(argv[0]);
  fs::path root_path = fs::path(program_name).parent_path();
  option.root_path = root_path.string();

  po::variables_map vm;
  // define all the command line options 
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "Print this help message.")
      ("server_address", 
       po::value<std::string>(&option.server_address)->implicit_value(option.server_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens on")
      ("control_address", 
       po::value<std::string>(&option.control_address)->implicit_value(option.control_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens for control "
                         "messages on. OPTIONAL")
      ("publish_address", 
       po::value<std::string>(&option.publish_address)->implicit_value(option.publish_address),
       "This must be a valid ZeroMQ endpoint and "
                          "is the address on which the server "
                          "publishes status logs. OPTIONAL")
      ("secret_key",
       po::value<std::string>(&option.secret_key),
       "Secret key used to secure the communication. Client must know the public "
       "key. Default is not to use secure communication.")
      ("auth_token", 
       po::value<std::string>(&option.auth_token)->implicit_value(option.auth_token),
       "This is an arbitrary string which is used to authenticate "
                     "the connection. OPTIONAL")
      ("daemon", po::value<bool>(&option.daemon)->default_value(false),
       "If set to true, will run the process in back-groundable daemon mode.")
      ("log_file",
       po::value<std::string>(&option.log_file),
       "The aggregated log output file. Logs will be printed to stderr as well as "
       "written to the log file ")
      ("log_rotation_interval", 
       po::value<size_t>(&option.log_rotation_interval)->implicit_value(60*60*24)->default_value(0),
       "The log rotation interval in seconds. If set, Log rotation will be performed. "
       "The default log rotation interval is 1 day (60*60*24 seconds). "
       "--log_file must be set for this to be meaningful. The log files will be named "
       "[log_file].0, [log_file].1, etc")
      ("log_rotation_truncate",
       po::value<size_t>(&option.log_rotation_truncate)->implicit_value(8)->default_value(0),
       "The maximum number of logs to keep around. If set log truncation will be performed. "
       "--log_file and --log_rotation_interval must be set for this to be meaningful.");

  po::positional_options_description positional;
  positional.add("server_address", 1);
  positional.add("control_address", 1);
  positional.add("publish_address", 1);
  positional.add("auth_token", 1);

  // try to parse the command line options
  try {
    po::command_line_parser parser(argc, argv);
    parser.options(desc);
    parser.positional(positional);
    po::parsed_options parsed = parser.run();
    po::store(parsed, vm);
    po::notify(vm);
  } catch(std::exception& error) {
    std::cout << "Invalid syntax:\n"
              << "\t" << error.what()
              << "\n\n" << std::endl
              << "Description:"
              << std::endl;
    print_help(program_name, desc);
    return 1;
  }

  if(vm.count("help")) {
    print_help(program_name, desc);
    return 2;
  }
  return 0;
}

} // end of namespace graphlab
