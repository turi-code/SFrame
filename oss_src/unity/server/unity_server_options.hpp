/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SERVER_OPTIONS_HPP
#define GRAPHLAB_UNITY_SERVER_OPTIONS_HPP
#include <string>

namespace graphlab {

class unity_server_options {
 public:
  std::string server_address;
  std::string control_address;
  std::string publish_address;
  std::string auth_token;
  std::string secret_key;
  std::string log_file;
  std::string root_path;
  bool daemon = false;
  size_t log_rotation_interval = 0;
  size_t log_rotation_truncate = 0;

  /**
   * Parse server options from commandline input
   * Return 0 if success, 1 if error, 2 if help.
   */
  int parse_command_line(int argc, char** argv);
};


} // end of namespace graphlab
#endif
