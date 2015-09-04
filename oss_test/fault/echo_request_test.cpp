/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <iostream>
#include <string>
#include <fault/sockets/socket_errors.hpp>
#include <fault/sockets/request_socket.hpp>
#include <zookeeper_util/key_value.hpp>
#include <zmq_utils.h>
using namespace libfault;
int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Usage: zookeeper_test [zkhost] [prefix] \n";
    return 0;
  }
  std::string zkhost = argv[1];
  std::string prefix = argv[2];
  std::vector<std::string> zkhosts; zkhosts.push_back(zkhost);
  std::string name = "";
  void* zmq_ctx = zmq_ctx_new();
  graphlab::zookeeper_util::key_value key_value(zkhosts, prefix, name);
  request_socket reqsock(zmq_ctx,
                                   &key_value,
                                   "echo",
                                   std::vector<std::string>());
  /*zmq_msg_vector sendmsg, response;
  zmq_msg_t* msg = sendmsg.insert_back();
  zmq_msg_init_size(msg, 8);
  memcpy(zmq_msg_data(msg), "hellowor", 8);
  void* t = zmq_stopwatch_start();
  for (size_t i = 0;i < 100000; ++i) { 
    int ret = reqsock.request_master(sendmsg, response);
    assert(ret == 0);
    response.clear_and_free();
    if (i % 10000 == 0) {
      std::cout << ".";
      std::cout.flush();
    }
  }
  size_t rt = zmq_stopwatch_stop(t);
  std::cout << rt << "\n"; 
  return 0;  */
  while(1) {
    std::string s;
    std::cout << "ECHO: ";
    std::cin >> s;
    // prepare the send message
    zmq_msg_vector sendmsg, response;
    zmq_msg_t* msg = sendmsg.insert_back();
    zmq_msg_init_size(msg, s.length());
    memcpy(zmq_msg_data(msg), s.c_str(), s.length());
    int ret = reqsock.request_master(sendmsg, response);
    if (ret == 0) {
      std::cout << "Response = ";
      while(1) {
        zmq_msg_t* res = response.read_next();
        if (res == NULL) break;
        std::string s((char*)zmq_msg_data(res), zmq_msg_size(res));
        std::cout << s;
      }
      std::cout << "\n";
    } else if (ret == EHOSTUNREACH) {
      std::cout << "Unreachable\n";
    } else if (ret == EPIPE) {
      std::cout << "Fatal\n";
    } else  {
      std::cout << "Unknown Error\n";
    }
  }
}
