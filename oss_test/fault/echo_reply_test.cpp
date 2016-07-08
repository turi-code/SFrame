/*
* Copyright (C) 2016 Turi
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
#include <cassert>
#include <fault/sockets/socket_errors.hpp>
#include <fault/sockets/async_reply_socket.hpp>
#include <fault/sockets/socket_receive_pollset.hpp>
#include <zookeeper_util/key_value.hpp>
using namespace libfault;
size_t ctr = 0;
bool callback(zmq_msg_vector& recv,
              zmq_msg_vector& reply) {
  reply.clone_from(recv);
  ++ctr;
  return true;
}


int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Usage: zookeeper_test [zkhost] [prefix] \n";
    return 0;
  }
  std::string zkhost = argv[1];
  std::string prefix = argv[2];
  std::vector<std::string> zkhosts; zkhosts.push_back(zkhost);
  std::string name = "echo";
  void* zmq_ctx = zmq_ctx_new();
  zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, 4);
  graphlab::zookeeper_util::key_value key_value(zkhosts, prefix, name);
  async_reply_socket repsock(zmq_ctx,
                             &key_value,
                             callback);
  if(!repsock.register_key("echo")) {
    std::cout << "Unable to register the echo service. An echo service already exists\n";
  }
  socket_receive_pollset pollset;
  repsock.add_to_pollset(&pollset);
  pollset.start_poll_thread();
  std::cout << "Echo server running. Hit enter to quit\n";
  getchar();
  pollset.stop_poll_thread();
  repsock.close();
}
