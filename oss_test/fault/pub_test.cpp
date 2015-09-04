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
#include <cassert>
#include <fault/sockets/socket_errors.hpp>
#include <fault/sockets/publish_socket.hpp>
#include <fault/sockets/socket_receive_pollset.hpp>
using namespace libfault;

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "Usage: pub_test [listen_addr] \n";
    return 0;
  }
  std::string listen_addr = argv[1];
  void* zmq_ctx = zmq_ctx_new();
  zmq_ctx_set(zmq_ctx, ZMQ_IO_THREADS, 4);
  publish_socket pubsock(zmq_ctx,
                         NULL,
                         listen_addr);
  std::cout << "Publish server running. Empty message to quit\n";
  while(1) {
    std::string msg;
    std::cout << "Message to Publish: ";
    std::getline(std::cin, msg);
    if (msg.size() == 0) break;
    else {
      zmq_msg_vector v;
      zmq_msg_t* zmsg = v.insert_back();
      zmq_msg_init_size(zmsg, msg.size());
      memcpy(zmq_msg_data(zmsg), msg.c_str(), msg.length());
      pubsock.send(v);
    }
  }
  pubsock.close();
}
