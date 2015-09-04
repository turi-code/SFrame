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
#include <deque>
#include <fault/sockets/socket_errors.hpp>
#include <fault/sockets/async_request_socket.hpp>
#include <zookeeper_util/key_value.hpp>
#include <zmq_utils.h>
#include <boost/thread/thread.hpp>
using namespace libfault;
async_request_socket* reqsock;
std::vector<size_t> failed_count;
void run_a_thread(size_t idx, size_t n) {
  zmq_msg_vector sendmsg;
  zmq_msg_t* msg = sendmsg.insert_back();
  zmq_msg_init_size(msg, 16);
  std::deque<future_reply> futures;
  for (size_t i = 0;i < n; ++i) {
    sprintf((char*)zmq_msg_data(msg), "%ld", i);
    if (i % 2 == 0) {
      futures.push_back(reqsock->request_master(sendmsg));
    } else {
      reqsock->request_master(sendmsg, true);
    }
  }
  std::deque<future_reply>::iterator iter = futures.begin();
  size_t expected = 0;
  while (iter != futures.end()) {
    message_reply* reply = iter->get();
    assert(iter->has_exception() == false);
    if (reply->status != 0) {
      failed_count[idx]++;
    } else {
      assert(reply->msgvec.size() == 1);
      size_t ctr;
      ctr = strtol((char*)zmq_msg_data(reply->msgvec[0]), NULL, 10);
      if (ctr != expected) {
        std::cout << ctr << "!=" << expected << "\n";
      }
      assert(ctr == expected);
    }
    delete reply;
    ++iter; expected += 2;
  }
}

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
  reqsock = new async_request_socket(zmq_ctx,
                                     &key_value,
                                     "echo",
                                     std::vector<std::string>());
  socket_receive_pollset pollset;
  reqsock->add_to_pollset(&pollset);
  pollset.start_poll_thread();
  void* t = zmq_stopwatch_start();
  boost::thread_group grp;
  failed_count.resize(6, 0);
  for (size_t i = 0;i < failed_count.size(); ++i) {
    grp.create_thread(boost::bind(run_a_thread, i, 10000));
  }
  grp.join_all();
  size_t rt = zmq_stopwatch_stop(t);
  std::cout << rt << "\n"; 

  std::cout << "Failure Counter: \n";
  for (size_t i = 0;i < failed_count.size(); ++i) {
    std::cout << failed_count[i] << "\t";
  }
  std::cout << "\n";
  pollset.stop_poll_thread();
  delete reqsock;
  return 0;  
}
