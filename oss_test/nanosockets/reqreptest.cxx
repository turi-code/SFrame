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
#include <string>
#include <iostream>
#include <nanosockets/async_request_socket.hpp>
#include <nanosockets/async_reply_socket.hpp>
using namespace graphlab;
using namespace nanosockets; 

//#define ADDRESS "ipc:///tmp/abcabc"
#define ADDRESS "inproc://abc"

size_t get_value(zmq_msg_vector& msgvec) {
  TS_ASSERT_EQUALS(msgvec.size(), 1);
  iarchive iarc(msgvec.front()->data(), msgvec.front()->length());
  size_t val;
  iarc >> val;
  return val;
}

void set_value(zmq_msg_vector& msgvec, size_t val) {
  msgvec.clear();
  oarchive oarc;
  oarc << val;
  std::string s(oarc.buf, oarc.off);
  free(oarc.buf);
  msgvec.insert_back(s);
}
bool server_handler(zmq_msg_vector& recv,
                    zmq_msg_vector& reply) {
  size_t val = get_value(recv);
  set_value(reply, val + 1);
  return true;
}

volatile bool done = false;

void start_server(){  
  async_reply_socket reply(server_handler, 4, ADDRESS);
  while (done == false) sleep(1);
}

void test_client(async_request_socket& sock, size_t id = 0) {
  for (size_t i = 0;i < 100000; ++i) {
    if (i % 1000 == 0) std::cout << id <<": " << i << std::endl;
    zmq_msg_vector req;
    zmq_msg_vector response;
    set_value(req, i);
    int rc = sock.request_master(req, response);
    TS_ASSERT_EQUALS(rc, 0);
    TS_ASSERT_EQUALS(get_value(response), i + 1);
  }
  std::cout << "Finished " << id << std::endl;
}

class reqrep_test: public CxxTest::TestSuite {  
 public:
  void _test_single_threaded() {
    thread_group grp;
    grp.launch(start_server);
    thread_group grp2;
    async_request_socket req(ADDRESS);
    test_client(req);
    done = true;    
    grp.join();
  }
  void test_multi_thread() {
    thread_group grp;
    grp.launch(start_server);
    thread_group grp2;
    async_request_socket req(ADDRESS);
    grp2.launch([&]() { test_client(req,0); });
    grp2.launch([&]() { test_client(req,1); });
    grp2.launch([&]() { test_client(req,2); });
    grp2.launch([&]() { test_client(req,3); });
    grp2.join();
    done = true;    
    grp.join();
  }
};

