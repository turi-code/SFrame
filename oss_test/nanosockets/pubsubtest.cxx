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
#include <nanosockets/publish_socket.hpp>
#include <nanosockets/subscribe_socket.hpp>
using namespace graphlab;
using namespace nanosockets; 

size_t get_value(zmq_msg_vector& msgvec) {
  TS_ASSERT_EQUALS(msgvec.size(), 1);
  TS_ASSERT_EQUALS(msgvec.front()->length(), sizeof(size_t));
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

volatile bool done = false;
size_t num_received = 0;
// pubsub is inherently lossy. but in this case we want to make sure we
// receive as as much as possible, so we control the start up
// ordering the pub and the sub
volatile bool sub_is_ready = false;
volatile bool pub_is_ready = false;

void sub_handler(const std::string& recv){
  TS_ASSERT_EQUALS(recv.substr(0,4), "moof");
  iarchive iarc(recv.data() + 4, recv.length() - 4);
  size_t v;
  iarc >> v;
  ++num_received;
}

void start_sub(std::string address){  
  subscribe_socket subsock(sub_handler);
  subsock.connect(address);
  subsock.subscribe("moof");
  sub_is_ready = true;
  while (done == false) sleep(1);
}

void start_pub(std::string address) {
  publish_socket pubsock(address);
  pub_is_ready = true;
  while (!sub_is_ready) sleep(1);
  oarchive oarc;
  for (size_t i = 0;i <= 11000; ++i) {
    oarc.off = 0;
    if (i % 2 == 0) {
      // only evens should be received
      oarc.write("moof", 4);
    } else {
      oarc.write("boof", 4);
    }
    oarc << i;
    if (i % 1000 == 0) std::cout << "sending " << i << std::endl;
    pubsock.send(std::string(oarc.buf, oarc.off));
  }
  done = true;
}

class pubsub_test: public CxxTest::TestSuite {  
 public:
  void test_pubsub() {
    done = false;
    std::string address = "inproc://ccc";
    thread_group grp;
    grp.launch([&]() { start_pub(address); });
    grp.launch([&]() { start_sub(address); });
    grp.join();
    TS_ASSERT_LESS_THAN_EQUALS(3000, num_received); //should be 5500 if all received
  }
};
