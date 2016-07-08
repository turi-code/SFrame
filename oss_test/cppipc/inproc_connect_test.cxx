/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <iostream>
#include <cppipc/cppipc.hpp>
#include <thread>
#include <chrono>


static volatile bool terminate = false;

class inproc_connect_test: public CxxTest::TestSuite {

 private:
    std::string address = "inproc://#1";

 public:
  void test_connect() {

    cppipc::comm_server server({}, "", address);

    std::function<void(void)> server_launch_fn = [&]() {
      std::cout << "Starting server at " << address << std::endl;
      server.start();
      std::cout << "Server started" << std::endl;;
      while (!terminate) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      server.stop();
    };

    boost::thread server_thread(server_launch_fn);

    std::cout << "Starting client at " << address << std::endl;
    cppipc::comm_client client(address, server.get_zmq_context());
    client.start();
    std::cout << "Client started" << std::endl;

    std::cout << "Issue ping" << std::endl;
    std::string reply = client.ping("ping");
    TS_ASSERT_EQUALS(reply, "ping");
    std::cout << "Received ping" << std::endl;

    client.stop();
    terminate = true;
    server_thread.join();
  }
};
