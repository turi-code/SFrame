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
#include <cxxtest/TestSuite.h>
#include <cppipc/cppipc.hpp>
#include <cppipc/common/authentication_token_method.hpp>
#include <fileio/temp_files.hpp>
#include <thread>
#include <chrono>
#include "test_object_base.hpp"

class test_long_file_name: public CxxTest::TestSuite {
  public:
    void test_lfn() {
      //boost::thread server(server_thread_func);
      // Start server
      std::string prefix = graphlab::get_temp_name();
      // make prefix more than 130 bytes long. Just append a bunch of 'a's
      if (prefix.length() < 130) prefix += std::string(130 - prefix.length(), 'a');
      std::cout << "Target address: " << "ipc://" << prefix << "\n";
      std::string server_ipc_file = std::string("ipc://"+prefix);
      cppipc::comm_server server({}, "", server_ipc_file);

      server.register_type<test_object_base>([](){ return new test_object_impl;});

      std::cout << "Server gonna start now!" << std::endl;
      server.start();
      // Start client
      cppipc::comm_client client({}, server_ipc_file);

      client.start();
      {
        test_object_proxy test_object(client);
        TS_ASSERT_EQUALS(test_object.ping("hello world"), "hello world");
      }
      client.stop();
    }
};
