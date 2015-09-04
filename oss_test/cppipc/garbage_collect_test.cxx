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
#include <cxxtest/TestSuite.h>
#include <cppipc/cppipc.hpp>
#include <cppipc/common/authentication_token_method.hpp>
#include <fileio/temp_files.hpp>
#include <thread>
#include <chrono>
#include "test_object_base.hpp"

void pester_server_with_new_friends(cppipc::comm_client& client, size_t num_times) {
  for(size_t i = 0; i < num_times; ++i) {
    try {
      test_object_proxy test_object(client);
      std::cout << test_object.ping("hello world") << "\n";
    } catch (cppipc::reply_status status) {
      std::cout << "Exception: " << cppipc::reply_status_to_string(status) << "\n";
    } catch (const char* s) {
      std::cout << "Exception: " << s << "\n";
    }
  }
}

class garbage_collect_test : public CxxTest::TestSuite {
  public:
    void test_gc_session() {
      //boost::thread server(server_thread_func);
      // Start server
      std::string prefix = graphlab::get_temp_name();
      std::string server_ipc_file = std::string("ipc://"+prefix);
      cppipc::comm_server server({}, "", server_ipc_file);

      server.register_type<test_object_base>([](){ return new test_object_impl;});

      std::cout << "Server gonna start now!" << std::endl;
      server.start();
      // Start client
      cppipc::comm_client client({}, server_ipc_file);

      client.start();

      // We start with one object tracked
      TS_ASSERT_EQUALS(server.num_registered_objects(), 1); // client

      pester_server_with_new_friends(client, 14);

      std::this_thread::sleep_for(std::chrono::seconds(2)); 
      test_object_proxy thing(client);
      TS_ASSERT_EQUALS(server.num_registered_objects(), 2); // client and thing

      std::shared_ptr<test_object_proxy> thing2 = std::make_shared<test_object_proxy>(client);

      // Test to see if server-created objects are created and deleted correctly
      std::shared_ptr<test_object_proxy> p = 
          std::dynamic_pointer_cast<test_object_proxy>(thing - thing2);
      ASSERT_NE(p.get(), NULL);
      TS_ASSERT_EQUALS(server.num_registered_objects(), 4); // client, thing, thing2, and p

      // Execute a function that returns an existing object
      std::shared_ptr<test_object_proxy> q = std::dynamic_pointer_cast<test_object_proxy>(thing + thing2);
      ASSERT_NE(q.get(), NULL);
      TS_ASSERT_EQUALS(server.num_registered_objects(), 4);

      // Sync objects on delete
      std::this_thread::sleep_for(std::chrono::seconds(2));
      p.reset();
      q.reset();
      TS_ASSERT_EQUALS(server.num_registered_objects(), 3); // client, thing, thing2

      // Failed client and reconnect
      client.stop();

      cppipc::comm_client next_client({}, server_ipc_file); 

      // New client
      next_client.start();
      TS_ASSERT_EQUALS(server.num_registered_objects(), 3); // new client, thing, thing2
      test_object_proxy new_thing(next_client);
      std::shared_ptr<test_object_proxy> new_thing2 = std::make_shared<test_object_proxy>(next_client);

      next_client.stop();
    }
};
