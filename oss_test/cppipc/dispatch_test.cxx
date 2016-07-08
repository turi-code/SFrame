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
#include <string>
#include <iostream>
#include <cppipc/server/dispatch.hpp>
#include <cppipc/server/dispatch_impl.hpp>
struct test {
  size_t a;
  std::string add(size_t c) {
    std::stringstream strm;
    strm << (a + c);
    return strm.str();
  }

  std::string add_more(size_t&& c, const size_t& d, const size_t e, size_t f) {
    std::stringstream strm;
    strm << (a + c + d + e + f);
    return strm.str();
  }


  std::string add_one(std::string s) {
    return s + "1";
  }
};

void call_dispatch(cppipc::dispatch* d,
                   void* testobject,
                   std::stringstream& message,
                   std::stringstream& response) {
  graphlab::iarchive iarc(message);
  graphlab::oarchive oarc(response);
  d->execute(testobject, NULL /* comm_server here. we don't have one */, 
             iarc, oarc);
}


class dispatch_test: public CxxTest::TestSuite {  
 public:
  void test_basic_argument() {
    // create a test object
    test testobject;
    testobject.a = 20;

    // create a dispatch to the add call
    cppipc::dispatch* d = cppipc::create_dispatch(&test::add);

    // build a message
    std::stringstream message_stream, response_stream;
    graphlab::oarchive message(message_stream);
    message << (size_t)10;

    // perform the call
    call_dispatch(d, &testobject, message_stream, response_stream);

    // parse the response
    graphlab::iarchive response(response_stream);
    std::string response_string;
    response >> response_string;

    TS_ASSERT_EQUALS(response_string, "30");
  }

  void test_interesting_arguments() {
    // create a test object
    test testobject;
    testobject.a = 20;

    // create a dispatch to the add call
    cppipc::dispatch* d = cppipc::create_dispatch(&test::add_more);

    // build a message
    std::stringstream message_stream, response_stream;
    graphlab::oarchive message(message_stream);
    message << (size_t)10 << (size_t)20 << (size_t)30 << (size_t)40;


    // perform the call
    call_dispatch(d, &testobject, message_stream, response_stream);


    // parse the response
    graphlab::iarchive response(response_stream);
    std::string response_string;
    response >> response_string;

    TS_ASSERT_EQUALS(response_string, "120");
  }


  void test_string_argument() {
    // create a test object
    test testobject;

    // create a dispatch to the add call
    cppipc::dispatch* d = cppipc::create_dispatch(&test::add_one);

    // build a message
    std::stringstream message_stream, response_stream;
    graphlab::oarchive message(message_stream);
    message << std::string("abc");

    // perform the call
    call_dispatch(d, &testobject, message_stream, response_stream);


    // parse the response
    graphlab::iarchive response(response_stream);
    std::string response_string;
    response >> response_string;

    TS_ASSERT_EQUALS(response_string, "abc1");
  }
};

