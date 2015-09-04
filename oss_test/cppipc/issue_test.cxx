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
#include <cppipc/client/issue.hpp>
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


class issue_test: public CxxTest::TestSuite {  
 public:
  void test_basic_argument() {
    // create a dispatch to the add call
    std::stringstream message_stream;
    graphlab::oarchive message(message_stream);
    // intentionally write a wrong type. It should cast correctly
    // by the issuer
    cppipc::issue(message, &test::add, (char)20);
    
    // parse the issued message 
    graphlab::iarchive read_message(message_stream);
    size_t arg1;
    read_message >> arg1;

    TS_ASSERT_EQUALS(arg1, 20);
  }

  void test_interesting_arguments() {
    // create a dispatch to the add call
    std::stringstream message_stream;
    graphlab::oarchive message(message_stream);
    // intentionally write a wrong type. It should cast correctly
    // by the issuer
    cppipc::issue(message, &test::add_more, (char)20, int(20), long(30), (unsigned int)(40));

    
    // parse the issued message 
    graphlab::iarchive read_message(message_stream);
    size_t arg1, arg2, arg3, arg4;
    read_message >> arg1 >> arg2 >> arg3 >> arg4;
    TS_ASSERT_EQUALS(arg1, 20);
    TS_ASSERT_EQUALS(arg2, 20);
    TS_ASSERT_EQUALS(arg3, 30);
    TS_ASSERT_EQUALS(arg4, 40);
  }


  void test_string_argument() {
    // create a dispatch to the add call
    std::stringstream message_stream;
    graphlab::oarchive message(message_stream);
    // intentionally write a wrong type. It should cast correctly
    // by the issuer
    cppipc::issue(message, &test::add_one, "hello");

    
    // parse the issued message 
    graphlab::iarchive read_message(message_stream);
    std::string s;
    read_message >> s;
    TS_ASSERT_EQUALS(s, "hello");
  }
};

