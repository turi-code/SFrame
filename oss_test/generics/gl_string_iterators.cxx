/*
 * Copyright (c) 2013 GraphLab Inc.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://graphlab.com
 *
 */

#include <iostream>
#include <typeinfo>       // operator typeid
#include <cstddef>
#include <functional>
#include <iterator>
#include <type_traits>
#include <iterator>

#include <cxxtest/TestSuite.h>

#include <generics/gl_string.hpp>
#include <util/testing_utils.hpp>

using namespace graphlab;

class test_string_iterators : public CxxTest::TestSuite {
 public:

  void _test_begin(gl_string s) {
    const gl_string& cs = s;
    gl_string::iterator b = s.begin();
    gl_string::const_iterator cb = cs.begin();
    if (!s.empty())
    {
      TS_ASSERT(*b == s[0]);
    }
    TS_ASSERT(b == cb);
  }

  void test_begin() {
    _test_begin(gl_string());
    _test_begin(gl_string("123"));
  }

  void _test_cbegin(const gl_string& s) {
    gl_string::const_iterator cb = s.cbegin();
    if (!s.empty())
    {
      TS_ASSERT(*cb == s[0]);
    }
    TS_ASSERT(cb == s.begin());
  }

  void test_cbegin() {
    _test_cbegin(gl_string());
    _test_cbegin(gl_string("123"));
  }


  void _test_cend(const gl_string& s) {
    gl_string::const_iterator ce = s.cend();
    TS_ASSERT(ce == s.end());
  }

  void test_cend() {
    _test_cend(gl_string());
    _test_cend(gl_string("123"));
  }

  void _test_crbegin(const gl_string& s) {
    gl_string::const_reverse_iterator cb = s.crbegin();
    if (!s.empty())
    {
      TS_ASSERT(*cb == s.back());
    }
    TS_ASSERT(cb == s.rbegin());
  }

  void test_crbegin() {
    _test_crbegin(gl_string());
    _test_crbegin(gl_string("123"));
  }
  
  void _test_crend(const gl_string& s) {
    gl_string::const_reverse_iterator ce = s.crend();
    TS_ASSERT(ce == s.rend());
  }

  void test_crend() {
    _test_crend(gl_string());
    _test_crend(gl_string("123"));
  }
  
  void _test_end(gl_string s) {
    const gl_string& cs = s;
    gl_string::iterator e = s.end();
    gl_string::const_iterator ce = cs.end();
    if (s.empty())
    {
      TS_ASSERT(e == s.begin());
      TS_ASSERT(ce == cs.begin());
    }
    TS_ASSERT_EQUALS(e - s.begin(), s.size());
    TS_ASSERT_EQUALS(ce - cs.begin(), cs.size());
  }

  void test_end() {
    _test_end(gl_string());
    _test_end(gl_string("123"));
  }
  
  void test_iterator_traits() { 
    typedef gl_string C;
    C::iterator ii1{}, ii2{};
    C::iterator ii4 = ii1;
    C::const_iterator cii{};
    TS_ASSERT ( ii1 == ii2 );
    TS_ASSERT ( ii1 == ii4 );
    TS_ASSERT ( ii1 == cii );
    TS_ASSERT ( !(ii1 != ii2 ));
    TS_ASSERT ( !(ii1 != cii ));
  }

  void _test_rbegin(gl_string s) {
    const gl_string& cs = s;
    gl_string::reverse_iterator b = s.rbegin();
    gl_string::const_reverse_iterator cb = cs.rbegin();
    if (!s.empty())
    {
      TS_ASSERT(*b == s.back());
    }
    TS_ASSERT(b == cb);
  }

  void test_rbegin() {
    _test_rbegin(gl_string());
    _test_rbegin(gl_string("123"));
  }
  

  void _test_rend(gl_string s) {
    const gl_string& cs = s;
    gl_string::reverse_iterator e = s.rend();
    gl_string::const_reverse_iterator ce = cs.rend();
    if (s.empty())
    {
      TS_ASSERT(e == s.rbegin());
      TS_ASSERT(ce == cs.rbegin());
    }
    TS_ASSERT_EQUALS(e - s.rbegin(), s.size());
    TS_ASSERT_EQUALS(ce - cs.rbegin(), cs.size());
  }

  void test_rend() {
    _test_rend(gl_string());
    _test_rend(gl_string("123"));
  }

  
};
