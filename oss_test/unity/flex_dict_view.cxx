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

#include <cxxtest/TestSuite.h>

#include <unistd.h>

#include <unity/lib/unity_sarray.hpp>
#include <unity/lib/flex_dict_view.hpp>
using namespace graphlab;

class flex_dict_test: public CxxTest::TestSuite {
 public:
  flex_dict_test() {
    global_logger().set_log_level(LOG_FATAL);
  }

  void test_iterator() {
    size_t ROW_COUNT = 3;
    size_t ELEMENT_COUNT = 2;

    std::vector<flexible_type> v(ROW_COUNT);
    for(size_t i = 0; i < ROW_COUNT; i++) {
      std::vector<std::pair<flexible_type, flexible_type>> elem;

      for(size_t j = 0; j < ELEMENT_COUNT; j++) {
        elem.push_back(std::make_pair(std::to_string(i * j), i * j));
      }

      v[i] = flexible_type(elem);
    }

    unity_sarray* sa = new unity_sarray();
    sa->construct_from_vector(v, flex_type_enum::DICT);

    TS_ASSERT_EQUALS(sa->dtype(), flex_type_enum::DICT);
    TS_ASSERT_EQUALS(sa->size(), ROW_COUNT);

    size_t i = 0;  // i is row, j is element index in each row
    sa->begin_iterator();
    while(true) {
      auto vec = sa->iterator_get_next(1);
      if (vec.size() == 0) break;

      flex_dict_view fdv = vec[0];
      size_t j = 0;
      for(auto iter = fdv.begin(); iter != fdv.end(); iter++) {
        std::pair<flexible_type, flexible_type> val = *iter;
        TS_ASSERT_EQUALS(val.first, std::to_string(i * j));
        TS_ASSERT_EQUALS(val.second, i * j);
        j++;
      }

      // test size
      TS_ASSERT_EQUALS(fdv.size(), ELEMENT_COUNT);

      // test keys
      auto keys = fdv.keys();
      TS_ASSERT_EQUALS(keys.size(), ELEMENT_COUNT);
      for(size_t k = 0; k < keys.size(); k++) {
        TS_ASSERT_EQUALS(keys[k], std::to_string(k * i));
      }

      // test values
      auto values = fdv.values();
      TS_ASSERT_EQUALS(values.size(), ELEMENT_COUNT);
      for(size_t k = 0; k < values.size(); k++) {
        TS_ASSERT_EQUALS(values[k], k * i);
      }

      // test has_key
      for(size_t k = 0; k < ELEMENT_COUNT; k++) {
        TS_ASSERT(fdv.has_key(std::to_string(i * k)));
      }

      TS_ASSERT(!fdv.has_key("some random value"));

      i++;
    }
  }

};