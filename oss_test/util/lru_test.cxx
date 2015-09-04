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
#include <set>
#include <util/lru.hpp>
#include <cxxtest/TestSuite.h>

using namespace graphlab;


class lru_test: public CxxTest::TestSuite {

 public:

  void test_lru() {
    // basic cache test. whether LRU is performed on just insertions
    lru_cache<std::string, size_t> cache;
    cache.set_size_limit(3);
    cache.insert("a", 1);
    cache.insert("b", 1);
    cache.insert("c", 1);
    cache.insert("d", 1);
    TS_ASSERT_EQUALS(cache.query("a").first, false);
    TS_ASSERT_EQUALS(cache.query("b").first, true);
    TS_ASSERT_EQUALS(cache.query("c").first, true);
    TS_ASSERT_EQUALS(cache.query("d").first, true);
    cache.insert("e", 1);
    cache.insert("f", 1);
    TS_ASSERT_EQUALS(cache.query("b").first, false);
    TS_ASSERT_EQUALS(cache.query("c").first, false);
    TS_ASSERT_EQUALS(cache.query("d").first, true);
    TS_ASSERT_EQUALS(cache.query("e").first, true);
    TS_ASSERT_EQUALS(cache.query("f").first, true);
    TS_ASSERT_EQUALS(cache.size(), 3);

    std::set<std::string> s;
    for (const auto& i : cache) {
      s.insert(i.first);
    }
    TS_ASSERT_EQUALS(s.size(), 3);
    auto iter = s.begin();
    TS_ASSERT_EQUALS((*iter), "d"); ++iter;
    TS_ASSERT_EQUALS((*iter), "e"); ++iter;
    TS_ASSERT_EQUALS((*iter), "f"); 
  }

  void test_lru_query() {
    // mixed insertions and querying
    lru_cache<std::string, size_t> cache;
    cache.set_size_limit(3);
    cache.insert("a", 1);
    cache.insert("b", 1);
    cache.insert("c", 1);
    cache.insert("d", 1); // bcd in cache
    cache.query("b"); // b is now head so c will be evicted next
    cache.insert("e", 1); // should be bde
    cache.insert("f", 1); // should be bef
    TS_ASSERT_EQUALS(cache.query("d").first, false);
    TS_ASSERT_EQUALS(cache.query("b").first, true);
    TS_ASSERT_EQUALS(cache.query("e").first, true);
    TS_ASSERT_EQUALS(cache.query("f").first, true);
    TS_ASSERT_EQUALS(cache.size(), 3);
  }

  void test_repeated_inserts() {
    lru_cache<std::string, size_t> cache;
    cache.set_size_limit(3);
    cache.insert("a", 1);
    cache.insert("b", 1);
    cache.insert("c", 1);
    cache.insert("d", 1); // bcd in cache
    cache.insert("b", 2); // b is now head so c is tail
    cache.insert("c", 2); // d is tail
    cache.insert("b", 3); // d is stil tail
    cache.insert("e", 1); // d is popped. should be b:3, c:2, e:1
    TS_ASSERT_EQUALS(cache.query("d").first, false);
    TS_ASSERT_EQUALS(cache.query("b").first, true);
    TS_ASSERT_EQUALS(cache.query("b").second, 3);
    TS_ASSERT_EQUALS(cache.query("c").first, true);
    TS_ASSERT_EQUALS(cache.query("c").second, 2);
    TS_ASSERT_EQUALS(cache.query("e").first, true);
    TS_ASSERT_EQUALS(cache.query("e").second, 1);
    TS_ASSERT_EQUALS(cache.size(), 3);
    // deletion
    cache.erase("e");
    TS_ASSERT_EQUALS(cache.size(), 2);
    TS_ASSERT_EQUALS(cache.query("b").first, true);
    TS_ASSERT_EQUALS(cache.query("c").first, true);
    cache.erase("b");
    TS_ASSERT_EQUALS(cache.size(), 1);
    TS_ASSERT_EQUALS(cache.query("c").first, true);
  }
};

