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
#include <fileio/block_cache.hpp>
#include <fileio/temp_files.hpp>
#include <random/random.hpp>
#include <cxxtest/TestSuite.h>

using namespace graphlab;


class block_cache_test: public CxxTest::TestSuite {

 public:

  void test_block_cache() {
    block_cache cache;
    cache.init(get_temp_directories()[0] + "/");
    size_t num_keys = 1024;
    size_t num_probes = 4;

    // for each key, we insert a deterministic sequence of values
    for (size_t key = 0; key < num_keys; ++key) {
      std::string value;
      value.resize(256);
      for (size_t i = 0;i < 256; ++i) {
        value[i] = (i + key) % 256;
      }

      cache.write(std::to_string(key), value);
    }


    random::seed(10001);
    // check every key, selecting a random sub sequence at each time.
    for (size_t key = 0; key < num_keys; ++key) {
      for (size_t nprobes = 0; nprobes < num_probes; ++nprobes) {
        TS_ASSERT_EQUALS(cache.value_length(std::to_string(key)), 256);
        // generate a random start-end sequence to read
        size_t start = random::fast_uniform<size_t>(0, 255);
        size_t end = random::fast_uniform<size_t>(0, 256);
        std::string value;
        auto ret = cache.read(std::to_string(key), value, start, end);
        // make sure the values we read were valid
        auto expected_ret = start < end ? end - start : 0;
        TS_ASSERT_EQUALS(ret, expected_ret);
        for (size_t i = start; i < end; ++i) {
          size_t offset = i - start;
          TS_ASSERT_EQUALS((size_t)(unsigned char)(value[offset]), (i + key) % 256);
        }
      }
    }

    // check that the cache is operating correctly.
    // We should have num_key misses (one for new block read)
    // and for each subsequent read inside the same key, that should be a hit.
    TS_ASSERT_EQUALS(cache.file_handle_cache_misses(), num_keys);
    TS_ASSERT_EQUALS(cache.file_handle_cache_hits(), num_keys * (num_probes - 1));
  }
  void test_block_cache_evict() {
    block_cache cache;
    cache.init(get_temp_directories()[0] + "/" + "evict_test_");
    size_t num_keys = 1024;

    // for each key, we insert a deterministic sequence of values
    for (size_t key = 0; key < num_keys; ++key) {
      std::string value;
      value.resize(256);
      for (size_t i = 0;i < 256; ++i) {
        value[i] = (i + key) % 256;
      }

      cache.write(std::to_string(key), value);
    }
    std::string value;
    auto ret = cache.read(std::to_string(0), value, 0, 256);
    TS_ASSERT_EQUALS(ret, 256);

    TS_ASSERT_EQUALS(cache.evict_key(std::to_string(0)), true);
    TS_ASSERT_EQUALS(cache.evict_key(std::to_string(1023)), true);
    TS_ASSERT_EQUALS(cache.evict_key(std::to_string(0)), false);
    TS_ASSERT_EQUALS(cache.evict_key(std::to_string(1023)), false);

    // nonexistant key
    TS_ASSERT_EQUALS(cache.evict_key(std::to_string(1024)), false);

    ret = cache.read(std::to_string(0), value, 0, 256);
    TS_ASSERT_EQUALS(ret, -1);
    ret = cache.read(std::to_string(1023), value, 0, 256);
    TS_ASSERT_EQUALS(ret, -1);
    // all keys are readable
    for (size_t i = 1; i < 1023; ++i) {
      ret = cache.read(std::to_string(i), value, 0, 256);
      TS_ASSERT_EQUALS(ret, 256);
    }

  }
};

