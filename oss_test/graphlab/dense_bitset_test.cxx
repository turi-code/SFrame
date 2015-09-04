/*  
 * Copyright (c) 2009 Carnegie Mellon University. 
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
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <cxxtest/TestSuite.h>
#include <util/dense_bitset.hpp>
#include <graphlab/macros_def.hpp>
using namespace graphlab;

class DenseBitsetTestSuite : public CxxTest::TestSuite {
public:
  void test_densebitset(void) {
    dense_bitset d;
    d.resize(100);
    d.clear();
    size_t probelocations[7] = {0, 10, 12, 50, 66, 81, 99};
    // test setting
    for (size_t i= 0;i < 7; ++i) {
      d.set_bit(probelocations[i]);
    }
    
    for (size_t i = 0;i< 100; ++i) {
      bool inprobe=false;
      for (size_t j = 0;j <7; ++j) inprobe |= (probelocations[j] == i);
      TS_ASSERT_EQUALS(d.get(i), inprobe);
    }

    // test iteration
    size_t iter = (size_t)(-1);
    TS_ASSERT_EQUALS(d.first_bit(iter), true)
    for (size_t i= 0;i < 7; ++i) {
      TS_ASSERT_EQUALS(iter, probelocations[i]);
      bool ret = d.next_bit(iter);
      TS_ASSERT_EQUALS(ret, i < 6);
    }

    d.invert();
    // test zero iteration
    iter = (size_t)(-1);
    TS_ASSERT_EQUALS(d.first_zero_bit(iter), true)
    for (size_t i= 0;i < 7; ++i) {
      TS_ASSERT_EQUALS(iter, probelocations[i]);
      bool ret = d.next_zero_bit(iter);
      TS_ASSERT_EQUALS(ret, i < 6);
    }

    d.invert();

    size_t ctr = 0;
    foreach(iter, d) {
      TS_ASSERT(ctr < 7);
      TS_ASSERT_EQUALS(iter, probelocations[ctr]);
      ++ctr;
    }
    
    std::stringstream strm;
    graphlab::oarchive oarc(strm);
    oarc << d;
    strm.flush();
    graphlab::iarchive iarc(strm);
    dense_bitset d2;
    iarc >> d2;


    for (size_t i = 0;i< 100; ++i) {
      bool inprobe=false;
      for (size_t j = 0;j <7; ++j) inprobe |= (probelocations[j] == i);
      TS_ASSERT_EQUALS(d2.get(i), inprobe);
    }
    // testclearing
    for (size_t i= 0;i < 7; ++i) {
      d.clear_bit(probelocations[i]);
    }
    for (size_t i = 0;i< 100; ++i) {
      TS_ASSERT_EQUALS(d.get(i), false);
    }

    d.fill();
    TS_ASSERT_EQUALS(d.popcount(), d.size());
    d.invert();
    TS_ASSERT_EQUALS(d.popcount(), 0);
    d.invert();
    TS_ASSERT_EQUALS(d.popcount(), d.size());

    d2.fill();
    TS_ASSERT_EQUALS(d2.popcount(), d2.size());


  }


  void test_fixeddensebitset(void) {
    fixed_dense_bitset<100> d;
    size_t probelocations[7] = {0, 10, 12, 50, 66, 81, 99};
    // test setting
    for (size_t i= 0;i < 7; ++i) {
      d.set_bit(probelocations[i]);
    }
    
    for (size_t i = 0;i< 100; ++i) {
      bool inprobe=false;
      for (size_t j = 0;j <7; ++j) inprobe |= (probelocations[j] == i);
      TS_ASSERT_EQUALS(d.get(i), inprobe);
    }

    // test iteration
    size_t iter = (size_t)(-1);
    TS_ASSERT_EQUALS(d.first_bit(iter), true)
    for (size_t i= 0;i < 7; ++i) {
      TS_ASSERT_EQUALS(iter, probelocations[i]);
      bool ret = d.next_bit(iter);
      TS_ASSERT_EQUALS(ret, i < 6);
    }
    
    size_t ctr = 0;
    foreach(iter, d) {
      TS_ASSERT(ctr < 7);
      TS_ASSERT_EQUALS(iter, probelocations[ctr]);
      ++ctr;
    }

    std::stringstream strm;
    graphlab::oarchive oarc(strm);
    oarc << d;
    strm.flush();
    graphlab::iarchive iarc(strm);
    fixed_dense_bitset<100> d2;
    iarc >> d2;


    for (size_t i = 0;i< 100; ++i) {
      bool inprobe=false;
      for (size_t j = 0;j <7; ++j) inprobe |= (probelocations[j] == i);
      TS_ASSERT_EQUALS(d2.get(i), inprobe);
    }
    
    
    // testclearing
    for (size_t i= 0;i < 7; ++i) {
      d.clear_bit(probelocations[i]);
    }
    for (size_t i = 0;i< 100; ++i) {
      TS_ASSERT_EQUALS(d.get(i), false);
    }

    d.fill();
    TS_ASSERT_EQUALS(d.popcount(), d.size());

    d2.fill();
    TS_ASSERT_EQUALS(d2.popcount(), d2.size());

  }


};

