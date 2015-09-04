/**
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


#include <graphlab/util/union_find.hpp>
#include <random/random.hpp>
#include <parallel/pthread_tools.hpp>

graphlab::concurrent_union_find uf2;

void add_even() {
  for (size_t i = 2; i < 1000000; i+=2) {
    size_t unionwith = 0;
    while(1){
      unionwith = graphlab::random::fast_uniform((size_t)0, i - 1);
      if (unionwith % 2 == 0) break;
    }
    uf2.merge(i, unionwith);
  }
}


void add_odd() {
  for (size_t i = 3; i < 1000000; i+=2) {
    size_t unionwith = 0;
    while(1){
      unionwith = graphlab::random::fast_uniform((size_t)0, i - 1);
      if (unionwith % 2 == 1) break;
    }
    uf2.merge(i, unionwith);
  }
}


class UnionFindTest: public CxxTest::TestSuite {
 public:
  void test_union_find() {
    graphlab::union_find<size_t, size_t> uf;
    uf.init(1000);
    // union all the odd together and all the even together
    for (size_t i = 2; i < 1000; i+=2) {
      size_t unionwith = 0;
      while(1){
        unionwith = graphlab::random::fast_uniform((size_t)0, i - 1);
        if (unionwith % 2 == 0) break;
      }
      uf.merge(i, unionwith);
    }
    
    // union all the odd together and all the even together
    for (size_t i = 3; i < 1000; i+=2) {
      size_t unionwith = 0;
      while(1){
        unionwith = graphlab::random::fast_uniform((size_t)0, i - 1);
        if (unionwith % 2 == 1) break;
      }
      uf.merge(i, unionwith);
    }

    // assert that all evens are together and all odds are together
    size_t evenid = uf.find(0);
    for (size_t i = 0; i < 1000; i+=2) {
      TS_ASSERT_EQUALS(uf.find(i), evenid);
    }

    size_t oddid = uf.find(1);
    for (size_t i = 1; i < 1000; i+=2) {
      TS_ASSERT_EQUALS(uf.find(i), oddid);
    }
  }

  void test_union_find2() {
    uf2.init(1000000);

    graphlab::thread_group tg;
    tg.launch(add_even);
    tg.launch(add_even);
    tg.launch(add_even);
    tg.launch(add_odd);
    tg.launch(add_odd);
    tg.launch(add_odd);

    tg.join();


    // assert that all evens are together and all odds are together
    size_t evenid = uf2.find(0);
    for (size_t i = 0; i < 1000000; i+=2) {
      TS_ASSERT_EQUALS(uf2.find(i), evenid);
    }

    size_t oddid = uf2.find(1);
    for (size_t i = 1; i < 1000000; i+=2) {
      TS_ASSERT_EQUALS(uf2.find(i), oddid);
    }
  }
};
