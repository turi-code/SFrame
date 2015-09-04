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


#include <boost/bind.hpp>
#include <cxxtest/TestSuite.h>
#include <parallel/pthread_tools.hpp>
#include <parallel/lockfree_push_back.hpp>
using namespace graphlab;

std::vector<size_t> vec;
lockfree_push_back<std::vector<size_t> > pusher(vec, 0);

void testthread(size_t range0, size_t range1) {
  for (size_t i = range0;i < range1; ++i) {
    pusher.push_back(i);
  }
}

class LockFreePushBack : public CxxTest::TestSuite {
public:

  void test_lockfree_push_back(void) {
    thread_group thr;
    for (size_t i = 0;i < 16; ++i) {
      thr.launch(boost::bind(testthread, i * 100000, (i+1) * 100000));
    }

    thr.join();

    TS_ASSERT_EQUALS(pusher.size(), (size_t)16 * 100000);
    vec.resize(pusher.size());
    std::sort(vec.begin(), vec.end());
    for (size_t i = 0;i < vec.size(); ++i) {
      TS_ASSERT_EQUALS(vec[i], i);
    }
  }
};

