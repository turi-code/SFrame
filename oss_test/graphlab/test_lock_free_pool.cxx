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


#include <graphlab/util/lock_free_pool.hpp>
#include <parallel/pthread_tools.hpp>
#include <logger/assertions.hpp>

using namespace graphlab;
lock_free_pool<size_t> pool;

void exec() {
  size_t *s = NULL;
  for (size_t i = 0;i < 1000000; ++i) {
    while(1) {
      s = pool.alloc();
      if (s == NULL) continue;
      else {
       for (size_t j = 0;j < 10; ++j) (*s)++;
       pool.free(s);
       break;
      }
    } 
  }
}


class LockFreePoolTestSuite: public CxxTest::TestSuite {
 public:  
  void test_lock_free_pool() {
    size_t nthreads = 8;
    
    pool.reset_pool(32);
    thread_group g;
    for (size_t i = 0; i < nthreads; ++i) {
      g.launch(exec);
    }
    while(1) {
      try {
        g.join();
        break;
      }
      catch(const char* c ) {
        std::cout << c << "\n";
      }
    }
    
    std::vector<size_t> alldata = pool.unsafe_get_pool_ref();
    size_t total = 0;
    for (size_t i = 0;i < alldata.size(); ++i) {
      total += alldata[i];
    }
    TS_ASSERT_EQUALS(total, 10000000 * nthreads);
  }
};
