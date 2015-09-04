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


#include <iostream>

#include <parallel/pthread_tools.hpp>
#include <parallel/thread_pool.hpp>
#include <parallel/atomic.hpp>
#include <logger/assertions.hpp>
#include <timer/timer.hpp>
#include <boost/bind.hpp>
#include <thread>
#include <chrono>

using namespace graphlab;

atomic<int> testval;

void test_inc() {
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  testval.inc();
}

void test_dec() {
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  testval.dec();
}



void thread_assert_false() {
  ASSERT_TRUE(false);
}


void test_group_exception_forwarding(){
  std::cout << "\n";
  std::cout << "----------------------------------------------------------------\n";
  std::cout << "This test will print a  large number of assertional failures\n";
  std::cout << "and back traces. This is intentional as we are testing the\n" ;
  std::cout << "exception forwarding scheme\n";
  std::cout << "----------------------------------------------------------------\n";
  std::cout << std::endl;

  thread_group group;

  
  thread thr3;
  thr3.launch(thread_assert_false);
  try {
    thr3.join();
  }
  catch(const char* c) {
    logstream(LOG_INFO) << "Exception " << c << " forwarded successfully!" << std::endl;
  }
  
  
  for (size_t i = 0;i < 10; ++i) {
    group.launch(thread_assert_false);
  }
  
  size_t numcaught = 0;
  try {
    group.join();
  }
  catch (std::string c){
    logstream(LOG_INFO) << "Exception " << c << " forwarded successfully!" << std::endl;
    numcaught++;
  }
  logstream(LOG_INFO) << "Caught " << numcaught << " exceptions!" << std::endl;
  TS_ASSERT(numcaught > 0);
}

void test_pool(){
  testval.value = 0;
  thread_pool pool(4);
  for (size_t j = 0;j < 10; ++j) {
    for (size_t i = 0;i < 10; ++i) {
      pool.launch(test_inc);
    }
    for (size_t i = 0;i < 10; ++i) {
      pool.launch(test_dec);
    }
    pool.set_cpu_affinity(j % 2);
  }
  
  pool.join();
  TS_ASSERT_EQUALS(testval.value, 0);
}

void test_pool_exception_forwarding(){
  std::cout << "\n";
  std::cout << "----------------------------------------------------------------\n";
  std::cout << "This test will print a  large number of assertional failures\n";
  std::cout << "and back traces. This is intentional as we are testing the\n" ;
  std::cout << "exception forwarding scheme\n";
  std::cout << "----------------------------------------------------------------\n";
  std::cout << std::endl;
  thread_pool thpool(10);
  parallel_task_queue pool(thpool);


  
  thread thr3;
  thr3.launch(thread_assert_false);
  try {
    thr3.join();
  }
  catch(std::string c) {
    logstream(LOG_INFO) << "Exception " << c << " forwarded successfully!" << std::endl;
  }
  
  
  for (size_t i = 0;i < 10; ++i) {
    pool.launch(thread_assert_false);
    if (i == 50) {
      thpool.set_cpu_affinity(true);
    }
  }
  
  size_t numcaught = 0;
  while (1) {
    try {
      pool.join();
      break;
    }
    catch (std::string c){
      logstream(LOG_INFO) << "Exception " << c << " forwarded successfully!" << std::endl;
      numcaught++;
    }
  }
  logstream(LOG_INFO) << "Caught " << numcaught << " exceptions!" << std::endl;
  TS_ASSERT(numcaught > 0);
}






class ThreadToolsTestSuite : public CxxTest::TestSuite {
public:

  void test_thread_pool(void) {
   test_pool();
  }

  // TODO: Make this test WORK again
  void thread_group_exception(void) {
    test_group_exception_forwarding();
  }

  void  test_thread_pool_exception(void) {
    test_pool_exception_forwarding();
  }

};
