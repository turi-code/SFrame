/*
* Copyright (C) 2016 Turi
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




#include <cxxtest/TestSuite.h>

#include <cmath>
#include <iostream>
#include <vector>

#include <graphlab.hpp>


typedef double vertex_data_type;
typedef double edge_data_type;



template<typename NumType>
void uniform_speed(const size_t max_iter) {
  NumType sum(0);
  graphlab::timer ti;
  ti.start();
  for(size_t i = 0; i < max_iter; ++i) {
    sum += (NumType)(graphlab::random::uniform<NumType>(0, 10));
  }
  double slow_time = ti.current_time();
  ti.start();
  for(size_t i = 0; i < max_iter; ++i) {
    sum += (NumType)(graphlab::random::fast_uniform<NumType>(0, 10));
  }
  double fast_time = ti.current_time();
  std::cout << slow_time << ", " << fast_time << std::endl; 
}


class thread_worker {
public:
  std::vector<int> values;
  void run() {
    namespace random = graphlab::random;
    for(size_t i = 0; i < values.size(); ++i) {
      values[i] = random::uniform<int>(0,3);
    }
  }
};

template<typename T>
std::ostream& operator<<(std::ostream& out, const std::vector<T>& values) {
  out << "{";
  for(size_t i = 0; i < values.size(); ++i) {
    out << values[i];
    if(i + 1 < values.size()) out << ", ";
  }
  return out << "}";
}


std::vector<int> operator+(const std::vector<int>& v1, 
                           const std::vector<int>& v2) {
  assert(v1.size() == v2.size());
  std::vector<int> result(v1.size());
  for(size_t i = 0; i < result.size(); ++i) {
    result[i] = v1[i] + v2[i];
  }
  return result;
}




class RandomTestSuite: public CxxTest::TestSuite {
  size_t iterations;
  
  public:

  RandomTestSuite() : iterations(1E8) { }
  
 
  void test_nondet_generator() {
    graphlab::random::nondet_seed();
    graphlab::random::nondet_seed();
    graphlab::random::nondet_seed();
  }


  void test_random_number_generators() {
    std::cout << std::endl;
    std::cout << "beginning seed" << std::endl;
    namespace random = graphlab::random;
    graphlab::random::seed();
    graphlab::random::time_seed();
    graphlab::random::nondet_seed();
    graphlab::random::seed(12345);
    std::cout << "finished" << std::endl;

    const size_t num_iterations(20);
    std::vector<thread_worker> workers(10);
    for(size_t i = 0; i < workers.size(); ++i) 
      workers[i].values.resize(num_iterations);
    graphlab::thread_group threads;
    for(size_t i = 0; i < workers.size(); ++i) {
      threads.launch(boost::bind(&thread_worker::run, &(workers[i])));
    }
    threads.join();
    for(size_t i = 0; i < workers.size(); ++i) {
      std::cout << workers[i].values << std::endl;
    }
    std::vector<int> sum(workers[0].values.size());
    for(size_t i = 0; i < workers.size(); ++i) {
      sum = sum + workers[i].values;
    }
    std::cout << "Result: " << sum << std::endl;
  }





  void shuffle() {
    namespace random = graphlab::random;
    random::nondet_seed();
    std::vector<int> numbers(100);
    for(size_t i = 0; i < numbers.size(); ++i) numbers[i] = (int)i + 1;
    for(size_t j = 0; j < 10; ++j) {
      // shuffle the numbers
      random::shuffle(numbers);
      std::cout << numbers << std::endl;
    }
  }



  // void speed() {
  //   namespace random = graphlab::random;
  //   std::cout << "speed test run: " << std::endl;
  //   const size_t MAX_ITER(10000);
  //   std::cout << "size_t:   "; 
  //   uniform_speed<size_t>(MAX_ITER);
  //   std::cout << "int:      "; 
  //   uniform_speed<int>(MAX_ITER);
  //   std::cout << "uint32_t: "; 
  //   uniform_speed<uint32_t>(MAX_ITER);
  //   std::cout << "uint16_t: "; 
  //   uniform_speed<uint16_t>(MAX_ITER);
  //   std::cout << "char:     "; 
  //   uniform_speed<char>(MAX_ITER);
  //   std::cout << "float:    "; 
  //   uniform_speed<float>(MAX_ITER);
  //   std::cout << "double:   "; 
  //   uniform_speed<double>(MAX_ITER);
    
  //   std::cout << "gaussian: ";
  //   double sum = 0;
  //   graphlab::timer time;
  //   time.start();
  //   for(size_t i = 0; i < MAX_ITER; ++i) 
  //     sum += random::gaussian();
  //   std::cout << time.current_time() << std::endl;
    
  //   std::cout << "shuffle:  "; 
  //   std::vector<int> numbers(6);
  //   for(size_t i = 0; i < numbers.size(); ++i) numbers[i] = (int)i + 1;
  //   time.start();
  //   for(size_t j = 0; j < MAX_ITER/numbers.size(); ++j) {
  //     // shuffle the numbers
  //     random::shuffle(numbers);
  //   }
  //   std::cout << time.current_time() << ", ";
  //   time.start();
  //   for(size_t j = 0; j < MAX_ITER/numbers.size(); ++j) {
  //     // shuffle the numbers
  //     std::random_shuffle(numbers.begin(), numbers.end());
  //   }
  //   std::cout << time.current_time() << std::endl;    
  // }


  
  
};
