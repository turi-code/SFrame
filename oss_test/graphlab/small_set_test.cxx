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


#include <vector>
#include <algorithm>
#include <iostream>

#include <boost/unordered_set.hpp>

#include <cxxtest/TestSuite.h>

#include <graphlab/util/small_set.hpp>
#include <util/stl_util.hpp>

using namespace graphlab;

#include <graphlab/macros_def.hpp>
class test_small_set : public CxxTest::TestSuite {
public:

  
  void test_union() {
    std::cout << std::endl;
    std::cout << "Testing set union" << std::endl;

    typedef small_set<10, int> set_type;
    typedef small_set<5, int> small_set_type;
    small_set<0, int> empty_set;
    small_set<10, int> set1;
    small_set<10, int> set2;
    set1 += set_type(1) + small_set_type(3) + set_type(2) + empty_set;
    set1 += 1;
    set1 += 3;
    set1 += 2;
    set1 += empty_set;
    set1 += 1;
    std::set<int> true_set1;
    true_set1.insert(1);
    true_set1.insert(2);
    true_set1.insert(3);
    ASSERT_EQ(set_type(true_set1), set1);
    std::cout << "set1: " << set1 << std::endl;
    set2 += set_type(2) + small_set_type(5) + small_set_type(3) + set_type(7);
    set2.insert(0);
    set2 += 7;
    set2 += 0;

    std::set<int> true_set2;
    true_set2.insert(0);
    true_set2.insert(2);
    true_set2.insert(5);
    true_set2.insert(3);
    true_set2.insert(7);
    ASSERT_EQ(set_type(true_set2), set2);    
    std::cout << "set2: " << set2 << std::endl;

    small_set<7, int> set3 = set1 + set2;
    std::set<int> true_set3 = set_union(true_set1, true_set2);
    ASSERT_EQ(set_type(true_set3), set3);
    std::cout << "set3 = set1 + set2: " << set3 << std::endl;
    std::cout << "set3 + set3: " << (set3  + set3) << std::endl;
    ASSERT_EQ(set_type(true_set3), (set3 + set3));    
  }



  void test_intersection() {
    std::cout << std::endl;
    std::cout << "Testing set union" << std::endl;

    typedef small_set<10, int> set_type;
    typedef small_set<5, int> small_set_type;
    small_set<0, int> empty_set;
    small_set<10, int> set1;
    small_set<10, int> set2;
    set1 += set_type(1) + small_set_type(3) + set_type(2) + empty_set;
    set1.insert(8);
    // do some intersections
    set1 *= set1;
    set1 = set1 * set1;
    std::set<int> true_set1;
    true_set1.insert(1);
    true_set1.insert(2);
    true_set1.insert(3);
    true_set1.insert(8);
    ASSERT_EQ(set_type(true_set1), set1);
    std::cout << "set1: " << set1 << std::endl;
    
    set2 += set_type(2) + small_set_type(5) + small_set_type(3) + set_type(7);
    set2.insert(0);
    set2 += 4;

    std::set<int> true_set2;
    true_set2.insert(0);
    true_set2.insert(2);
    true_set2.insert(5);
    true_set2.insert(3);
    true_set2.insert(7);
    true_set2.insert(4);
    ASSERT_EQ(set_type(true_set2), set2);    
    std::cout << "set2: " << set2 << std::endl;

    small_set<7, int> set3 = set1 * set2;
    std::set<int> true_set3 = set_intersect(true_set1, true_set2);
    ASSERT_EQ(set_type(true_set3), set3);
    std::cout << "set3 = set1 * set2: " << set3 << std::endl;
    std::cout << "set3 * set3: " << (set3  + set3) << std::endl;
    ASSERT_EQ(set_type(true_set3), (set3 + set3));    
  }


  void test_difference() {
    std::cout << std::endl;
    std::cout << "Testing set diff" << std::endl;

    typedef small_set<10, int> set_type;
    typedef small_set<5, int> small_set_type;
    small_set<0, int> empty_set;
    small_set<10, int> set1;
    small_set<10, int> set2;
    set1 += set_type(1) + small_set_type(3) + set_type(2) + empty_set;
    set1.insert(8);
    // do some intersections
    ASSERT_EQ(empty_set, set1 - set1);
    ASSERT_EQ(empty_set, empty_set - empty_set);
    empty_set = (empty_set - set1);
    ASSERT_EQ(empty_set, empty_set - set1);
    ASSERT_EQ(set1, set1 - empty_set);
    std::set<int> true_set1;
    true_set1.insert(1);
    true_set1.insert(2);
    true_set1.insert(3);
    true_set1.insert(8);
    ASSERT_EQ(set_type(true_set1), set1);
    std::cout << "set1: " << set1 << std::endl;
    set2 += set_type(2) + small_set_type(5) + small_set_type(3) + set_type(7);
    set2.insert(0);
    set2 += 4;
    std::set<int> true_set2;
    true_set2.insert(0);
    true_set2.insert(2);
    true_set2.insert(5);
    true_set2.insert(3);
    true_set2.insert(7);
    true_set2.insert(4);
    ASSERT_EQ(set_type(set_difference(true_set1, true_set2)),
              set1 - set2);
    ASSERT_EQ(set_type(set_difference(true_set2, true_set1)),
              set2 - set1);
  }

  void test_range_iteration() {
    typedef std::pair<int, std::string> pair_type;
    typedef small_set<20, pair_type > set_type;
    set_type set = 
      set_type(std::make_pair(1, "hello")) + 
      set_type(std::make_pair(2, "world"));
    foreach(const pair_type& value, set) {
      std::cout << value.first << value.second << ", ";
    }
    std::cout << std::endl;
  }

  

};

#include <graphlab/macros_undef.hpp>
