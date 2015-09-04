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
#include <map>

#include <boost/unordered_set.hpp>

#include <cxxtest/TestSuite.h>

#include <graphlab/util/small_map.hpp>
#include<util/stl_util.hpp>

using namespace graphlab;

#include <graphlab/macros_def.hpp>
class test_small_map : public CxxTest::TestSuite {
public:

  void test_lookup() {
    typedef small_map<32, size_t, double> map_type;
    map_type map;
    std::map<size_t, double> stdmap;
    map[5] = stdmap[5] = 5.1;
    map[1] = stdmap[1] = 1.1;
    map[2] = stdmap[2] = 2.1;    
    TS_ASSERT_EQUALS(map.size(), stdmap.size());
    std::cout << std::endl;
    std::cout << map << std::endl;
    typedef std::pair<size_t, double> pair_type;
    foreach(pair_type pair, stdmap) {
      TS_ASSERT_EQUALS(map[pair.first], pair.second);
      TS_ASSERT_EQUALS(map.safe_find(pair.first), pair.second);
      ASSERT_TRUE(map.has_key(pair.first));
    }
    foreach(pair_type pair, map) {
      TS_ASSERT_EQUALS(stdmap[pair.first], pair.second);
    }

    map_type map2;
    std::map<size_t, double> stdmap2;
    map2[0] = stdmap2[0] = 0.2;
    map2[5] = stdmap2[5] = 5.2;
    map2[2] = stdmap2[2] = 2.2;    
    map2[1] = stdmap2[1] = 1.2;
    map2[8] = stdmap2[8] = 8.2;
    TS_ASSERT_EQUALS(map2.size(), stdmap2.size());
    map_type map3 = map + map2;
    std::map<size_t, double> stdmap3 = 
      graphlab::map_union(stdmap, stdmap2);

    std::cout << map3 << std::endl;
    
    foreach(pair_type pair, stdmap3) {
      TS_ASSERT_EQUALS(map3[pair.first], pair.second);
      TS_ASSERT_EQUALS(map3.safe_find(pair.first), pair.second);
      ASSERT_TRUE(map3.has_key(pair.first));
    }
    foreach(pair_type pair, map3) {
      TS_ASSERT_EQUALS(stdmap3[pair.first], pair.second);
    }

    
  }


  // void test_lookup() {
  //   typedef small_map<32, size_t, std::string> map_type;
  //   map_type map;
  //   std::map<size_t, std::string> stdmap;
  //   map[5] = stdmap[5] = "five";
  //   map[1] = stdmap[1] = "one";
  //   map[2] = stdmap[2] = "two";    
  //   TS_ASSERT_EQUALS(map.size(), stdmap.size());
  //   std::cout << std::endl;
  //   std::cout << map << std::endl;
  //   typedef std::pair<size_t, std::string> pair_type;
  //   foreach(pair_type pair, stdmap) {
  //     TS_ASSERT_EQUALS(map[pair.first], pair.second);
  //     TS_ASSERT_EQUALS(map.safe_find(pair.first), pair.second);
  //     ASSERT_TRUE(map.has_key(pair.first));
  //   }
  //   foreach(pair_type pair, map) {
  //     TS_ASSERT_EQUALS(stdmap[pair.first], pair.second);
  //   }

  //   map_type map2;
  //   std::map<size_t, std::string> stdmap2;
  //   map2[0] = stdmap2[0] = "ZERO";
  //   map2[5] = stdmap2[5] = "FIVE";
  //   map2[2] = stdmap2[2] = "TWO";    
  //   map2[1] = stdmap2[1] = "ONE";
  //   map2[8] = stdmap2[8] = "EIGHT";
  //   TS_ASSERT_EQUALS(map2.size(), stdmap2.size());
  //   map_type map3 = map + map2;
  //   std::map<size_t, std::string> stdmap3 = 
  //     graphlab::map_union(stdmap, stdmap2);

  //   std::cout << map3 << std::endl;
    
  //   foreach(pair_type pair, stdmap3) {
  //     TS_ASSERT_EQUALS(map3[pair.first], pair.second);
  //     TS_ASSERT_EQUALS(map3.safe_find(pair.first), pair.second);
  //     ASSERT_TRUE(map3.has_key(pair.first));
  //   }
  //   foreach(pair_type pair, map3) {
  //     TS_ASSERT_EQUALS(stdmap3[pair.first], pair.second);
  //   }

    
  // }



};
#include <graphlab/macros_undef.hpp>
