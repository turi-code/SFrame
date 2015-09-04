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

#include <sstream>
#include <graphlab/util/cuckoo_map.hpp>
#include <graphlab/util/cuckoo_map_pow2.hpp>
#include <graphlab/util/cuckoo_set_pow2.hpp>
#include <timer/timer.hpp>
#include <random/random.hpp>
#include <perf/memory_info.hpp>
#include <boost/unordered_map.hpp>
#include <logger/assertions.hpp>
#include <serialization/serialization_includes.hpp>
#include <graphlab/macros_def.hpp>

void sanity_checks() {
  boost::unordered_map<size_t, size_t> um;
  graphlab::cuckoo_map_pow2<size_t, size_t> cm(-1);
  ASSERT_TRUE(cm.begin() == cm.end());
  for (size_t i = 0;i < 10000; ++i) {
    cm[17 * i] = i;
    um[17 * i] = i;
  }

  for (size_t i = 0;i < 10000; ++i) {
    assert(cm[17 * i] == i);
    assert(um[17 * i] == i);
  }
  assert(cm.size() == 10000);
  assert(um.size() == 10000);

  for (size_t i = 0;i < 10000; i+=2) {
    cm.erase(17*i);
    um.erase(17*i);
  }
  for (size_t i = 0;i < 10000; i+=2) {
    assert(cm.count(17*i) == i % 2);
    assert(um.count(17*i) == i % 2);
    if (cm.count(17*i)) {
      assert(cm.find(17*i)->second == i);
    }
  }

  assert(cm.size() == 5000);
  assert(um.size() == 5000);

  typedef graphlab::cuckoo_map_pow2<size_t, size_t, (size_t)(-1)>::value_type vpair;
  {
    size_t cnt = 0;
    foreach(vpair &v, cm) {
      ASSERT_EQ(v.second, um[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, 5000);
  }
  {
    size_t cnt = 0;
    foreach(const vpair &v, cm) {
      ASSERT_EQ(v.second, um[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, 5000);
  }
}




void sanity_checks2() {
  boost::unordered_map<size_t, size_t> um;
  graphlab::cuckoo_map<size_t, size_t> cm(-1);
  ASSERT_TRUE(cm.begin() == cm.end());

  for (size_t i = 0;i < 10000; ++i) {
    cm[17 * i] = i;
    um[17 * i] = i;
  }

  for (size_t i = 0;i < 10000; ++i) {
    assert(cm[17 * i] == i);
    assert(um[17 * i] == i);
  }
  assert(cm.size() == 10000);
  assert(um.size() == 10000);

  for (size_t i = 0;i < 10000; i+=2) {
    cm.erase(17*i);
    um.erase(17*i);
  }
  for (size_t i = 0;i < 10000; i+=2) {
    assert(cm.count(17*i) == i % 2);
    assert(um.count(17*i) == i % 2);
    if (cm.count(17*i)) {
      assert(cm.find(17*i)->second == i);
    }
  }

  assert(cm.size() == 5000);
  assert(um.size() == 5000);

  typedef graphlab::cuckoo_map<size_t, size_t, (size_t)(-1)>::value_type vpair;
  {
    size_t cnt = 0;
    foreach(vpair &v, cm) {
      ASSERT_EQ(v.second, um[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, 5000);
  }
  {
    size_t cnt = 0;
    foreach(const vpair &v, cm) {
      ASSERT_EQ(v.second, um[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, 5000);
  }
}

std::string randstring(size_t len) {
  std::string ret; ret.reserve(len);
  for (size_t i = 0;i < len; ++i) {
    ret = ret + graphlab::random::fast_uniform('A','Z');
  }
  return ret;
}

void more_interesting_data_types_check() {
  boost::unordered_map<std::string, std::string> um;
  graphlab::cuckoo_map_pow2<std::string, std::string> cm("");
  for (size_t i = 0;i < 10000; ++i) {
    std::string s = randstring(16);
    cm[s] = s;
    um[s] = s;
  }

  assert(cm.size() == 10000);
  assert(um.size() == 10000);

  
  typedef boost::unordered_map<std::string, std::string>::value_type vpair;
  foreach(vpair& v, um) {
    ASSERT_EQ(v.second, cm[v.first]);
  }


  foreach(vpair& v, cm) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  foreach(const vpair& v, cm) {
    ASSERT_EQ(v.second, um[v.first]);
  }

  // test assignment
  graphlab::cuckoo_map_pow2<std::string, std::string> cm2("");
  cm2 = cm;

  foreach(vpair& v, um) {
    ASSERT_EQ(v.second, cm2[v.first]);
  }


  foreach(vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  foreach(const vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }

  std::stringstream strm;
  graphlab::oarchive oarc(strm);
  oarc << cm;
  strm.flush();

  
  cm2.clear();
  ASSERT_EQ(cm2.size(), 0);
  graphlab::iarchive iarc(strm);
  iarc >> cm2;
  ASSERT_EQ(cm2.size(), 10000);

  foreach(vpair& v, um) {
    ASSERT_EQ(v.second, cm2[v.first]);
  }


  foreach(vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  foreach(const vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }
}


void more_interesting_data_types_check2() {
  boost::unordered_map<std::string, std::string> um;
  graphlab::cuckoo_map<std::string, std::string> cm("");
  for (size_t i = 0;i < 10000; ++i) {
    std::string s = randstring(16);
    cm[s] = s;
    um[s] = s;
  }

  assert(cm.size() == 10000);
  assert(um.size() == 10000);


  typedef boost::unordered_map<std::string, std::string>::value_type vpair;
  foreach(vpair& v, um) {
    ASSERT_EQ(v.second, cm[v.first]);
  }


  foreach(vpair& v, cm) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  foreach(const vpair& v, cm) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  // test assignment
  graphlab::cuckoo_map<std::string, std::string> cm2("");
  cm2 = cm;

  foreach(vpair& v, um) {
    ASSERT_EQ(v.second, cm2[v.first]);
  }


  foreach(vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  foreach(const vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  std::stringstream strm;
  graphlab::oarchive oarc(strm);
  oarc << cm;
  strm.flush();


  cm2.clear();
  ASSERT_EQ(cm2.size(), 0);
  graphlab::iarchive iarc(strm);
  iarc >> cm2;
  ASSERT_EQ(cm2.size(), 10000);

  foreach(vpair& v, um) {
    ASSERT_EQ(v.second, cm2[v.first]);
  }


  foreach(vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }


  foreach(const vpair& v, cm2) {
    ASSERT_EQ(v.second, um[v.first]);
  }
}


void benchmark() {
  graphlab::timer ti;

  size_t NUM_ELS = 10000000;
  
  std::vector<uint32_t> v;
  uint32_t u = 0;
  for (size_t i = 0;i < NUM_ELS; ++i) {
    v.push_back(u);
    u += 1 + rand() % 8;
  }
  std::random_shuffle(v.begin(), v.end());
  graphlab::memory_info::print_usage();

  {
    boost::unordered_map<uint32_t, uint32_t> um;
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      um[v[i]] = i;
    }
    std::cout <<  NUM_ELS / 1000000 << "M unordered map inserts in " << ti.current_time() << " (Load factor = " << um.load_factor() << ")" << std::endl;

    graphlab::memory_info::print_usage();
    
    ti.start();
    for (size_t i = 0;i < 10000000; ++i) {
      size_t t = um[v[i]];
      assert(t == i);
    }
    std::cout << "10M unordered map successful probes in " << ti.current_time() << std::endl;
    um.clear();
  }

  {
    graphlab::cuckoo_map<uint32_t, uint32_t, 3, uint32_t> cm(-1, 128);

    //cm.reserve(102400);
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      cm[v[i]] = i;
      if (i % 1000000 == 0) std::cout << cm.load_factor() << std::endl;

    }
    std::cout <<  NUM_ELS / 1000000 << "M cuckoo map inserts in " << ti.current_time() << " (Load factor = " << cm.load_factor() << ")" << std::endl;

    graphlab::memory_info::print_usage();

    ti.start();
    for (size_t i = 0;i < 10000000; ++i) {
      size_t t = cm[v[i]];
      assert(t == i);
    }
    std::cout << "10M cuckoo map successful probes in " << ti.current_time() << std::endl;

  }
  
  {
    graphlab::cuckoo_map_pow2<uint32_t, uint32_t, 3, uint32_t> cm(-1, 128);
    
    //cm.reserve(102400);
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      cm[v[i]] = i;
      if (i % 1000000 == 0) std::cout << cm.load_factor() << std::endl;

    }
    std::cout << NUM_ELS / 1000000 << "M cuckoo map pow2 inserts in " << ti.current_time() << " (Load factor = " << cm.load_factor() << ")" << std::endl;

    graphlab::memory_info::print_usage();

    ti.start();
    for (size_t i = 0;i < 10000000; ++i) {
      size_t t = cm[v[i]];
      assert(t == i);
    }
    std::cout << "10M cuckoo map pow2 successful probes in " << ti.current_time() << std::endl;

  }
}




void benchmark_strings() {
  graphlab::timer ti;

  size_t NUM_ELS = 1000000;

  std::vector<std::string> v;
  for (size_t i = 0;i < NUM_ELS; ++i) {
    v.push_back(randstring(16));
  }
  graphlab::memory_info::print_usage();

  {
    boost::unordered_map<std::string, std::string> um;
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      um[v[i]] = v[i];
    }
    std::cout <<  NUM_ELS / 1000000 << "M unordered map inserts in " << ti.current_time() << " (Load factor = " << um.load_factor() << ")" << std::endl;

    graphlab::memory_info::print_usage();

    ti.start();
    for (size_t i = 0;i < 1000000; ++i) {
      std::string t = um[v[i]];
      assert(t == v[i]);
    }
    std::cout << "1M unordered map successful probes in " << ti.current_time() << std::endl;
    um.clear();
  }

  {
    graphlab::cuckoo_map<std::string, std::string, 3, uint32_t> cm("", 128);

    //cm.reserve(102400);
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      cm[v[i]] = v[i];
      if (i % 1000000 == 0) std::cout << cm.load_factor() << std::endl;

    }
    std::cout <<  NUM_ELS / 1000000 << "M cuckoo map inserts in " << ti.current_time() << " (Load factor = " << cm.load_factor() << ")" << std::endl;

    graphlab::memory_info::print_usage();

    ti.start();
    for (size_t i = 0;i < 1000000; ++i) {
      std::string t = cm[v[i]];
      assert(t == v[i]);
    }
    std::cout << "1M cuckoo map successful probes in " << ti.current_time() << std::endl;

  }

  {
    graphlab::cuckoo_map_pow2<std::string, std::string, 3, uint32_t> cm("", 128);

    //cm.reserve(102400);
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      cm[v[i]] = v[i];
      if (i % 1000000 == 0) std::cout << cm.load_factor() << std::endl;

    }
    std::cout << NUM_ELS / 1000000 << "M cuckoo map pow2 inserts in " << ti.current_time() << " (Load factor = " << cm.load_factor() << ")" << std::endl;

    graphlab::memory_info::print_usage();

    ti.start();
    for (size_t i = 0;i < 1000000; ++i) {
      std::string t = cm[v[i]];
      assert(t == v[i]);
    }
    std::cout << "1M cuckoo map pow2 successful probes in " << ti.current_time() << std::endl;

  }
}


void save_load_test() {
  typedef graphlab::cuckoo_map_pow2<uint32_t, uint32_t, 3, uint32_t> cuckoo_map_type;
  cuckoo_map_type map(-1);
  for(uint32_t i = 0; i < 10000; ++i) map[i] = i;
  std::ofstream fout("tmp.txt");
  graphlab::oarchive oarc(fout);
  std::string t = "The end.";
  oarc << map << t;
  fout.close();
  std::ifstream fin("tmp.txt");
  graphlab::iarchive iarc(fin);
  cuckoo_map_type map2(-1);
  std::string txt;
  iarc >> map2;
  iarc >> txt;
  ASSERT_EQ(txt, std::string("The end."));
  for(uint32_t i = 0; i < 10000; ++i) 
    ASSERT_EQ(map[i], i);
} // end of save load test



void cuckoo_set_sanity_checks() {
  boost::unordered_set<uint32_t> um;
  graphlab::cuckoo_set_pow2<uint32_t> cm(-1, 2, 2);
  ASSERT_TRUE(cm.begin() == cm.end());
  for (size_t i = 0;i < 10000; ++i) {
    cm.insert(17 * i);
    um.insert(17 * i);
  }

  for (size_t i = 0;i < 10000; ++i) {
    assert(cm.count(17 * i) == 1);
    assert(um.count(17 * i) == 1);
  }
  assert(cm.size() == 10000);
  assert(um.size() == 10000);

  for (size_t i = 0;i < 10000; i+=2) {
    cm.erase(17*i);
    um.erase(17*i);
  }
  for (size_t i = 0;i < 10000; i+=2) {
    assert(cm.count(17*i) == i % 2);
    assert(um.count(17*i) == i % 2);
  }

  assert(cm.size() == 5000);
  assert(um.size() == 5000);

  std::ofstream fout("tmp.txt");
  graphlab::oarchive oarc(fout);
  oarc << cm;
  fout.close();
  std::ifstream fin("tmp.txt");
  graphlab::iarchive iarc(fin);
  graphlab::cuckoo_set_pow2<uint32_t> set2(-1);
  iarc >> set2;
  assert(set2.size() == 5000);
}





int main(int argc, char** argv) {
  std::cout << "Basic Sanity Checks... ";
  std::cout.flush();
  sanity_checks();
  sanity_checks2();
  more_interesting_data_types_check();
  more_interesting_data_types_check2();
  save_load_test();


  cuckoo_set_sanity_checks();

  std::cout << "Done" << std::endl;


  // std::cout << "\n\n\nRunning Benchmarks. uint32-->uint32" << std::endl;
  // benchmark();


  // std::cout << "\n\n\nRunning Benchmarks. string-->string" << std::endl;
  // benchmark_strings();

}
