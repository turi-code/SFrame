/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <graphlab/util/hopscotch_table.hpp>
#include <graphlab/util/hopscotch_map.hpp>
#include <graphlab/util/cuckoo_map_pow2.hpp>
#include <boost/unordered_set.hpp>
#include <boost/bind.hpp>
#include <logger/assertions.hpp>
#include <parallel/pthread_tools.hpp>

#include <perf/memory_info.hpp>
#include <graphlab/macros_def.hpp>



boost::unordered_map<uint32_t, uint32_t> um2;
graphlab::hopscotch_map<uint32_t, uint32_t> cm2;

void hopscotch_map_sanity_checks() {
  const size_t NINS = 1500000;
  ASSERT_TRUE(cm2.begin() == cm2.end());
  for (size_t i = 0;i < NINS; ++i) {
    cm2[17 * i] = i;
    um2[17 * i] = i;
  }

  for (size_t i = 0;i < NINS; ++i) {
    assert(cm2[17 * i] == i);
    assert(um2[17 * i] == i);
  }
  assert(cm2.size() == NINS);
  assert(um2.size() == NINS);

  for (size_t i = 0;i < NINS; i+=2) {
    cm2.erase(17*i);
    um2.erase(17*i);
  }
  for (size_t i = 0;i < NINS; i+=2) {
    assert(cm2.count(17*i) == i % 2);
    assert(um2.count(17*i) == i % 2);
    if (cm2.count(17*i)) {
      assert(cm2.find(17*i)->second == i);
    }
  }

  assert(cm2.size() == NINS / 2);
  assert(um2.size() == NINS / 2);

  typedef graphlab::hopscotch_map<uint32_t, uint32_t>::value_type vpair;
  {
    size_t cnt = 0;
    foreach(vpair &v, cm2) {
      ASSERT_EQ(v.second, um2[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, NINS / 2);
  }
  {
    size_t cnt = 0;
    foreach(const vpair &v, cm2) {
      ASSERT_EQ(v.second, um2[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, NINS / 2);
  }

  std::stringstream strm;
  graphlab::oarchive oarc(strm);
  oarc << cm2;
  strm.flush();

  cm2.clear();
  ASSERT_EQ(cm2.size(), 0);
  graphlab::iarchive iarc(strm);
  iarc >> cm2;
  ASSERT_EQ(cm2.size(), NINS / 2);

}





struct bad_hasher {
  size_t operator()(uint32_t a) const {
    return 1;
  }
};



void hopscotch_high_collision_sanity_checks() {
  const size_t NINS = 15000;
  boost::unordered_map<uint32_t, uint32_t> um2;
  graphlab::hopscotch_map<uint32_t, uint32_t, bad_hasher> cm2;
  ASSERT_TRUE(cm2.begin() == cm2.end());
  for (size_t i = 0;i < NINS; ++i) {
    cm2[17 * i] = i;
    um2[17 * i] = i;
  }

  for (size_t i = 0;i < NINS; ++i) {
    assert(cm2[17 * i] == i);
    assert(um2[17 * i] == i);
  }
  assert(cm2.size() == NINS);
  assert(um2.size() == NINS);

  for (size_t i = 0;i < NINS; i+=2) {
    cm2.erase(17*i);
    um2.erase(17*i);
  }
  for (size_t i = 0;i < NINS; i+=2) {
    assert(cm2.count(17*i) == i % 2);
    assert(um2.count(17*i) == i % 2);
    if (cm2.count(17*i)) {
      assert(cm2.find(17*i)->second == i);
    }
  }

  assert(cm2.size() == NINS / 2);
  assert(um2.size() == NINS / 2);

  typedef graphlab::hopscotch_map<uint32_t, uint32_t>::value_type vpair;
  {
    size_t cnt = 0;
    foreach(vpair &v, cm2) {
      ASSERT_EQ(v.second, um2[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, NINS / 2);
  }
  {
    size_t cnt = 0;
    foreach(const vpair &v, cm2) {
      ASSERT_EQ(v.second, um2[v.first]);
      ++cnt;
    }
    ASSERT_EQ(cnt, NINS / 2);
  }

  std::stringstream strm;
  graphlab::oarchive oarc(strm);
  oarc << cm2;
  strm.flush();

  cm2.clear();
  ASSERT_EQ(cm2.size(), 0);
  graphlab::iarchive iarc(strm);
  iarc >> cm2;
  ASSERT_EQ(cm2.size(), NINS / 2);

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
    graphlab::cuckoo_map_pow2<uint32_t, uint32_t, 3, uint32_t> cm(-1, 128);

    //cm.reserve(102400);
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      cm[v[i]] = i;
  //    if (i % 1000000 == 0) std::cout << cm.load_factor() << std::endl;

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

{
    graphlab::hopscotch_map<uint32_t, uint32_t> cm;
    ti.start();
    for (size_t i = 0;i < NUM_ELS; ++i) {
      cm[v[i]] = i;
//      if (i % 1000000 == 0) std::cout << cm.load_factor() << std::endl;

    }
    std::cout << NUM_ELS / 1000000 << "M hopscotch inserts in " << ti.current_time() << " (Load factor = " << cm.load_factor() << ")" << std::endl;

    graphlab::memory_info::print_usage();

    ti.start();
    for (size_t i = 0;i < 10000000; ++i) {
      size_t t = cm[v[i]];
      assert(t == i);
    }
    std::cout << "10M hopscotch successful probes in " << ti.current_time() << std::endl;

  }
}



int main(int argc, char** argv) {
  std::cout << "Hopscotch Map Sanity Checks... \n";
  hopscotch_map_sanity_checks();

  std::cout << "Hopscotch High Collision Sanity Checks... \n";
  hopscotch_high_collision_sanity_checks();

  std::cout << "Map Benchmarks... \n";
  benchmark();
  std::cout << "Done" << std::endl;
}
