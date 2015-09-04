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


#include <queue>
#include <graph/graph/graph.hpp>
#include <graphlab/util/chandy_misra.hpp>
#include <util/dense_bitset.hpp>
#include <graphlab/macros_def.hpp>
using namespace graphlab;

typedef graph<int, int> graph_type;



class ChandyMisraTest: public CxxTest::TestSuite {
 public:
  void test_cm() {
    graph_type g;
    for (size_t i = 0;i < 25; ++i) g.add_vertex(0);
    for (size_t i = 0;i < 25; ++i) {
      for (size_t j = 0;j < 25; ++j) {
        if ((i != j) && (rand() % 1000 <= 100)) {
          ASSERT_NE(i, j);
          g.add_edge(i, j, 0);
        }
      }
    }
    g.finalize();
    chandy_misra<graph_type> cm(g);
    for (size_t i = 0;i < 100; ++i) {
      TS_ASSERT_EQUALS(cm.make_philosopher_hungry(i % 25), i % 25);
      std::vector<vertex_id_type> r = cm.philosopher_stops_eating(i % 25);
      TS_ASSERT_EQUALS(r.size(), size_t(0));
    }
    // test more aggressive
    for (size_t k = 0;k < 10; ++k) {
      dense_bitset locked, ready, complete;
      locked.resize(25); ready.resize(25);
      locked.clear(); ready.clear();
      complete.resize(25); complete.clear();

      for (size_t i = 0;i < 25; ++i) {
        locked.set_bit(i);
        vertex_id_type ret = cm.make_philosopher_hungry(i);
        if (ret != (vertex_id_type)(-1)) {
          complete.set_bit(ret);
          ready.set_bit(ret);
        }
      }
      cm.complete_consistency_check();
      
      while(1) {
        if (ready.popcount() == 0 && complete.popcount() == g.num_vertices()) break;
        foreach(size_t i, ready) {
          ready.clear_bit(i);
          std::vector<vertex_id_type> r = cm.philosopher_stops_eating(i);
          cm.complete_consistency_check();
          
          foreach(vertex_id_type j, r) {
            TS_ASSERT(locked.get(j));
            complete.set_bit(j);
            ready.set_bit(j);
          }
        }
      }
      cm.no_locks_consistency_check();
      cm.complete_consistency_check();
    }
    cm.no_locks_consistency_check();
    cm.complete_consistency_check();
    {
      // test very aggressive
      std::vector<vertex_id_type> ctr(25, 10);
      size_t n = 25 * 10;
      std::queue<size_t> ready;
      for (size_t i = 0;i < 25; ++i) {
        vertex_id_type ret = cm.make_philosopher_hungry(i);
        if (ret != vertex_id_type(-1)) ready.push(ret);
      }
      while(!ready.empty()) {
        size_t i = ready.front(); ready.pop();
        TS_ASSERT(ctr[i] > 0);
        ctr[i]--;
        n--;
        std::vector<vertex_id_type> r = cm.philosopher_stops_eating(i);
        foreach(vertex_id_type v, r) ready.push(v);
        if (ctr[i] > 0) {
          vertex_id_type ret = cm.make_philosopher_hungry(i);
          if (ret != vertex_id_type(-1)) ready.push(ret);
        }
        cm.complete_consistency_check();
      }
      TS_ASSERT_EQUALS(n, size_t(0));
    }
  }

  void test_parallel() {
    
  }
};
