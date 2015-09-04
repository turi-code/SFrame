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


#include <graphlab.hpp>

#include <graphlab/macros_def.hpp>

#define RING_SIZE 200
#define NUM_ITERATIONS 1000

typedef graphlab::graph<size_t, size_t> graph_type;


class increment_update : public
                  graphlab::iupdate_functor<graph_type, increment_update> {
 public:
  void operator()(icontext_type& context) {
    ++context.vertex_data();
    foreach(edge_type edge, context.out_edges()) {
      const vertex_id_type nbr_id = edge.target();
      if (context.const_vertex_data(nbr_id) < NUM_ITERATIONS) {
        context.schedule(nbr_id, *this);    
      }
    }
  }
}; // end of shortest path update functor

void make_graph(graph_type &graph) {
  for (size_t i = 0;i < RING_SIZE; ++i) {
    graph.add_vertex(0);
  }
  
  for (size_t i = 0;i < RING_SIZE; ++i) {
    graph.add_edge(i, (i+1) % RING_SIZE, 0);
  }
}


class EngineTerminatorTestSuite: public CxxTest::TestSuite {
 public:  
  void test_engine_terminator() {
    // Create a graphlab core
    graphlab::core<graph_type, increment_update> core;
    make_graph(core.graph());
    core.graph().finalize();
    for (size_t ncpus = 1; ncpus <= 8; ++ncpus) {
      core.set_ncpus(ncpus);
      core.set_scope_type("edge");
      core.schedule(0, increment_update());
      const double runtime = core.start();
      std::cout << ncpus << " Procs: " << runtime << std::endl;
      // check the graph and reset it
      for (size_t i = 0;i < RING_SIZE; ++i) {
        if (core.graph().vertex_data(i) != (size_t)NUM_ITERATIONS) {
          std::cout << "vertex " << i << " ";
          TS_ASSERT_EQUALS(core.graph().vertex_data(i), (size_t)NUM_ITERATIONS);
        }
        core.graph().vertex_data(i) = 0;
      }
    }
  }
};



