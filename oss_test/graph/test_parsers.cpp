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


#include <graph/distributed_graph.hpp>
#include <graphlab/macros_def.hpp>

typedef graphlab::distributed_graph<size_t, size_t> graph_type;

void check_structure(graph_type &graph) {
  ASSERT_EQ(graph.num_vertices(), 5);
  ASSERT_EQ(graph.num_edges(), 7);
  // check vertex 0 
  {
    graph_type::vertex_type vtype = graph.vertex(0);
    graph_type::local_edge_list_type v0_out = graph_type::local_vertex_type(vtype).out_edges();
    ASSERT_EQ(v0_out.size(), 1);
    ASSERT_EQ(v0_out[0].target().global_id(), 5);
  }
  // vertex 1
  {
    graph_type::vertex_type vtype = graph.vertex(1);
    graph_type::local_edge_list_type v0_out = graph_type::local_vertex_type(vtype).out_edges();
    ASSERT_EQ(v0_out.size(), 2);
    ASSERT_EQ(v0_out[0].target().global_id(), 0);
    ASSERT_EQ(v0_out[1].target().global_id(), 5);
  }
  
  // vertex 2
  {
    graph_type::vertex_type vtype = graph.vertex(2);
    graph_type::local_edge_list_type v0_out = graph_type::local_vertex_type(vtype).out_edges();
    ASSERT_EQ(v0_out.size(), 2);
    ASSERT_EQ(v0_out[0].target().global_id(), 0);
    ASSERT_EQ(v0_out[1].target().global_id(), 5);
  }
  // vertex 3
  {
    graph_type::vertex_type vtype = graph.vertex(3);
    graph_type::local_edge_list_type v0_out = graph_type::local_vertex_type(vtype).out_edges();
    ASSERT_EQ(v0_out.size(), 2);
    ASSERT_EQ(v0_out[0].target().global_id(), 0);
    ASSERT_EQ(v0_out[1].target().global_id(), 5);
  }
}



void test_adj(graphlab::distributed_control& dc) {
  graphlab::distributed_graph<size_t, size_t> graph(dc);
  graph.load_format("data/test_adj", "adj");
  graph.finalize();
  check_structure(graph);  
}

void test_snap(graphlab::distributed_control& dc) {
  graphlab::distributed_graph<size_t, size_t> graph(dc);
  graph.load_format("data/test_snap", "snap");
  graph.finalize();
  check_structure(graph);  
}

void test_tsv(graphlab::distributed_control& dc) {
  graphlab::distributed_graph<size_t, size_t> graph(dc);
  graph.load_format("data/test_tsv", "tsv");
  graph.finalize();
  check_structure(graph);  
}

void test_powerlaw(graphlab::distributed_control& dc) {
  graphlab::distributed_graph<size_t, size_t> graph(dc);
  graph.load_synthetic_powerlaw(1000);
  graph.finalize();
  ASSERT_EQ(graph.num_vertices(), 1000);
  std::cout << graph.num_edges() << " Edges\n";
}


void test_save_load(graphlab::distributed_control& dc) {
  graphlab::distributed_graph<size_t, size_t> graph(dc);
  graph.load_synthetic_powerlaw(1000);
  graph.finalize();
  ASSERT_EQ(graph.num_vertices(), 1000);
  graph.save_format("data/plawtest_tsv", "tsv");
  graph.save_format("data/plawtest_jrl", "graphjrl");
  // load it back
  graphlab::distributed_graph<size_t, size_t> graph2(dc);
  graph2.load_format("data/plawtest_tsv", "tsv");
  graph2.finalize();
  ASSERT_EQ(graph.num_vertices(), graph2.num_vertices());
  ASSERT_EQ(graph.num_edges(), graph2.num_edges());

  graphlab::distributed_graph<size_t, size_t> graph3(dc);
  graph3.load_format("data/plawtest_jrl", "graphjrl");
  graph3.finalize();
  ASSERT_EQ(graph.num_vertices(), graph3.num_vertices());
  ASSERT_EQ(graph.num_edges(), graph3.num_edges());

}


int main(int argc, char** argv) {
  graphlab::distributed_control dc;
  test_adj(dc);
  test_snap(dc);
  test_tsv(dc);
  test_powerlaw(dc);
  test_save_load(dc);
};

