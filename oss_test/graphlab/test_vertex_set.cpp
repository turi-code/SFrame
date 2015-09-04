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


// #include <cxxtest/TestSuite.h>

#include <graphlab.hpp>

typedef graphlab::distributed_graph<int,int> graph_type;


bool select_out_degree_le(graph_type::vertex_type vtx, size_t ndeg) {
  return vtx.num_out_edges() <= ndeg;
}


bool select_out_degree_eq(graph_type::vertex_type vtx, size_t ndeg) {
  return vtx.num_out_edges() == ndeg;
}


bool select_vid_modulo(graph_type::vertex_type vtx, size_t divisor) {
  return (vtx.id() % divisor) == 0;
}


size_t is_divisible(graph_type::vertex_type vtx, size_t divisor) {
  return (vtx.id() % divisor) == 0;
}

size_t count_edges(graph_type::edge_type e) {
  return 1;
}

void set_to_one(graph_type::vertex_type vtx) {
  vtx.data() = 1;
}

int vertex_data_identity(graph_type::vertex_type vtx) {
  return vtx.data();
}



int main(int argc, char** argv) {

  global_logger().set_log_level(LOG_INFO);
  ///! Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;

  graph_type graph(dc);
  graph.load_synthetic_powerlaw(100000);
  graph.finalize();

  dc.cout() << graph.vertex_set_size(graph.complete_set()) << " Vertices\n";

  ASSERT_EQ(graph.vertex_set_size(graph.complete_set()), graph.num_vertices());
  // select all vertices which have <= 1 neighbors
  graphlab::vertex_set small = graph.select(boost::bind(select_out_degree_le, _1, 1));
  dc.cout() << graph.vertex_set_size(small) << " vertices with out degree <= 1\n";

  // all vertices which have  > 1 neighbors
  graphlab::vertex_set connected = graph.complete_set() - small; 
  dc.cout() << graph.vertex_set_size(connected) << " vertices with out degree > 1\n";

  // union of the two of them should give me all vertices
  graphlab::vertex_set all = small | connected;
  ASSERT_EQ(graph.vertex_set_size(all), graph.num_vertices());

  // select all vertices with an even ID
  graphlab::vertex_set even_id = graph.select(boost::bind(select_vid_modulo, _1, 2));
  // select all vertices with ID divisible by 3
  graphlab::vertex_set div_3_id = graph.select(boost::bind(select_vid_modulo, _1, 3));

  // intersect
  graphlab::vertex_set div_6_id = even_id & div_3_id;

  // count the number of IDs which are divisible by 6 
  size_t num_div_6 = graph.map_reduce_vertices<size_t>(boost::bind(is_divisible, _1, 6)); 

  ASSERT_EQ(num_div_6, 1 + (graph.num_vertices() - 1) / 6);

  // do a restricted map_reduce
  size_t num_div_6_restricted = graph.map_reduce_vertices<size_t>
                                (boost::bind(is_divisible, _1, 6), div_6_id); 

  ASSERT_EQ(num_div_6, num_div_6_restricted);

  ASSERT_EQ(graph.vertex_set_size(div_6_id), num_div_6);



  graphlab::vertex_set out_deg_one = graph.select(boost::bind(select_out_degree_eq, _1, 1));
  // test edge mapreduce
  size_t num_small_edges = graph.map_reduce_edges<size_t>(
                            count_edges, out_deg_one, graphlab::OUT_EDGES);
  // since the set only has stuff with out degree == 1... the number
  // of edges must match the size of out_deg_one

  ASSERT_EQ(num_small_edges, graph.vertex_set_size(out_deg_one));

  // test transform 
  // set vdata to 1 for the vertices with out degree 1 
  graph.transform_vertices(set_to_one, out_deg_one);
  size_t total = graph.map_reduce_vertices<size_t>(vertex_data_identity, out_deg_one);
  ASSERT_EQ(total, graph.vertex_set_size(out_deg_one)); 

  // test neighborhood selection 
  // extract the set of out neighbors of out_deg_one 
  graphlab::vertex_set out_nbrs = graph.neighbors(out_deg_one, graphlab::OUT_EDGES);

  dc.cout() << graph.vertex_set_size(out_nbrs) << " nbr size\n";
  // extract the set of in neighbors of these out neighbors
  graphlab::vertex_set out_nbrs_in_nbrs = graph.neighbors(out_nbrs, graphlab::IN_EDGES);

  dc.cout() << graph.vertex_set_size(out_nbrs_in_nbrs) << " nbr nbr size\n";
  // this set must contain the original out_deg_one set
  ASSERT_TRUE(graph.vertex_set_empty((out_deg_one & out_nbrs_in_nbrs) - out_deg_one));
  graphlab::mpi_tools::finalize();
}

