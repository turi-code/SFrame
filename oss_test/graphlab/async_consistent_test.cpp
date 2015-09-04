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

#include <vector>
#include <algorithm>
#include <iostream>


// #include <cxxtest/TestSuite.h>

#include <graphlab.hpp>

typedef graphlab::distributed_graph<int,int> graph_type;


class count_in_neighbors :
  public graphlab::ivertex_program<graph_type, int>,
  public graphlab::IS_POD_TYPE {
public:
  edge_dir_type
  gather_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::IN_EDGES;
  }
  gather_type
  gather(icontext_type& context, const vertex_type& vertex,
         edge_type& edge) const {
    return 1;
  }
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    ASSERT_EQ( total, int(vertex.num_in_edges()) );
  }
  edge_dir_type
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors


void test_in_neighbors(graphlab::distributed_control& dc,
                       graphlab::command_line_options& clopts,
                       graph_type& graph) {
  std::cout << "Constructing an engine for in neighbors" << std::endl;
  typedef graphlab::async_consistent_engine<count_in_neighbors> engine_type;
  engine_type engine(dc, graph, clopts);
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all();
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;
}


class count_out_neighbors :
  public graphlab::ivertex_program<graph_type, int>,
  public graphlab::IS_POD_TYPE {
public:
  edge_dir_type
  gather_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::OUT_EDGES;
  }
  gather_type
  gather(icontext_type& context, const vertex_type& vertex,
         edge_type& edge) const {
    return 1;
  }
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    ASSERT_EQ( total, int(vertex.num_out_edges()) );
  }
  edge_dir_type
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors

void test_out_neighbors(graphlab::distributed_control& dc,
                        graphlab::command_line_options& clopts,
                        graph_type& graph) {
  std::cout << "Constructing an engine for out neighbors" << std::endl;
  typedef graphlab::async_consistent_engine<count_out_neighbors> engine_type;
  engine_type engine(dc, graph, clopts);
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all();
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;
}


class count_all_neighbors :
  public graphlab::ivertex_program<graph_type, int, int>,
  public graphlab::IS_POD_TYPE {
public:
  void init(icontext_type& context, const vertex_type& vertex,
                    const message_type& msg) {
    ASSERT_EQ(msg, 100);
  }
  
  edge_dir_type
  gather_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::ALL_EDGES;
  }
  gather_type
  gather(icontext_type& context, const vertex_type& vertex,
         edge_type& edge) const {
    return 1;
  }
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    ASSERT_EQ( total, int(vertex.num_in_edges() + vertex.num_out_edges() ) );
  }
  edge_dir_type
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors

void test_all_neighbors(graphlab::distributed_control& dc,
                        graphlab::command_line_options& clopts,
                        graph_type& graph) {
  std::cout << "Constructing an engine for all neighbors" << std::endl;
  typedef graphlab::async_consistent_engine<count_all_neighbors> engine_type;
  engine_type engine(dc, graph, clopts);
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all(100);
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;
}











// Make a slow version so that the asynchronous aggregators get a change
// to run. Basically, sleep a bit on apply.
class count_all_neighbors_slow :
  public graphlab::ivertex_program<graph_type, int, int>,
  public graphlab::IS_POD_TYPE {
public:
  void init(icontext_type& context, const vertex_type& vertex,
                    const message_type& msg) {
    ASSERT_EQ(msg, 100);
  }

  edge_dir_type
  gather_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::ALL_EDGES;
  }
  gather_type
  gather(icontext_type& context, const vertex_type& vertex,
         edge_type& edge) const {
    return 1;
  }
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    graphlab::timer::sleep_ms(100);
    ASSERT_EQ( total, int(vertex.num_in_edges() + vertex.num_out_edges() ) );
  }
  edge_dir_type
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors






typedef graphlab::async_consistent_engine<count_all_neighbors_slow> agg_engine_type;

size_t agg_map(agg_engine_type::icontext_type& context,
              const agg_engine_type::vertex_type& vtx) {
  return 1;
}

void agg_finalize(agg_engine_type::icontext_type& context,
                  size_t result) {
  std::cout << "Aggregator: #vertices = " << result << std::endl;
}


size_t agg_edge_map(agg_engine_type::icontext_type& context,
              const agg_engine_type::edge_type& vtx) {
  return 1;
}

void agg_edge_finalize(agg_engine_type::icontext_type& context,
                  size_t result) {
  std::cout << "Aggregator: #edges= " << result << std::endl;
}



size_t identity_vertex_map(agg_engine_type::vertex_type vtx) {
  return vtx.data();
}
size_t identity_edge_map(agg_engine_type::edge_type e) {
  return e.data();
}


size_t identity_vertex_map_context(agg_engine_type::icontext_type& context,
                                   agg_engine_type::vertex_type vtx) {
  return vtx.data();
}
size_t identity_edge_map_context(agg_engine_type::icontext_type& context,
                                 agg_engine_type::edge_type e) {
  return e.data();
}



void  set_vertex_to_one(agg_engine_type::vertex_type vtx) {
  vtx.data() = 1;
}
void  set_edge_to_one(agg_engine_type::edge_type e) {
  e.data() = 1;
}


void  vertex_plus_one(agg_engine_type::vertex_type vtx) {
  ++vtx.data();
}


void vertex_minus_one_context(agg_engine_type::icontext_type& context,
                                agg_engine_type::vertex_type vtx) {
  --vtx.data();
}

void edge_plus_one(agg_engine_type::edge_type e) {
  ++e.data();
}


void edge_minus_one_context(agg_engine_type::icontext_type& context,
                              agg_engine_type::edge_type e) {
  --e.data();
}

void test_aggregator(graphlab::distributed_control& dc,
                     graphlab::command_line_options& clopts,
                     graph_type& graph) {
  std::cout << "Constructing an engine for all neighbors" << std::endl;
  agg_engine_type engine(dc, graph, clopts);
  engine.add_vertex_aggregator<size_t>("num_vertices_counter", agg_map, agg_finalize);
  engine.add_edge_aggregator<size_t>("num_edges_counter", agg_edge_map, agg_edge_finalize);
  // reset all
  graph.transform_vertices(set_vertex_to_one);
  graph.transform_edges(set_edge_to_one);
  
  ASSERT_EQ(graph.map_reduce_vertices<size_t>(identity_vertex_map), graph.num_vertices());
  graph.transform_vertices(vertex_plus_one);
  ASSERT_EQ(graph.map_reduce_vertices<size_t>(identity_vertex_map), 2 * graph.num_vertices());
  engine.transform_vertices(vertex_minus_one_context);
  ASSERT_EQ(graph.map_reduce_vertices<size_t>(identity_vertex_map), graph.num_vertices());
  ASSERT_EQ(engine.map_reduce_vertices<size_t>(identity_vertex_map_context), graph.num_vertices());
  
  ASSERT_EQ(graph.map_reduce_edges<size_t>(identity_edge_map), graph.num_edges());
  graph.transform_edges(edge_plus_one);
  ASSERT_EQ(graph.map_reduce_edges<size_t>(identity_edge_map), 2 * graph.num_edges());
  engine.transform_edges(edge_minus_one_context);
  ASSERT_EQ(graph.map_reduce_edges<size_t>(identity_edge_map), graph.num_edges());
  ASSERT_EQ(engine.map_reduce_edges<size_t>(identity_edge_map_context), graph.num_edges());
  
  ASSERT_TRUE(engine.aggregate_now("num_vertices_counter"));
  ASSERT_TRUE(engine.aggregate_now("num_edges_counter"));
  ASSERT_TRUE(engine.aggregate_periodic("num_vertices_counter", 0.2));
  ASSERT_TRUE(engine.aggregate_periodic("num_edges_counter", 0.2));
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all(100);
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;
}









int main(int argc, char** argv) {

  global_logger().set_log_level(LOG_INFO);
  ///! Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::dc_init_param rpc_parameters;
  graphlab::init_param_from_mpi(rpc_parameters);
  graphlab::distributed_control dc(rpc_parameters);

  graphlab::command_line_options clopts("Test code.");
  clopts.set_scheduler_type("queued_fifo");
  std::cout << "Creating a powerlaw graph" << std::endl;
  graph_type graph(dc, clopts);
  graph.load_synthetic_powerlaw(100);

  test_in_neighbors(dc, clopts, graph);
  test_out_neighbors(dc, clopts, graph);
  test_all_neighbors(dc, clopts, graph);
  test_aggregator(dc, clopts, graph);
  graphlab::mpi_tools::finalize();
} // end of main





