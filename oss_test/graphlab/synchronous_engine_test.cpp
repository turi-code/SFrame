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
    // if (edge.target().id() == 7)
    //   std::cout << edge.source().id() << "\t" << edge.target().id() << std::endl;
    return 1;    
  }
  void apply(icontext_type& context, vertex_type& vertex, 
             const gather_type& total) {
    // if (total != int(vertex.num_in_edges())) {
    //   std::cout << "test fail vid : " << vertex.id() << std::endl;
    // }
    ASSERT_EQ( total, int(vertex.num_in_edges()) );
    context.signal(vertex);
  }
  edge_dir_type
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors


void test_in_neighbors(graphlab::distributed_control& dc,
                       graphlab::command_line_options& clopts,
                       graph_type& graph) {
  std::cout << "Constructing a syncrhonous engine for in neighbors" << std::endl;
  typedef graphlab::synchronous_engine<count_in_neighbors> engine_type;
  engine_type engine(dc, graph, clopts);
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all();

  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;

  if (graph.is_dynamic()) {
    std::cout << "Test engine on dynamic graph !" << std::endl;
    graph.load_synthetic_powerlaw(10000);
    graph.finalize();
    engine.signal_all();
    std::cout << "Running!" << std::endl;
    engine.start();
    std::cout << "Finished" << std::endl;
  }
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
    context.signal(vertex);
  }
  edge_dir_type 
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors

void test_out_neighbors(graphlab::distributed_control& dc,
                        graphlab::command_line_options& clopts,
                        graph_type& graph) {
  std::cout << "Constructing a syncrhonous engine for out neighbors" << std::endl;
  typedef graphlab::synchronous_engine<count_out_neighbors> engine_type;
  engine_type engine(dc, graph, clopts);
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all();
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;

  if (graph.is_dynamic()) {
    std::cout << "Test engine on dynamic graph !" << std::endl;
    graph.load_synthetic_powerlaw(10000);
    graph.finalize();
    engine.signal_all();
    std::cout << "Running!" << std::endl;
    engine.start();
    std::cout << "Finished" << std::endl;
  }
}


class count_all_neighbors : 
  public graphlab::ivertex_program<graph_type, int>,
  public graphlab::IS_POD_TYPE {
public:
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
    context.signal(vertex);
  }
  edge_dir_type 
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors

void test_all_neighbors(graphlab::distributed_control& dc,
                        graphlab::command_line_options& clopts,
                        graph_type& graph) {
  std::cout << "Constructing a syncrhonous engine for all neighbors" << std::endl;
  typedef graphlab::synchronous_engine<count_all_neighbors> engine_type;
  engine_type engine(dc, graph, clopts);
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all();
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;
  if (graph.is_dynamic()) {
    std::cout << "Test engine on dynamic graph !" << std::endl;
    graph.load_synthetic_powerlaw(10000);
    graph.finalize();
    engine.signal_all();
    std::cout << "Running!" << std::endl;
    engine.start();
    std::cout << "Finished" << std::endl;
  }
}




class basic_messages : 
  public graphlab::ivertex_program<graph_type, int, int>,
  public graphlab::IS_POD_TYPE {
  int message_value;
public:

  void init(icontext_type& context, const vertex_type& vertex,
                    const message_type& msg) {
    message_value = msg;
  } 

  edge_dir_type 
  gather_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::IN_EDGES;
  }
 
  gather_type gather(icontext_type& context, const vertex_type& vertex, 
         edge_type& edge) const {
    return 1;
  }

  void apply(icontext_type& context, vertex_type& vertex, 
             const gather_type& total) {
    context.signal(vertex, 0);
    if(message_value < 0) {
      // first iteration has wrong messages
      return;
    }
    ASSERT_EQ(total, message_value);

  }

  edge_dir_type 
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::OUT_EDGES;
  }

  void scatter(icontext_type& context, const vertex_type& vertex, 
               edge_type& edge) const {
    context.signal(edge.target(), 1);
  }

}; // end of test_messages

void test_messages(graphlab::distributed_control& dc,
                   graphlab::command_line_options& clopts,
                   graph_type& graph) {
  std::cout << "Testing messages" << std::endl;
  typedef graphlab::synchronous_engine<basic_messages> engine_type;
  engine_type engine(dc, graph, clopts);
  std::cout << "Scheduling all vertices to test messages" << std::endl;
  engine.signal_all(-1);
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;
  if (graph.is_dynamic()) {
    engine.init();
    std::cout << "Test engine on dynamic graph !" << std::endl;
    graph.load_synthetic_powerlaw(10000);
    graph.finalize();
    engine.signal_all(-1);
    std::cout << "Running!" << std::endl;
    engine.start();
    std::cout << "Finished" << std::endl;
  }
}







class count_aggregators : 
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
    ASSERT_LT(vertex.data(), 100);
    ASSERT_GE(vertex.data(), 0);
    return vertex.data();
  }
  void apply(icontext_type& context, vertex_type& vertex, 
             const gather_type& total) {
    ASSERT_EQ( total, context.iteration() * vertex.num_in_edges() );
    vertex.data() = context.iteration() + 1; 
    if(context.iteration() < 10) context.signal(vertex);
  }
  edge_dir_type 
  scatter_edges(icontext_type& context, const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count aggregators

int iteration_counter(count_aggregators::icontext_type& context,
                      const graph_type::vertex_type& vertex) {
  ASSERT_LT(vertex.data(), 100);
  return vertex.data();
}
int finalize_iter = 0;
void iteration_finalize(count_aggregators::icontext_type& context,
                        const int& total) {
  std::cout << "Finalized" << std::endl;
  ASSERT_EQ(total, context.num_vertices() * (context.iteration()+1));
  ASSERT_EQ(finalize_iter++, context.iteration());
}


void test_count_aggregators(graphlab::distributed_control& dc,
                            graphlab::command_line_options& clopts,
                            graph_type& graph) {
  std::cout << "Constructing a syncrhonous engine for aggregators" << std::endl;
  typedef graphlab::synchronous_engine<count_aggregators> engine_type;
  engine_type engine(dc, graph, clopts);
  engine.add_vertex_aggregator<int>("iteration_counter", 
                                    iteration_counter, iteration_finalize);
  engine.aggregate_periodic("iteration_counter", 0);
  std::cout << "Scheduling all vertices to count their neighbors" << std::endl;
  engine.signal_all();
  std::cout << "Running!" << std::endl;
  engine.start();
  std::cout << "Finished" << std::endl;
  ASSERT_EQ(finalize_iter, engine.iteration());
}




int main(int argc, char** argv) {
  ///! Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::dc_init_param rpc_parameters;
  graphlab::init_param_from_mpi(rpc_parameters);
  graphlab::distributed_control dc(rpc_parameters);

  graphlab::command_line_options clopts("Test code.");
  clopts.engine_args.set_option("max_iterations", 10);
  std::cout << "Creating a powerlaw graph" << std::endl;
  graph_type graph(dc, clopts);
  graph.load_synthetic_powerlaw(10000);
  graph.finalize();
  test_in_neighbors(dc, clopts, graph);
  test_out_neighbors(dc, clopts, graph);
  test_all_neighbors(dc, clopts, graph);
  test_messages(dc, clopts, graph);
  test_count_aggregators(dc, clopts, graph);

  graphlab::mpi_tools::finalize();
} // end of main





