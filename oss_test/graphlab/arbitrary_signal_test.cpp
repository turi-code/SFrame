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


class test_uf:
  public graphlab::ivertex_program<graph_type, int>,
  public graphlab::IS_POD_TYPE {
public:
  edge_dir_type
  gather_edges(icontext_type& context, 
               const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
    if (vertex.id() < 99) context.signal_vid(vertex.id() + 1);
  }
  edge_dir_type scatter_edges(icontext_type& context, 
                              const vertex_type& vertex) const {
    return graphlab::NO_EDGES;
  }
}; // end of count neighbors


typedef graphlab::async_consistent_engine<test_uf> agg_engine_type;
//typedef graphlab::synchronous_engine<test_uf> agg_engine_type;

int main(int argc, char** argv) {
  global_logger().set_log_level(LOG_WARNING);
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


  typedef agg_engine_type engine_type;
  engine_type engine(dc, graph, clopts);
  engine.signal(0);
  engine.start();

  ASSERT_EQ(engine.num_updates(), 100);
  graphlab::mpi_tools::finalize();
} // end of main





