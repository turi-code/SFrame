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
#include <string>
#include <boost/unordered_set.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_init_from_mpi.hpp>
#include <graph/graph_ops.hpp>
#include <graph/distributed_graph.hpp>
#include <graphlab/options/command_line_options.hpp>
#include <graphlab/util/fs_util.hpp>
#include <graphlab/engine/distributed_chandy_misra.hpp>


#include <graphlab/macros_def.hpp>

#define INITIAL_NLOCKS_TO_ACQUIRE 1000
graphlab::mutex mt;
graphlab::conditional cond;
std::vector<graphlab::vertex_id_type> lockable_vertices;
boost::unordered_map<graphlab::vertex_id_type, size_t> demand_set;
boost::unordered_map<graphlab::vertex_id_type, size_t> current_demand_set;
boost::unordered_map<graphlab::vertex_id_type, size_t> locked_set;
size_t nlocksacquired ;

size_t nlocks_to_acquire;

struct vertex_data {
  uint32_t nupdates;
  double value, old_value;
  vertex_data(double value = 1) :
    nupdates(0), value(value), old_value(0) { }
}; // End of vertex data
SERIALIZABLE_POD(vertex_data);
std::ostream& operator<<(std::ostream& out, const vertex_data& vdata) {
  return out << "Rank=" << vdata.value;
}

struct edge_data { }; // End of edge data
SERIALIZABLE_POD(edge_data);

typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;


graphlab::distributed_chandy_misra<graph_type> *locks;
graph_type *ggraph;
graphlab::blocking_queue<graphlab::vertex_id_type> locked_elements;


void callback(graphlab::vertex_id_type v) {
  //logstream(LOG_INFO) << "Locked " << ggraph->global_vid(v) << std::endl;
  mt.lock();
  ASSERT_EQ(current_demand_set[v], 1);
  locked_set[v]++;
  nlocksacquired++;
  mt.unlock();
//  graphlab::my_sleep(1);
  locked_elements.enqueue(v);
}


void thread_stuff() {
  std::pair<graphlab::vertex_id_type, bool> deq;
  while(1) {
    deq = locked_elements.dequeue();
    if (deq.second == false) break;
    else {
      locks->philosopher_stops_eating(deq.first);
      mt.lock();
      current_demand_set[deq.first] = 0;
      bool getnextlock = nlocks_to_acquire > 0;
      if (nlocks_to_acquire > 0) {
        nlocks_to_acquire--;
        if (nlocks_to_acquire % 100 == 0) {
          std::cout << "Remaining: " << nlocks_to_acquire << std::endl;
        }
      }
      if (nlocks_to_acquire == 0 &&
        nlocksacquired == INITIAL_NLOCKS_TO_ACQUIRE + lockable_vertices.size()) cond.signal();
      mt.unlock();

      if (getnextlock > 0) {
        graphlab::vertex_id_type toacquire = 0;
        while(1) {
          mt.lock();
           toacquire = lockable_vertices[graphlab::random::rand() %
                                         lockable_vertices.size()];
          if (current_demand_set[toacquire] == 0) {
            current_demand_set[toacquire] = 1;
            demand_set[toacquire]++;
            mt.unlock();
            break;
          }
          mt.unlock();
        }
        locks->make_philosopher_hungry(toacquire);
      }
    }
  }
}


int main(int argc, char** argv) {
//   global_logger().set_log_level(LOG_INFO);
//   global_logger().set_log_to_console(true);

  ///! Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::dc_init_param rpc_parameters;
  graphlab::init_param_from_mpi(rpc_parameters);
  graphlab::distributed_control dc(rpc_parameters);


  // Parse command line options -----------------------------------------------
  graphlab::command_line_options clopts("distributed chandy misra test.");
  std::string format = "adj";
  std::string graph_dir = "";
  clopts.attach_option("graph", graph_dir,
                       "The graph file.  If none is provided "
                       "then a toy graph will be created");
  clopts.add_positional("graph");
  clopts.attach_option("format",format,
                       "The graph file format: {metis, snap, tsv, adj, bin}");
  size_t ring = 0;
  clopts.attach_option("ring", ring,
                       "The size of the ring. "
                       "If ring=0 then the graph file is used.");
  size_t randomconnect = 0;
  clopts.attach_option("randomconnect", randomconnect,
                       "The size of a randomly connected network. "
                       "If randomconnect=0 then the graph file is used.");

  if(!clopts.parse(argc, argv)) {
    std::cout << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << dc.procid() << ": Starting." << std::endl;
  graphlab::timer timer; timer.start();
  graph_type graph(dc, clopts);
  ggraph = &graph;
  if(ring > 0) {
    if(dc.procid() == 0) {
      for(size_t i = 0; i < ring; ++i) graph.add_edge(i, i + 1);
      graph.add_edge(ring, 0);
    }
  } else if(randomconnect > 0) {
    if(dc.procid() == 0) {
      for(size_t i = 0; i < randomconnect; ++i) {
        std::vector<bool> v(randomconnect, false);
        v[i] = true;
        for (size_t r = 0; r < randomconnect /2 ; ++r) {
          size_t t = graphlab::random::rand() % randomconnect;
          if (v[t] == false && t > i) {
            graph.add_edge(i, t);
            //            std::cout << i << "->" << t << "\n";
            v[t] = true;
          }
        }
      }
    }
  } else {
    std::vector<std::string> graph_files;
    graphlab::fs_util::list_files_with_prefix(graph_dir, "", graph_files);
    for(size_t i = 0; i < graph_files.size(); ++i) {
      if (i % dc.numprocs() == dc.procid()) {
        const std::string graph_fname = graph_dir + graph_files[i];
        std::cout << "Loading graph from structure file: " << graph_fname
                  << std::endl;
        graph.load_format(graph_fname, format);
      }
    }
  }
  std::cout << dc.procid() << ": Enter Finalize" << std::endl;
  graph.finalize();
  
  boost::unordered_set<size_t> eidset1;
  boost::unordered_set<size_t> eidset2;
  typedef graph_type::local_edge_type  local_edge_type;
  typedef graph_type::local_edge_list_type local_edge_list_type;
 
  for (size_t v = 0; v < graph.num_local_vertices(); ++v) {
    const local_edge_list_type& in_edges = graph.l_in_edges(v);
    foreach(const local_edge_type& edge, in_edges) {
      size_t edgeid = edge.id();
      ASSERT_TRUE(eidset1.find(edgeid) == eidset1.end());
      eidset1.insert(edgeid);
    }
    const local_edge_list_type& out_edges = graph.l_out_edges(v);
    foreach(const local_edge_type& edge, out_edges) {
      size_t edgeid = edge.id();
      ASSERT_TRUE(eidset1.find(edgeid) == eidset1.end());
      ASSERT_TRUE(eidset2.find(edgeid) == eidset2.end());
      eidset2.insert(edgeid);
    }
  }
  ASSERT_EQ(eidset1.size(), eidset2.size());
  eidset1.clear(); eidset2.clear();
  
  std::cout << " ==============================================================="
            << std::endl;
  std::cout << dc.procid() << ": Finished in " << timer.current_time() << std::endl;

  std::cout
    << "========== Graph statistics on proc " << dc.procid()
    << " ==============="
    << "\n Num vertices: " << graph.num_vertices()
    << "\n Num edges: " << graph.num_edges()
    << "\n Num replica: " << graph.num_replicas()
    << "\n Replica to vertex ratio: "
    << (float)graph.num_replicas()/graph.num_vertices()
    << "\n --------------------------------------------"
    << "\n Num local own vertices: " << graph.num_local_own_vertices()
    << "\n Num local vertices: " << graph.num_local_vertices()
    << "\n Replica to own ratio: "
    << (float)graph.num_local_vertices()/graph.num_local_own_vertices()
    << "\n Num local edges: " << graph.num_local_edges()
    << "\n Edge balance ratio: " << (float)graph.num_local_edges()/graph.num_edges()
    << std::endl;

  // for (graphlab::vertex_id_type v = 0; v < graph.num_local_vertices(); ++v) {
  //   std::cout << graph.l_get_vertex_record(v).gvid << ": " << graph.l_get_vertex_record(v).owner << ":";
  //   foreach(graphlab::procid_t pid,  graph.l_get_vertex_record(v).get_replicas()) {
  //     std::cout << pid << " ";
  //   }
  //   std::cout << "\n";
  // }
  dc.barrier();
  locks = new graphlab::distributed_chandy_misra<graph_type>(dc, graph, callback);
  nlocksacquired = 0;
  nlocks_to_acquire = INITIAL_NLOCKS_TO_ACQUIRE;
  dc.full_barrier();
  for (graphlab::vertex_id_type v = 0; v < graph.num_local_vertices(); ++v) {
    if (graph.l_get_vertex_record(v).owner == dc.procid()) {
      demand_set[v] = 1;
      current_demand_set[v] = 1;
      lockable_vertices.push_back(v);
    }
  }
  dc.full_barrier();
  graphlab::thread_group thrs;
  for (size_t i = 0;i < 10; ++i) {
    thrs.launch(thread_stuff);
  }
  for (graphlab::vertex_id_type v = 0; v < graph.num_local_vertices(); ++v) {
    if (graph.l_get_vertex_record(v).owner == dc.procid()) {
      //std::cout << dc.procid() << ": Lock Req for " << graph.l_get_vertex_record(v).gvid << std::endl;
      locks->make_philosopher_hungry(v);
    }
  }
  mt.lock();
  while (nlocksacquired != INITIAL_NLOCKS_TO_ACQUIRE + lockable_vertices.size()) cond.wait(mt);
  mt.unlock();
  dc.barrier();
  locked_elements.stop_blocking();
  thrs.join();
  std::cout << INITIAL_NLOCKS_TO_ACQUIRE + lockable_vertices.size() << " Locks to acquire\n";
  std::cout << nlocksacquired << " Locks Acquired in total\n";
  boost::unordered_map<graphlab::vertex_id_type, size_t>::const_iterator iter = demand_set.begin();
  bool bad = (nlocksacquired != INITIAL_NLOCKS_TO_ACQUIRE + lockable_vertices.size());
  while (iter != demand_set.end()) {
    if(locked_set[iter->first] != iter->second) {
      std::cout << graph.l_get_vertex_record(iter->first).gvid << " mismatch: "
                << locked_set[iter->first] << ", " << iter->second << "\n";
      bad = true;
    }
    ++iter;
  }
  if (bad) {
    locks->print_out();
  }
  dc.barrier();
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
} // End of main


