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


// standard C++ headers
#include <iostream>
#include <rpc/dc.hpp>
#include <rpc/dc_init_from_mpi.hpp>
#include <graph/distributed_graph.hpp>
// #include <google/malloc_extension.h>
#include <rpc/mpi_tools.hpp>
#include <graphlab/options/command_line_options.hpp>
#include <graphlab/macros_def.hpp>

typedef char vertex_data;
typedef std::string edge_data;

typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;
typedef graph_type::vertex_record vertex_record; 

int main(int argc, char** argv) {
  ///! Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO); 
  graphlab::command_line_options clopts("Distributed graph load test.");
  std::string graphpath; 
  bool gzip = false;
  std::string prefix = ""; 
  std::string format = "adj";

  clopts.attach_option("graph", graphpath,
                       "The graph path \n");

  clopts.attach_option("prefix", prefix,
                       "The prefix for load/save binary file\n");

  clopts.attach_option("gzip", gzip,
                       "The input is in gzip format\n");

  clopts.attach_option("format", format,
                       "format of the graph: {adj, snap}\n");

  
  if(!clopts.parse(argc, argv)) {
    logstream(LOG_FATAL) << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }

  graphlab::timer mytimer; mytimer.start();
  // global_logger().set_log_to_console(true);

  graph_type graph(dc, clopts);
  
  graph.load_format(graphpath, format);

  // size_t heap_size_load;
  // size_t allocate_size_load;
  // MallocExtension::instance()->GetNumericProperty("generic.heap_size", &heap_size_load);
  // MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &allocate_size_load);

  // if (dc.procid() == 0) {
  //   std::cout << "==========================================" << std::endl;
  //   std::cout << "Heap Size (before finalize): " << (double)heap_size_load/(1024*1024) << "MB" << "\n";
  //   std::cout << "Allocated Size (before finalize): " << (double)allocate_size_load/(1024*1024) << "MB" << "\n";
  //   std::cout << "==========================================" << std::endl;
  // }

  double time_to_load = mytimer.current_time();
  graph.finalize();
  double time_all = mytimer.current_time();

  std::cout << dc.procid() << ": Finished in " << mytimer.current_time() << std::endl;
    std::cout 
      << "========== Graph statistics on proc " << dc.procid() << " ==============="
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
      << "\n --------------------------------------------" 
      << std::endl;
      std::cout << "==========================================" << std::endl;

   // size_t heap_size;
   // size_t allocate_size;
   // MallocExtension::instance()->GetNumericProperty("generic.heap_size", &heap_size);
   // MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &allocate_size);
   // if (dc.procid() == 0) {
   //   std::cout << "Heap Size: " << (double)heap_size/(1024*1024) << "MB" << "\n";
   //   std::cout << "Allocated Size: " << (double)allocate_size/(1024*1024) << "MB" << "\n";
   // }

  if (dc.procid() == 0) {
   std::ofstream fout;
   std::vector<std::string> keys = clopts.get_graph_args().get_option_keys();
   std::string ingress_method = "random";
   std::string constraint_graph = "na";
   std::string bufsize = "50000";
   bool usehash = false; 
   bool userecent = false; 

   foreach (std::string opt, keys) {
     if (opt == "ingress") {
       clopts.get_graph_args().get_option("ingress", ingress_method);
     } else if (opt == "bufsize") {
       clopts.get_graph_args().get_option("bufsize", bufsize);
     } else if (opt == "usehash") {
       clopts.get_graph_args().get_option("usehash", usehash);
     } else if (opt == "userecent") {
       clopts.get_graph_args().get_option("userecent", userecent);
     } else if (opt == "constrained_graph") {
       clopts.get_graph_args().get_option("constrained_graph", constraint_graph);
     }
   }

   fout.open("result.txt");
   fout << "#ingress: " << ingress_method  << std::endl
     << "#constraint: " << constraint_graph << std::endl
     << "#bufsize: " << bufsize << std::endl
     << "#usehash: " << usehash << std::endl
     << "#userecent: " << userecent
     << std::endl;

   fout << "Num procs: " << dc.numprocs() << std::endl;
   fout << "Replication factor: " << (float)graph.num_replicas()/graph.num_vertices() << std::endl;
   fout << "Balance factor: " << (float)graph.num_local_edges()/graph.num_edges() << std::endl;
   // fout << "Heap size (load): " << (double)heap_size_load/(1024*1024) << std::endl;
   // fout << "Allocated size (load): " << (double)allocate_size_load/(1024*1024) << std::endl;
   // fout << "Heap size (finalize): " << (double)heap_size/(1024*1024) << std::endl;
   // fout << "Allocated size (finalize): " << (double)allocate_size/(1024*1024) << std::endl;
   fout << "Runtime (load): " << time_to_load << std::endl;
   fout << "Runtime (total): " << time_all << std::endl;
   fout.close();
  }

  // graph.get_local_graph().save_adjacency("partition_"+boost::lexical_cast<std::string>(dc.procid())+".txt");
  // graph.save_format("partition", "snap", false, 1);

  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
} // end of main

#include <graphlab/macros_undef.hpp>
