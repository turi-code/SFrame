/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
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

#ifndef GRAPHLAB_DISTRIBUTED_OBLIVIOUS_INGRESS_HPP
#define GRAPHLAB_DISTRIBUTED_OBLIVIOUS_INGRESS_HPP


#include <graph/graph_basic_types.hpp>
#include <graph/ingress/distributed_ingress_base.hpp>
#include <graph/ingress/ingress_edge_decision.hpp>
#include <rpc/buffered_exchange.hpp>
#include <rpc/distributed_event_log.hpp>
#include <util/dense_bitset.hpp>
#include <graphlab/util/cuckoo_map_pow2.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
  /**
   * \brief Ingress object assigning edges using randoming hash function.
   */
  template<typename GraphType>
  class distributed_oblivious_ingress: 
    public distributed_ingress_base<GraphType> {
  public:
    typedef GraphType  graph_type;
    /// The type of the vertex data stored in the graph 
    typedef typename graph_type::vertex_data_type vertex_data_type;
    /// The type of the edge data stored in the graph 
    typedef typename graph_type::edge_data_type edge_data_type;

    typedef typename graph_type::vertex_record vertex_record;
    typedef typename graph_type::mirror_type mirror_type;

    typedef distributed_ingress_base<GraphType> base_type;
    // typedef typename boost::unordered_map<vertex_id_type, std::vector<size_t> > degree_hash_table_type;
    typedef fixed_dense_bitset<RPC_MAX_N_PROCS> bin_counts_type; 

    /** Type of the degree hash table: 
     * a map from vertex id to a bitset of length num_procs. */
    typedef cuckoo_map_pow2<vertex_id_type, bin_counts_type,3,uint32_t> degree_hash_table_type;
    degree_hash_table_type dht;

    /** Array of number of edges on each proc. */
    std::vector<size_t> proc_num_edges;

    /** Ingress tratis. */
    bool usehash;
    bool userecent;

  public:
    distributed_oblivious_ingress(distributed_control& dc, graph_type& graph, bool usehash = false, bool userecent = false) :
      base_type(dc, graph),
      dht(-1),proc_num_edges(dc.numprocs()), usehash(usehash), userecent(userecent) { 

      INITIALIZE_TRACER(ob_ingress_compute_assignments, "Time spent in compute assignment");
     }

    ~distributed_oblivious_ingress() { }

    /** Add an edge to the ingress object using oblivious greedy assignment. */
    void add_edge(vertex_id_type source, vertex_id_type target,
                  const edge_data_type& edata,
                  size_t thread_id) {
      thread_id = thread_id % MAX_BUFFER_LOCKS;
      dht[source]; dht[target];
      const procid_t owning_proc = 
        base_type::edge_decision.edge_to_proc_greedy(source, target, dht[source], dht[target], proc_num_edges, usehash, userecent);
      typedef typename base_type::edge_buffer_record edge_buffer_record;
      edge_buffer_record record(source, target, edata);
      base_type::edge_exchange.send(owning_proc, record, thread_id); 
    } // end of add edge

    virtual void finalize() {
     dht.clear();
     distributed_ingress_base<GraphType>::finalize(); 
      
    }

  }; // end of distributed_ob_ingress

}; // end of namespace graphlab
#include <graphlab/macros_undef.hpp>


#endif
