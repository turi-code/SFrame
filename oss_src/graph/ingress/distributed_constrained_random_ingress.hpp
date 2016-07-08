/**
 * Copyright (C) 2016 Turi
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

#ifndef GRAPHLAB_DISTRIBUTED_CONSTRAINED_RANDOM_INGRESS_HPP
#define GRAPHLAB_DISTRIBUTED_CONSTRAINED_RANDOM_INGRESS_HPP

#include <boost/functional/hash.hpp>

#include <rpc/buffered_exchange.hpp>
#include <graph/graph_basic_types.hpp>
#include <graph/ingress/distributed_ingress_base.hpp>
#include <graph/ingress/sharding_constraint.hpp>
#include <graph/ingress/ingress_edge_decision.hpp>


#include <graphlab/macros_def.hpp>
namespace graphlab {
  /**
   * \brief Ingress object assigning edges using randoming hash function.
   */
  template<typename GraphType>
  class distributed_constrained_random_ingress : 
    public distributed_ingress_base<GraphType> {
  public:
    typedef GraphType  graph_type;
    /// The type of the vertex data stored in the graph 
    typedef typename graph_type::vertex_data_type vertex_data_type;
    /// The type of the edge data stored in the graph 
    typedef typename graph_type::edge_data_type edge_data_type;


    typedef distributed_ingress_base<GraphType> base_type;

    sharding_constraint* constraint;
    boost::hash<vertex_id_type> hashvid;

  public:
    distributed_constrained_random_ingress(distributed_control& dc, graph_type& graph,
                                           const std::string& method) :
    base_type(dc, graph) {
      constraint = new sharding_constraint(dc.numprocs(), method);
    } // end of constructor

    ~distributed_constrained_random_ingress() { 
      delete constraint;
    }

    /** Add an edge to the ingress object using random assignment. */
    void add_edge(vertex_id_type source, vertex_id_type target,
                  const edge_data_type& edata,
                  size_t thread_id) {
      typedef typename base_type::edge_buffer_record edge_buffer_record;

      thread_id = thread_id % MAX_BUFFER_LOCKS;
      const std::vector<procid_t>& candidates = constraint->get_joint_neighbors(graph_hash::hash_vertex(source) % base_type::rpc.numprocs(),
                                                                                graph_hash::hash_vertex(target) % base_type::rpc.numprocs());

      const procid_t owning_proc = 
          base_type::edge_decision.edge_to_proc_random(source, target, candidates);


      const edge_buffer_record record(source, target, edata);

      base_type::edge_exchange.send(owning_proc, record, thread_id);
    } // end of add edge
  }; // end of distributed_constrained_random_ingress
}; // end of namespace graphlab
#include <graphlab/macros_undef.hpp>


#endif
