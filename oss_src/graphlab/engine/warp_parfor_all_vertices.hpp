/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
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


#ifndef GRAPHLAB_WARP_PARFOR_ALL_VERTICES_HPP
#define GRAPHLAB_WARP_PARFOR_ALL_VERTICES_HPP
#include <boost/function.hpp>
#include <fiber/fiber_group.hpp>
#include <parallel/atomic.hpp>
#include <graph/vertex_set.hpp>
#include <graphlab/engine/warp_event_log.hpp>
#include <rpc/dc.hpp>
namespace graphlab {
namespace warp {

namespace warp_impl {


/*
 * Actual Parfor implementation.
 * Holds a reference to all the arguments.
 * Each fiber increments the atomic counter and runs the fn on it)
 */
template <typename GraphType>
struct parfor_all_vertices_impl{

  GraphType& graph; 
  boost::function<void(typename GraphType::vertex_type)> fn;
  vertex_set& vset;
  atomic<size_t> ctr;

  parfor_all_vertices_impl(GraphType& graph,
                           boost::function<void(typename GraphType::vertex_type)> fn,
                           vertex_set& vset): graph(graph),fn(fn),vset(vset),ctr(0) { }

  void run_fiber() {
    while (1) {
      size_t lvid = ctr.inc_ret_last();
      if (lvid >= graph.num_local_vertices() || !vset.l_contains(lvid)) break;
      typename GraphType::local_vertex_type l_vertex = graph.l_vertex(lvid);
      if (l_vertex.owned()) {
        typename GraphType::vertex_type vertex(l_vertex);
        INCREMENT_EVENT(EVENT_WARP_PARFOR_VERTEX_COUNT, 1);
        fn(vertex);
      }
    } 
  }
};

} // namespace warp_impl


/**
 * \ingroup warp
 *
 * This Warp function provides a simple parallel for loop over all vertices
 * in the graph, or in a given set of vertices. A large number of light-weight 
 * threads called fibers are used to run the user specified function,
 * allowing the user to make what normally will be blocking calls, on 
 * the neighborhood of each vertex.
 *
 * \code
 * float pagerank_map(graph_type::edge_type edge, graph_type::vertex_type other) {
 *  return other.data() / other.num_out_edges();
 * }
 *
 * void pagerank(graph_type::vertex_type vertex) {
 *    // computes an aggregate over the neighborhood using map_reduce_neighborhood
 *    vertex.data() = 0.15 + 0.85 * warp::map_reduce_neighborhood(vertex,
 *                                                                IN_EDGES,
 *                                                                pagerank_map);
 * }
 *
 * ...
 * // runs the pagerank function on all the vertices in the graph.
 * parfor_all_vertices(graph, pagerank); 
 * \endcode
 *
 * \param graph A reference to the graph object
 * \param fn A function to run on each vertex. Has the prototype void(GraphType::vertex_type). Can be a boost::function
 * \param vset A set of vertices to run on
 * \param nfibers Number of fiber threads to use. Defaults to 10000
 * \param stacksize Size of each fiber stack in bytes. Defaults to 16384 bytes
 *
 * \see graphlab::warp::map_reduce_neighborhood()
 * \see graphlab::warp::warp_graph_transform()
 */
template <typename GraphType, typename FunctionType>
void parfor_all_vertices(GraphType& graph,
                         FunctionType fn,
                         vertex_set vset = GraphType::complete_set(),
                         size_t nfibers = 10000,
                         size_t stacksize = 16384) {
  distributed_control::get_instance()->barrier();
  initialize_counters();
  fiber_group group;
  group.set_stacksize(stacksize);
  warp_impl::parfor_all_vertices_impl<GraphType> parfor(graph, fn, vset);
  
  for (size_t i = 0;i < nfibers; ++i) {
    group.launch(boost::bind(&warp_impl::parfor_all_vertices_impl<GraphType>::run_fiber, &parfor));
  }
  group.join();
  distributed_control::get_instance()->barrier();
  graph.synchronize(vset);
}

} // namespace warp
} // namespace graphlab
#endif
