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


#ifndef GRAPHLAB_WARP_GRAPH_BROADCAST_HPP
#define GRAPHLAB_WARP_GRAPH_BROADCAST_HPP

#include <boost/bind.hpp>
#include <graphlab/util/generics/conditional_combiner_wrapper.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_control.hpp>
#include <fiber/fiber_remote_request.hpp>
#include <logger/assertions.hpp>
#include <rpc/dc.hpp>
#include <graphlab/engine/warp_event_log.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

namespace warp {

namespace warp_impl {

template <typename EngineType, typename GraphType>
struct broadcast_neighborhood_impl {

  typedef typename EngineType::context_type context_type;
  typedef typename GraphType::vertex_type vertex_type;
  typedef typename GraphType::vertex_data_type vertex_data_type;
  typedef typename GraphType::edge_type edge_type;
  typedef typename GraphType::local_vertex_type local_vertex_type;
  typedef typename GraphType::local_edge_type local_edge_type;
  typedef typename GraphType::vertex_record vertex_record;


/**************************************************************************/
/*                                                                        */
/*              Basic MapReduce Neighborhood Implementation               */
/*                                                                        */
/**************************************************************************/
/*
 * The master calls basic_mapreduce_neighborhood.
 * Which then issues calls to basic_local_mapper on each machine with a replica.
 */

  static void basic_local_broadcast_neighborhood(context_type& context, 
                                                 edge_dir_type edge_direction,
                                                 void(*broadcast_fn)(context_type& context,
                                                                     edge_type edge,
                                                                     vertex_type other),
                                                 vertex_id_type vid) {
    GraphType& graph(context.graph);
    lvid_type lvid = graph.local_vid(vid);
    local_vertex_type local_vertex(context.graph.l_vertex(lvid));
   
    if(edge_direction == IN_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.in_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.source());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();
        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        broadcast_fn(context, edge, other);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
    // do out edges
    if(edge_direction == OUT_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.out_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.target());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();
        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        broadcast_fn(context, edge, other);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
  }


  static void basic_local_broadcast_neighborhood_from_remote(std::pair<size_t, size_t> objid,
                                                             edge_dir_type edge_direction,
                                                             size_t broadcast_ptr,
                                                             vertex_id_type vid,
                                                             vertex_data_type& vdata) {
    EngineType* engine = reinterpret_cast<EngineType*>(distributed_control::get_instance()->get_registered_object(objid.first));
    GraphType* graph = reinterpret_cast<GraphType*>(distributed_control::get_instance()->get_registered_object(objid.second));
    vertex_type vertex(graph->l_vertex(graph->local_vid(vid)));
    context_type context(*engine, *graph, vertex);
    vertex.data() = vdata;
    // cast the mappers and combiners back into their pointer types
    void(*broadcast_fn)(context_type&, edge_type edge, vertex_type other) = 
        reinterpret_cast<void(*)(context_type&, edge_type, vertex_type)>(broadcast_ptr);
    basic_local_broadcast_neighborhood(
        context,
        edge_direction,
        broadcast_fn,
        vid);
  }

  static void basic_broadcast_neighborhood(context_type& context,
                                           typename GraphType::vertex_type current,
                                              edge_dir_type edge_direction,
                                              void(*broadcast_fn)(context_type& context, edge_type edge, vertex_type other)) {
    // get a reference to the graph
    GraphType& graph = current.graph_ref;
    // get the object ID of the graph
    std::pair<size_t, size_t> objid(context.engine.get_rpc_obj_id(), graph.get_rpc_obj_id());
    typename GraphType::vertex_record vrecord = graph.l_get_vertex_record(current.local_id());

    // make sure we are running on a master vertex
    ASSERT_EQ(vrecord.owner, distributed_control::get_instance_procid());
    
    // create num-mirrors worth of requests
    std::vector<request_future<void > > requests(vrecord.num_mirrors());
    
    size_t ctr = 0;
    foreach(procid_t proc, vrecord.mirrors()) {
      // issue the communication
        requests[ctr] = fiber_remote_request(proc, 
                                             basic_local_broadcast_neighborhood_from_remote,
                                             objid,
                                             edge_direction,
                                             reinterpret_cast<size_t>(broadcast_fn),
                                             current.id(),
                                             current.data());
        ++ctr;
    }
    // compute the local tasks
    basic_local_broadcast_neighborhood(context,
                                       edge_direction, 
                                       broadcast_fn, 
                                       current.id());
    // now, wait for everyone
    for (size_t i = 0;i < requests.size(); ++i) {
      requests[i]();
    }
  }
};


template <typename EngineType, typename GraphType, typename ExtraArg>
struct broadcast_neighborhood_impl2 {

  typedef typename EngineType::context_type context_type;
  typedef typename GraphType::vertex_type vertex_type;
  typedef typename GraphType::vertex_data_type vertex_data_type;
  typedef typename GraphType::edge_type edge_type;
  typedef typename GraphType::local_vertex_type local_vertex_type;
  typedef typename GraphType::local_edge_type local_edge_type;
  typedef typename GraphType::vertex_record vertex_record;

/**************************************************************************/
/*                                                                        */
/*           Extended MapReduce Neighborhood Implementation               */
/*                                                                        */
/**************************************************************************/
/*
 * The master calls extended_mapreduce_neighborhood.
 * Which then issues calls to extended_local_mapper on each machine with a replica.
 * The extended mapreduce neighborhood allows the mapper and combiner to take
 * an optional argument
 */


  static void extended_local_broadcast_neighborhood(context_type& context,
                                 edge_dir_type edge_direction,
                                 void(*broadcast_fn)(context_type& context,
                                                     edge_type edge,
                                                     vertex_type other,
                                                     const ExtraArg extra),
                                 vertex_id_type vid,
                                 const ExtraArg extra) {
    GraphType& graph(context.graph);
    lvid_type lvid = graph.local_vid(vid);
    local_vertex_type local_vertex(graph.l_vertex(lvid));
    
    if(edge_direction == IN_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.in_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.source());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();
        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        broadcast_fn(context, edge, other, extra);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
    // do out edges
    if(edge_direction == OUT_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.out_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.target());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();
        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        broadcast_fn(context, edge, other, extra);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
  }


  static void extended_local_broadcast_neighborhood_from_remote(std::pair<size_t, size_t> objid,
                                                                edge_dir_type edge_direction,
                                                                size_t broadcast_ptr,
                                                                vertex_id_type vid,
                                                                vertex_data_type& vdata,
                                                                const ExtraArg extra) {

    EngineType* engine = reinterpret_cast<EngineType*>(distributed_control::get_instance()->get_registered_object(objid.first));
    GraphType* graph = reinterpret_cast<GraphType*>(distributed_control::get_instance()->get_registered_object(objid.second));
    vertex_type vertex(graph->l_vertex(graph->local_vid(vid)));
    context_type context(*engine, *graph, vertex);
    vertex.data() = vdata;
    // cast the mappers and combiners back into their pointer types
    void(*broadcast_fn)(context_type&, edge_type edge, vertex_type other, const ExtraArg) = 
        reinterpret_cast<void(*)(context_type&, edge_type, vertex_type, const ExtraArg)>(broadcast_ptr);
    extended_local_broadcast_neighborhood(
        context,
        edge_direction,
        broadcast_fn,
        vid,
        extra);
  }

  static void extended_broadcast_neighborhood(context_type& context,
                                              typename GraphType::vertex_type current,
                                              edge_dir_type edge_direction,
                                              void(*broadcast_fn)(context_type& context, edge_type edge, vertex_type other, const ExtraArg extra),
                                              const ExtraArg extra) {
    // get a reference to the graph
    GraphType& graph = current.graph_ref;
    // get the object ID of the graph
    std::pair<size_t, size_t> objid(context.engine.get_rpc_obj_id(), graph.get_rpc_obj_id());
    typename GraphType::vertex_record vrecord = graph.l_get_vertex_record(current.local_id());

    // make sure we are running on a master vertex
    ASSERT_EQ(vrecord.owner, distributed_control::get_instance_procid());
    
    // create num-mirrors worth of requests
    std::vector<request_future<void> > requests(vrecord.num_mirrors());
    
    size_t ctr = 0;
    foreach(procid_t proc, vrecord.mirrors()) {
      // issue the communication
        requests[ctr] = fiber_remote_request(proc, 
                                             extended_local_broadcast_neighborhood_from_remote,
                                             objid,
                                             edge_direction,
                                             reinterpret_cast<size_t>(broadcast_fn),
                                             current.id(),
                                             current.data(),
                                             extra);
        ++ctr;
    }
    // compute the local tasks
    extended_local_broadcast_neighborhood(context, 
                                          edge_direction, 
                                          broadcast_fn, 
                                          current.id(),
                                          extra);
    // now, wait for everyone
    for (size_t i = 0;i < requests.size(); ++i) {
      requests[i]();
    }
  }


};

} // namespace warp::warp_impl


/**
 * \ingroup warp
 *
 * the broadcast_neighborhood function allows a parallel transformation of the
 * neighborhood of a vertex to be performed and also provides a warp_engine
 * context. . This is a blocking operation, and
 * will not return until the distributed computation is complete.  When run
 * inside a fiber, to hide latency, the system will automatically context
 * switch to evaluate some other fiber which is ready to run. This function 
 * is functionally similar to the transform_neighborhood function, but requires
 * a \ref graphlab::warp_engine "warp_engine" 
 * context to be provided. The warp_engine context will also be passed on to
 * the transform function.
 *
 * Abstractly, the computation accomplishes the following:
 *
 * \code
 * foreach(edge in neighborhood of current vertex) {
 *   transform_fn(context, edge, opposite vertex)
 * }
 * \endcode
 *
 * \attention It is important that the transform_fn should only make
 * modifications to the edge data, and not the data on either of the vertices.
 *
 * \attention Unlike the transform_neighborhood function, this call actually
 * performs synchronization, so the value of both vertex endpoints are
 * correct. 
 *
 * Here is an example which schedules all vertices on out edges.
 * 
 * \code
 * void schedule(engine_type::context& context,
 *               graph_type::edge_type edge, 
 *               graph_type::vertex_type other) {
 *   context.signal(other);
 * }
 *
 *
 * void update_function(engine_type::context& context,
 *                      graph_type::vertex_type vertex) {
 *    warp::broadcast_neighborhood(context,
 *                                 vertex,
 *                                 OUT_EDGES,
 *                                 schedule);
 * }
 *
 * \endcode
 *
 *
 * An overload is provided which allows you to pass an additional arbitrary
 * argument to the broadcast.
 *
 * \param current The vertex to map reduce the neighborhood over
 * \param edge_direction To run over all IN_EDGES, OUT_EDGES or ALL_EDGES
 * \param transform_fn The transform function to run on all the selected edges.
 * 
 * \see warp_engine
 * \see warp::parfor_all_vertices
 * \see warp::map_reduce_neighborhood
 * \see warp::transform_neighborhood
 */
template <typename ContextType, typename VertexType>
void broadcast_neighborhood(ContextType& context,
                            VertexType current,
                                edge_dir_type edge_direction,
                                void (*broadcast_fn)(ContextType& context,
                                                     typename VertexType::graph_type::edge_type edge,
                                                        VertexType other)) {
  INCREMENT_EVENT(EVENT_WARP_BROADCAST_COUNT, 1);
  warp_impl::
      broadcast_neighborhood_impl<typename ContextType::engine_type, typename VertexType::graph_type>::
                                      basic_broadcast_neighborhood(context, current, edge_direction, 
                                                                   broadcast_fn);
  context.set_synchronized();
}



/**
 * \ingroup warp
 *
 * the broadcast_neighborhood function allows a parallel transformation of the
 * neighborhood of a vertex to be performed and also provides a warp_engine
 * context. . This is a blocking operation, and
 * will not return until the distributed computation is complete.  When run
 * inside a fiber, to hide latency, the system will automatically context
 * switch to evaluate some other fiber which is ready to run. This function 
 * is functionally similar to the transform_neighborhood function, but requires
 * a \ref graphlab::warp_engine "warp_engine" 
 * context to be provided. The \ref graphlab::warp_engine::context 
 * "warp_engine context" will also be passed on to
 * the transform function.
 *
 * This is the more general overload of the broadcast_neighborhood function
 * which allows an additional arbitrary extra argument to be passed along
 * to the transform function
 *
 * Abstractly, the computation accomplishes the following:
 *
 * \code
 * foreach(edge in neighborhood of current vertex) {
 *   transform_fn(context, edge, opposite vertex, extraarg)
 * }
 * \endcode
 *
 * \attention It is important that the transform_fn should only make
 * modifications to the edge data, and not the data on either of the vertices.
 *
 * \attention Unlike the transform_neighborhood function, this call actually
 * performs synchronization, so the value of both vertex endpoints are
 * correct. 
 *
 * Here is an example which schedules all vertices on out edges with a 
 * particular value
 * \code
 * void schedule(engine_type::context& context,
 *               graph_type::edge_type edge, 
 *               graph_type::vertex_type other,
 *               int value) {
 *   if (edge.data() == value)  context.signal(other);
 * }
 *
 *
 * void update_function(engine_type::context& context,
 *                      graph_type::vertex_type vertex) {
 *    // schedule all neighbors connected to an out edge 
 *    // with value 10
 *    warp::broadcast_neighborhood(context,
 *                                 vertex,
 *                                 OUT_EDGES,
 *                                 int(10),
 *                                 schedule));
 * }
 *
 * \endcode
 *
 * \param current The vertex to map reduce the neighborhood over
 * \param edge_direction To run over all IN_EDGES, OUT_EDGES or ALL_EDGES
 * \param extra An additional argument to be passed to the broadcast
 * \param transform_fn The transform function to run on all the selected edges.
 * 
 * \see warp_engine
 * \see warp::parfor_all_vertices()
 * \see warp::map_reduce_neighborhood()
 * \see warp::transform_neighborhood()
 */
template <typename ContextType, typename ExtraArg, typename VertexType>
void broadcast_neighborhood(ContextType& context,
                            VertexType current,
                                edge_dir_type edge_direction,
                                void(*broadcast_fn)(ContextType& context,
                                                    typename VertexType::graph_type::edge_type edge,
                                                        VertexType other,
                                                        const ExtraArg extra),
                                const ExtraArg extra) {
  INCREMENT_EVENT(EVENT_WARP_BROADCAST_COUNT, 1);
  warp_impl::
      broadcast_neighborhood_impl2<typename ContextType::engine_type, typename VertexType::graph_type, ExtraArg>::
                                      extended_broadcast_neighborhood(context, current, edge_direction, broadcast_fn, extra);
  context.set_synchronized();
}




} // namespace warp

} // namespace graphlab

#include <graphlab/macros_undef.hpp>
#endif
