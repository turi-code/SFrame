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


#ifndef GRAPHLAB_WARP_GRAPH_TRANSFORM_HPP
#define GRAPHLAB_WARP_GRAPH_TRANSFORM_HPP

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

template <typename GraphType>
struct transform_neighborhood_impl {

  typedef typename GraphType::vertex_type vertex_type;
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

  static void basic_local_transform_neighborhood(GraphType& graph,
                                                 edge_dir_type edge_direction,
                                                 void(*transform_fn)(edge_type edge,
                                                                     vertex_type other),
                                                 vertex_id_type vid) {
    lvid_type lvid = graph.local_vid(vid);
    local_vertex_type local_vertex(graph.l_vertex(lvid));
    
    if(edge_direction == IN_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.in_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.source());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();
        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        transform_fn(edge, other);
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
        transform_fn(edge, other);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
  }


  static void basic_local_transform_neighborhood_from_remote(size_t objid,
                                                             edge_dir_type edge_direction,
                                                             size_t transform_ptr,
                                                             vertex_id_type vid) {
    // cast the mappers and combiners back into their pointer types
    void(*transform_fn)(edge_type edge, vertex_type other) = 
        reinterpret_cast<void(*)(edge_type, vertex_type)>(transform_ptr);
    basic_local_transform_neighborhood(
        *reinterpret_cast<GraphType*>(distributed_control::get_instance()->get_registered_object(objid)),
        edge_direction,
        transform_fn,
        vid);
  }

  static void basic_transform_neighborhood(typename GraphType::vertex_type current,
                                              edge_dir_type edge_direction,
                                              void(*transform_fn)(edge_type edge, vertex_type other)) {
    // get a reference to the graph
    GraphType& graph = current.graph_ref;
    // get the object ID of the graph
    size_t objid = graph.get_rpc_obj_id();
    typename GraphType::vertex_record vrecord = graph.l_get_vertex_record(current.local_id());

    // make sure we are running on a master vertex
    ASSERT_EQ(vrecord.owner, distributed_control::get_instance_procid());
    
    // create num-mirrors worth of requests
    std::vector<request_future<void > > requests(vrecord.num_mirrors());
    
    size_t ctr = 0;
    foreach(procid_t proc, vrecord.mirrors()) {
      // issue the communication
        requests[ctr] = fiber_remote_request(proc, 
                                             transform_neighborhood_impl<GraphType>::basic_local_transform_neighborhood_from_remote,
                                             objid,
                                             edge_direction,
                                             reinterpret_cast<size_t>(transform_fn),
                                             current.id());
        ++ctr;
    }
    // compute the local tasks
    basic_local_transform_neighborhood(graph, 
                                       edge_direction, 
                                       transform_fn, 
                                       current.id());
    // now, wait for everyone
    for (size_t i = 0;i < requests.size(); ++i) {
      requests[i]();
    }
  }
};


template <typename GraphType, typename ExtraArg>
struct transform_neighborhood_impl2 {

  typedef typename GraphType::vertex_type vertex_type;
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


  static void extended_local_transform_neighborhood(GraphType& graph,
                                 edge_dir_type edge_direction,
                                 void(*transform_fn)(edge_type edge,
                                                     vertex_type other,
                                                     const ExtraArg extra),
                                 vertex_id_type vid,
                                 const ExtraArg extra) {
    lvid_type lvid = graph.local_vid(vid);
    local_vertex_type local_vertex(graph.l_vertex(lvid));
    
    if(edge_direction == IN_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.in_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.source());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();
        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        transform_fn(edge, other, extra);
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
        transform_fn(edge, other, extra);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
  }


  static void extended_local_transform_neighborhood_from_remote(size_t objid,
                                                                edge_dir_type edge_direction,
                                                                size_t transform_ptr,
                                                                vertex_id_type vid,
                                                                const ExtraArg extra) {
    // cast the mappers and combiners back into their pointer types
    void(*transform_fn)(edge_type edge, vertex_type other, const ExtraArg) = 
        reinterpret_cast<void(*)(edge_type, vertex_type, const ExtraArg)>(transform_ptr);
    extended_local_transform_neighborhood(
        *reinterpret_cast<GraphType*>(distributed_control::get_instance()->get_registered_object(objid)),
        edge_direction,
        transform_fn,
        vid,
        extra);
  }

  static void extended_transform_neighborhood(typename GraphType::vertex_type current,
                                              edge_dir_type edge_direction,
                                              void(*transform_fn)(edge_type edge, vertex_type other, const ExtraArg extra),
                                              const ExtraArg extra) {
    // get a reference to the graph
    GraphType& graph = current.graph_ref;
    // get the object ID of the graph
    size_t objid = graph.get_rpc_obj_id();
    typename GraphType::vertex_record vrecord = graph.l_get_vertex_record(current.local_id());

    // make sure we are running on a master vertex
    ASSERT_EQ(vrecord.owner, distributed_control::get_instance_procid());
    
    // create num-mirrors worth of requests
    std::vector<request_future<void> > requests(vrecord.num_mirrors());
    
    size_t ctr = 0;
    foreach(procid_t proc, vrecord.mirrors()) {
      // issue the communication
        requests[ctr] = fiber_remote_request(proc, 
                                             transform_neighborhood_impl2<GraphType, ExtraArg>::extended_local_transform_neighborhood_from_remote,
                                             objid,
                                             edge_direction,
                                             reinterpret_cast<size_t>(transform_fn),
                                             current.id(),
                                             extra);
        ++ctr;
    }
    // compute the local tasks
    extended_local_transform_neighborhood(graph, 
                                          edge_direction, 
                                          transform_fn, 
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
 * The transform_neighborhood() function allows a parallel transformation of the
 * neighborhood of a vertex to be performed. This is a blocking operation, and
 * will not return until the distributed computation is complete.  When run
 * inside a fiber, to hide latency, the system will automatically context
 * switch to evaluate some other fiber which is ready to run. 
 *
 * Abstractly, the computation accomplishes the following:
 *
 * \code
 * foreach(edge in neighborhood of current vertex) {
 *   transform_fn(edge, opposite vertex)
 * }
 * \endcode
 *
 * \attention It is important that the transform_fn should only make modifications to the
 * edge data, and not the data on the other vertex.
 *
 * \attention This call does not accomplish synchronization, thus 
 * modifications to the current vertex will not be reflected during
 * the call. In other words, inside the mapper function, only the values on
 * edge.data() and other.data() will be valid. The value of the vertex
 * on the "self" end of the edge will not reflect changes you made to the vertex
 * immediately before calling transform_neighborhood(). Use the overload of 
 * transform_neighborhood (below) if you want to pass on additional information to the
 * mapper.
 *
 * Here is an example which clears all the in edge values of some set of vertices.
 * \code
 * void clear_value(graph_type::edge_type edge, graph_type::vertex_type other) {
 *  edge.data() = 0;
 * }
 *
 *
 * void clear_in_edges(graph_type::vertex_type vertex) {
 *    warp::transform_neighborhood(vertex,
 *                                 IN_EDGES,
 *                                 clear_value);
 * }
 *
 * ...
 * warp::parfor_all_vertices(graph, clear_in_edges, some_vset); 
 * \endcode
 *
 *
 * An overload is provided which allows you to pass an additional arbitrary
 * argument to the transform
 *
 * \param current The vertex to map reduce the neighborhood over
 * \param edge_direction To run over all IN_EDGES, OUT_EDGES or ALL_EDGES
 * \param transform_fn The transform function to run on all the selected edges.
 * 
 * \see warp_engine
 * \see warp::parfor_all_vertices()
 * \see warp::map_reduce_neighborhood()
 * \see warp::broadcast_neighborhood()
 */
template <typename VertexType>
void transform_neighborhood(VertexType current,
                                edge_dir_type edge_direction,
                                void (*transform_fn)(typename VertexType::graph_type::edge_type edge,
                                                        VertexType other)) {
  INCREMENT_EVENT(EVENT_WARP_TRANSFORM_COUNT, 1);
  warp_impl::
      transform_neighborhood_impl<typename VertexType::graph_type>::
                                      basic_transform_neighborhood(current, edge_direction, 
                                                                   transform_fn);
}


/**
 * \ingroup warp
 *
 * The transform_neighborhood function allows a parallel transformation of the
 * neighborhood of a vertex to be performed. This is a blocking operation, and
 * will not return until the distributed computation is complete.  When run
 * inside a fiber, to hide latency, the system will automatically context
 * switch to evaluate some other fiber which is ready to run. 
 *
 * This is the more general overload of the 
 * warp::transform_neighborhood() function
 * which allows an additional arbitrary extra argument to be passed along
 * to the mapper and combiner functions.
 *
 * Abstractly, the computation accomplishes the following:
 *
 * \code
 * foreach(edge in neighborhood of current vertex) {
 *   transform_fn(edge, extraarg, opposite vertex)
 * }
 * \endcode
 *
 * \attention It is important that the transform_fn should only make modifications to the
 * edge data, and not the data on the other vertex.
 *
 * \attention This call does not accomplish synchronization, thus 
 * modifications to the current vertex will not be reflected during
 * the call. In other words, inside the mapper function, only the values on
 * edge.data() and other.data() will be valid. The value of the vertex
 * on the "self" end of the edge will not reflect changes you made to the vertex
 * immediately before calling warp::transform_neighborhood() . Use the overload of 
 * warp::transform_neighborhood() if you want to pass on additional information to the
 * mapper.
 *
 * Here is an example which set all the in edge values of some set of vertices
 * to the source vertex's value.
 * \code
 * void set_value(graph_type::edge_type edge, graph_type::vertex_type other,
 *                  int value) {
 *  edge.data() = value;
 * }
 *
 *
 * void set_in_edges(graph_type::vertex_type vertex) {
 *    warp::transform_neighborhood(vertex,
 *                                 IN_EDGES,
 *                                 clear_value,
 *                                 vertex.data(), // say this is an integer
 *                                 );
 * }
 *
 * ...
 * warp::parfor_all_vertices(graph, set_in_edges, some_vset); 
 * \endcode
 *
 *
 * \param current The vertex to map reduce the neighborhood over
 * \param edge_direction To run over all IN_EDGES, OUT_EDGES or ALL_EDGES
 * \param extra An additional argument to be passed to the mapper and combiner functions.
 * \param transform_fn The transform function to run on all the selected edges.
 * 
 * \see warp_engine
 * \see warp::parfor_all_vertices()
 * \see warp::map_reduce_neighborhood()
 * \see warp::broadcast_neighborhood()
 */
template <typename ExtraArg, typename VertexType>
void transform_neighborhood(VertexType current,
                                edge_dir_type edge_direction,
                                void(*transform_fn)(typename VertexType::graph_type::edge_type edge,
                                                        VertexType other,
                                                        const ExtraArg extra),
                                const ExtraArg extra) {
  INCREMENT_EVENT(EVENT_WARP_TRANSFORM_COUNT, 1);
  warp_impl::
      transform_neighborhood_impl2<typename VertexType::graph_type, ExtraArg>::
                                      extended_transform_neighborhood(current, edge_direction, 
                                                                      transform_fn,
                                                                      extra);
}




} // namespace warp

} // namespace graphlab

#include <graphlab/macros_undef.hpp>
#endif
