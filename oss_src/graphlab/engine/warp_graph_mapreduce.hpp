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


#ifndef GRAPHLAB_WARP_GRAPH_MAP_REDUCE_HPP
#define GRAPHLAB_WARP_GRAPH_MAP_REDUCE_HPP

#include <boost/bind.hpp>
#include <graphlab/util/generics/conditional_combiner_wrapper.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_control.hpp>
#include <fiber/fiber_remote_request.hpp>
#include <logger/assertions.hpp>
#include <rpc/dc.hpp>
#include <graphlab/engine/warp_event_log.hpp>
#include <graphlab/macros_def.hpp>

#if defined(__cplusplus) && __cplusplus >= 201103L
#include <functional>
#endif

namespace graphlab {

namespace warp {

namespace warp_impl {

/**
 * The default combiner used for combining mapped results from
 * warp::map_reduce_neighborhood(); merges self with other using operator +=. 
 */
template <typename T>
void default_combiner(T& self, const T& other) {
  self += other;
}

/**
 * The default combiner used for combining mapped results from
 * warp::map_reduce_neighborhood() which
 * takes an unused argument; merges self with other using operator +=. 
 */
template <typename T, typename ExtraArgs>
void extended_default_combiner(T& self, const T& other, const ExtraArgs unused) {
  self += other;
}


template <typename RetType, typename GraphType>
struct map_reduce_neighborhood_impl {

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

  static conditional_combiner_wrapper<RetType> basic_local_mapper(GraphType& graph,
                                                           edge_dir_type edge_direction,
                                                           RetType (*mapper)(edge_type edge, vertex_type other),
                                                           void (*combiner)(RetType&, const RetType&),
                                                           vertex_id_type vid) {
    lvid_type lvid = graph.local_vid(vid);
    local_vertex_type local_vertex(graph.l_vertex(lvid));
    
    conditional_combiner_wrapper<RetType> accum(combiner);
    if(edge_direction == IN_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.in_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.source());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();
        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        accum += mapper(edge, other);
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
        accum += mapper(edge, other);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
    return accum;
  }


  static conditional_combiner_wrapper<RetType> basic_local_mapper_from_remote(size_t objid,
                                                           edge_dir_type edge_direction,
                                                           size_t mapper_ptr,
                                                           size_t combiner_ptr,
                                                           vertex_id_type vid) {
    // cast the mappers and combiners back into their pointer types
    RetType (*mapper)(edge_type edge, vertex_type other) = 
        reinterpret_cast<RetType(*)(edge_type, vertex_type)>(mapper_ptr);
    void (*combiner)(RetType&, const RetType&) = 
        reinterpret_cast<void (*)(RetType&, const RetType&)>(combiner_ptr);
    return basic_local_mapper(
        *reinterpret_cast<GraphType*>(distributed_control::get_instance()->get_registered_object(objid)),
        edge_direction,
        mapper,
        combiner,
        vid);
  }

  static RetType basic_map_reduce_neighborhood(typename GraphType::vertex_type current,
                                               edge_dir_type edge_direction,
                                               RetType (*mapper)(edge_type edge,
                                                                 vertex_type other),
                                               void (*combiner)(RetType& self, 
                                                                const RetType& other)) {
    // get a reference to the graph
    GraphType& graph = current.graph_ref;
    // get the object ID of the graph
    size_t objid = graph.get_rpc_obj_id();
    typename GraphType::vertex_record vrecord = graph.l_get_vertex_record(current.local_id());

    // make sure we are running on a master vertex
    ASSERT_EQ(vrecord.owner, distributed_control::get_instance_procid());
    
    // create num-mirrors worth of requests
    std::vector<request_future<conditional_combiner_wrapper<RetType> > > requests(vrecord.num_mirrors());
    
    size_t ctr = 0;
    foreach(procid_t proc, vrecord.mirrors()) {
      // issue the communication
        requests[ctr] = fiber_remote_request(proc, 
                                             map_reduce_neighborhood_impl<RetType, GraphType>::basic_local_mapper_from_remote,
                                             objid,
                                             edge_direction,
                                             reinterpret_cast<size_t>(mapper),
                                             reinterpret_cast<size_t>(combiner),
                                             current.id());
        ++ctr;
    }
    // compute the local tasks
    conditional_combiner_wrapper<RetType> accum = basic_local_mapper(graph, 
                                                                     edge_direction, 
                                                                     mapper, 
                                                                     combiner,
                                                                     current.id());
    accum.set_combiner(combiner);
    // now, wait for everyone
    for (size_t i = 0;i < requests.size(); ++i) {
      accum += requests[i]();
    }
    return accum.value;
  }

};

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

template <typename RetType, typename GraphType, typename ExtraArg>
struct map_reduce_neighborhood_impl2 {

  typedef typename GraphType::vertex_type vertex_type;
  typedef typename GraphType::edge_type edge_type;
  typedef typename GraphType::local_vertex_type local_vertex_type;
  typedef typename GraphType::local_edge_type local_edge_type;
  typedef typename GraphType::vertex_record vertex_record;

  static conditional_combiner_wrapper<RetType> extended_local_mapper(GraphType& graph,
                                                              edge_dir_type edge_direction,
                                                              RetType (*mapper)(edge_type edge, vertex_type other, const ExtraArg),
                                                              void (*combiner)(RetType&, const RetType&, const ExtraArg),
                                                              vertex_id_type vid,
                                                              const ExtraArg extra) {

    lvid_type lvid = graph.local_vid(vid);
    local_vertex_type local_vertex(graph.l_vertex(lvid));
    
    conditional_combiner_wrapper<RetType> accum(boost::bind(combiner, _1, _2, extra));
    if(edge_direction == IN_EDGES || edge_direction == ALL_EDGES) {
      foreach(local_edge_type local_edge, local_vertex.in_edges()) {
        edge_type edge(local_edge);
        vertex_type other(local_edge.source());
        lvid_type a = edge.source().local_id(), b = edge.target().local_id();

        graph.get_lock_manager()[std::min(a,b)].lock();
        graph.get_lock_manager()[std::max(a,b)].lock();
        accum += mapper(edge, other, extra);
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
        accum += mapper(edge, other, extra);
        graph.get_lock_manager()[a].unlock();
        graph.get_lock_manager()[b].unlock();
      }
    } 
    return accum;
  }

  static conditional_combiner_wrapper<RetType> extended_local_mapper_from_remote(size_t objid,
                                                              edge_dir_type edge_direction,
                                                              size_t mapper_ptr,
                                                              size_t combiner_ptr,
                                                              vertex_id_type vid,
                                                              const ExtraArg extra) {
    // cast the mappers and combiners back into their pointer types
    RetType (*mapper)(edge_type edge, vertex_type other, const ExtraArg) = 
        reinterpret_cast<RetType(*)(edge_type, vertex_type, const ExtraArg)>(mapper_ptr);
    void (*combiner)(RetType&, const RetType&, const ExtraArg) = 
        reinterpret_cast<void (*)(RetType&, const RetType&, const ExtraArg)>(combiner_ptr);
    return extended_local_mapper(
        *reinterpret_cast<GraphType*>(distributed_control::get_instance()->get_registered_object(objid)),
        edge_direction,
        mapper,
        combiner,
        vid,
        extra);
  }

  static RetType extended_map_reduce_neighborhood(typename GraphType::vertex_type current,
                                                  edge_dir_type edge_direction,
                                                  const ExtraArg extra,
                                                  RetType (*mapper)(edge_type edge,
                                                                    vertex_type other,
                                                                    const ExtraArg extra),
                                                  void (*combiner)(RetType& self, 
                                                                   const RetType& other,
                                                                   const ExtraArg extra)) {
    // get a reference to the graph
    GraphType& graph = current.graph_ref;
    typename GraphType::vertex_record vrecord = graph.l_get_vertex_record(current.local_id());
    size_t objid = graph.get_rpc_obj_id();

    // make sure we are running on a master vertex
    ASSERT_EQ(vrecord.owner, distributed_control::get_instance_procid());
    
    // create num-mirrors worth of requests
    std::vector<request_future<conditional_combiner_wrapper<RetType> > > requests(vrecord.num_mirrors());
    
    size_t ctr = 0;
    foreach(procid_t proc, vrecord.mirrors()) {
      // issue the communication
      requests[ctr] = fiber_remote_request(proc, 
                                           map_reduce_neighborhood_impl2::extended_local_mapper_from_remote,
                                           objid,
                                           edge_direction,
                                           reinterpret_cast<size_t>(mapper),
                                           reinterpret_cast<size_t>(combiner),
                                           current.id(),
                                           extra);
        ++ctr;
    }
    // compute the local tasks
    conditional_combiner_wrapper<RetType> accum = 
        extended_local_mapper(graph, edge_direction, mapper, 
                              combiner, current.id(), extra);

    accum.set_combiner(boost::bind(combiner, _1, _2, boost::ref(extra)));
    // now, wait for everyone
    for (size_t i = 0;i < requests.size(); ++i) {
      accum += requests[i]();
    }
    return accum.value;
  }

};

} // namespace warp::warp_impl

/**
 * \ingroup warp
 *
 * This Warp function allows a map-reduce aggregation of the neighborhood of a
 * vertex to be performed. This is a blocking operation, and will not return
 * until the distributed computation is complete.  When run inside a fiber, to
 * hide latency, the system will automatically context switch to evaluate some
 * other fiber which is ready to run. 
 *
 * Abstractly, the computation accomplishes the following:
 *
 * \code
 * ResultType result()
 * foreach(edge in neighborhood of current vertex) {
 *   combiner(result, mapper(edge, opposite vertex))
 * }
 * return result
 * \endcode
 *
 * \attention This call does not accomplish synchronization, thus 
 * modifications to the current vertex will not be reflected during
 * the call. In other words, inside the mapper function, only the values on
 * edge.data() and other.data() will be valid. The value of the vertex
 * on the "self" end of the edge will not reflect changes you made to the vertex
 * immediately before calling warp::map_reduce_neighborhood(). Use the overload
 * of map_reduce_neighborhood (below) if you want to pass on additional
 * information to the mapper.
 *
 * Here is an example which implements the PageRank computation, using
 * the parfor_all_vertices function to create a parallel for loop using fibers.
 * \code
 * float pagerank_map(graph_type::edge_type edge, graph_type::vertex_type other) {
 *  return other.data() / other.num_out_edges();
 * }
 *
 * // the function arguments of the combiner must match the return type of the
 * // map function.
 * void pagerank_combine(float& a, const float& b) {
 *   a += b;
 * }
 *
 * void pagerank(graph_type::vertex_type vertex) {
 *    // computes an aggregate over the neighborhood using map_reduce_neighborhood
 *    // The pagerank_map function will be executed over every in-edge of the graph,
 *    // and the result  of each map is combined using the pagerank_combine 
 *    // function. The pagerank_combine is not strictly necessary here since the
 *    // default combine behavior is to use += anyway.
 *    vertex.data() = 0.15 + 0.85 * warp::map_reduce_neighborhood(vertex,
 *                                                                IN_EDGES,
 *                                                                pagerank_map,
 *                                                                pagerank_combine);
 * }
 *
 * ...
 * // runs the pagerank function on all the vertices in the graph.
 * parfor_all_vertices(graph, pagerank); 
 * \endcode
 *
 * An overload is provided which allows you to pass an additional arbitrary
 * argument to the mappers and combiners.
 *
 *
 * \param current The vertex to map reduce the neighborhood over
 * \param edge_direction To run over all IN_EDGES, OUT_EDGES or ALL_EDGES
 * \param mapper The map function that will be executed. Must be a function pointer.
 * \param combiner The combine function that will be executed. Must be a function pointer.
 *                 Optional. Defaults to using "+=" on the output of the mapper
 * 
 * \return The result of the neighborhood map reduce operation. The return
 * type matches the return type of the mapper.
 *
 * \see warp_engine
 * \see warp::parfor_all_vertices()
 * \see warp::transform_neighborhood()
 * \see warp::broadcast_neighborhood()
 */

#if defined(__cplusplus) && __cplusplus >= 201103L

template <typename VertexType, typename MapperType>
auto map_reduce_neighborhood(VertexType current,
                  edge_dir_type edge_direction,
                  MapperType mapper) -> decltype(mapper(typename VertexType::graph_type::edge_type(), current)){
  typedef decltype(mapper(typename VertexType::graph_type::edge_type(), current)) result_type ;
  static_assert(std::is_convertible<MapperType, 
                                    result_type (*)(typename VertexType::graph_type::edge_type,
                                                    VertexType)>::value,
                "Mapper is not convertible to a function pointer of the appropriate type");
  return warp_impl::
      map_reduce_neighborhood_impl<result_type, 
                                  typename VertexType::graph_type>::
                    basic_map_reduce_neighborhood(current, edge_direction, 
                                                  mapper, 
                                                  warp_impl::default_combiner<result_type>);
}



template <typename VertexType, typename MapperType, typename CombinerType>
auto map_reduce_neighborhood(VertexType current,
                  edge_dir_type edge_direction,
                  MapperType mapper,
                  CombinerType combiner) -> decltype(mapper(typename VertexType::graph_type::edge_type(), current)){
  typedef decltype(mapper(typename VertexType::graph_type::edge_type(), current)) result_type ;
  static_assert(std::is_convertible<MapperType, 
                                    result_type (*)(typename VertexType::graph_type::edge_type,
                                                    VertexType)>::value,
                "Mapper is not convertible to a function pointer of the appropriate type");

  static_assert(std::is_convertible<CombinerType, 
                                    void (*)(result_type& self, 
                                             const result_type& other)>::value,
                "Combiner is not convertible to a function pointer of the appropriate type");

  return warp_impl::
      map_reduce_neighborhood_impl<result_type, 
                                  typename VertexType::graph_type>::
                    basic_map_reduce_neighborhood(current, edge_direction, 
                                                  mapper, 
                                                  combiner);
}
#else
template <typename RetType, typename VertexType>
RetType map_reduce_neighborhood(VertexType current,
                                edge_dir_type edge_direction,
                                RetType (*mapper)(typename VertexType::graph_type::edge_type edge,
                                                  VertexType other),
                                void (*combiner)(RetType& self, 
                                                 const RetType& other) = warp_impl::default_combiner<RetType>) {
  return warp_impl::
      map_reduce_neighborhood_impl<RetType, 
                                  typename VertexType::graph_type>::
                                      basic_map_reduce_neighborhood(current, edge_direction, 
                                                                    mapper, combiner);
}

#endif

/**
 * \ingroup warp
 *
 * This Warp function allows a map-reduce aggregation of the neighborhood of a
 * vertex to be performed. This is a blocking operation, and will not return
 * until the distributed computation is complete.  When run inside a fiber, to
 * hide latency, the system will automatically context switch to evaluate some
 * other fiber which is ready to run. 
 *
 * This is the more general overload of the map_reduce_neighborhood function
 * which allows an additional arbitrary extra argument to be passed along
 * to the mapper and combiner functions.
 * 
 * Abstractly, the computation accomplishes the following:
 *
 * \code
 * ResultType result()
 * foreach(edge in neighborhood of current vertex) {
 *   combiner(result, mapper(edge, opposite vertex, extraarg), extraarg)
 * }
 * return result
 * \endcode
 *
 * Here is an example which implements the PageRank computation, using
 * the parfor_all_vertices function to create a parallel for loop using fibers.
 * We demonstrate the additional argument by passing on the 0.85 scaling value to
 * be computed in the map.
 * \code
 * float pagerank_map(graph_type::edge_type edge, 
 *                    graph_type::vertex_type other, const float scale) {
 *  // the scale value here will match the last argument passed into
 *  // the map_reduce_neighborhood call. In this case, it is fixed to
 *  // 0.85.
 *  return scale * other.data() / other.num_out_edges();
 * }
 *
 * // the function arguments of the combiner must match the return type of the
 * // map function.
 * void pagerank_combine(float& a, const float& b, const float _scale_unused) {
 *   a += b;
 * }
 *
 * void pagerank(graph_type::vertex_type vertex) {
 *    // computes an aggregate over the neighborhood using map_reduce_neighborhood
 *    // The pagerank_map function will be executed over every in-edge of the graph,
 *    // and the result  of each map is combined using the pagerank_combine 
 *    // function. The pagerank_combine is not strictly necessary here since the
 *    // default combine behavior is to use += anyway.
 *    vertex.data() = 0.15 + warp::map_reduce_neighborhood(vertex,
 *                                                         IN_EDGES,
 *                                                         float(0.85),
 *                                                         pagerank_map,
 *                                                         pagerank_combine);
 * }
 *
 * ...
 * // runs the pagerank function on all the vertices in the graph.
 * parfor_all_vertices(graph, pagerank); 
 * \endcode
 *
 *
 * \param current The vertex to map reduce the neighborhood over
 * \param edge_direction To run over all IN_EDGES, OUT_EDGES or ALL_EDGES
 * \param extra An additional argument to be passed to the mapper and combiner
 * functions.  
 * \param mapper The map function that will be executed. Must be a
 * function pointer.
 * \param combiner The combine function that will be executed. Must be a
 * function pointer.  Optional. Defaults to using "+=" on the output of the
 * mapper \return The result of the neighborhood map reduce operation. The
 * return type matches the return type of the mapper.
 *
 * \see warp_engine
 * \see warp::parfor_all_vertices()
 * \see warp::transform_neighborhood()
 * \see warp::broadcast_neighborhood()
 */

#if defined(__cplusplus) && __cplusplus >= 201103L

template <typename VertexType, typename ExtraArg, typename MapperType>
auto map_reduce_neighborhood(VertexType current,
                  edge_dir_type edge_direction,
                  const ExtraArg extra,
                  MapperType mapper) -> decltype(mapper(typename VertexType::graph_type::edge_type(), current, extra)){
  typedef decltype(mapper(typename VertexType::graph_type::edge_type(), current, extra)) result_type ;
  static_assert(std::is_convertible<MapperType, 
                                    result_type (*)(typename VertexType::graph_type::edge_type,
                                                    VertexType,
                                                    const ExtraArg)>::value,
                "Mapper is not convertible to a function pointer of the appropriate type");

  return warp_impl::
      map_reduce_neighborhood_impl2<result_type, typename VertexType::graph_type, ExtraArg>::
                    extended_map_reduce_neighborhood(current, 
                                                     edge_direction, 
                                                     extra, 
                                                     mapper, 
                                                     warp_impl::extended_default_combiner<result_type, ExtraArg>);
}



template <typename VertexType, typename ExtraArg, typename MapperType, typename CombinerType>
auto map_reduce_neighborhood(VertexType current,
                  edge_dir_type edge_direction,
                  const ExtraArg extra,
                  MapperType mapper,
                  CombinerType combiner) -> decltype(mapper(typename VertexType::graph_type::edge_type(), current, extra)){
  typedef decltype(mapper(typename VertexType::graph_type::edge_type(), current, extra)) result_type ;
  static_assert(std::is_convertible<MapperType, 
                                    result_type (*)(typename VertexType::graph_type::edge_type,
                                                    VertexType,
                                                    const ExtraArg)>::value,
                "Mapper is not convertible to a function pointer of the appropriate type");

  static_assert(std::is_convertible<CombinerType, 
                                    void (*)(result_type& self, 
                                             const result_type& other,
                                             const ExtraArg)>::value,
                "Combiner is not convertible to a function pointer of the appropriate type");

  return warp_impl::
      map_reduce_neighborhood_impl2<result_type, typename VertexType::graph_type, ExtraArg>::
                                      extended_map_reduce_neighborhood(current, edge_direction, 
                                                                       extra, 
                                                                       mapper, combiner);
}

#else
template <typename RetType, typename ExtraArg, typename VertexType>
RetType map_reduce_neighborhood(VertexType current,
                                edge_dir_type edge_direction,
                                const ExtraArg extra,
                                RetType (*mapper)(typename VertexType::graph_type::edge_type edge,
                                                  VertexType other,
                                                  const ExtraArg extra),
                                void (*combiner)(RetType& self, 
                                                 const RetType& other,
                                                 const ExtraArg extra) = warp_impl::extended_default_combiner<RetType, ExtraArg>) {
  return warp_impl::
      map_reduce_neighborhood_impl2<RetType, typename VertexType::graph_type, ExtraArg>::
                                      extended_map_reduce_neighborhood(current, edge_direction, 
                                                                       extra, 
                                                                       mapper, combiner);
}
#endif


} // namespace warp

} // namespace graphlab

#include <graphlab/macros_undef.hpp>
#endif
