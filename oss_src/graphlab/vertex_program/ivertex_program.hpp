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
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved. 
 *
 */

#ifndef GRAPHLAB_IVERTEX_PROGRAM_HPP
#define GRAPHLAB_IVERTEX_PROGRAM_HPP


#include <graphlab/vertex_program/icontext.hpp>
#include <graphlab/util/empty.hpp>
#include <graph/graph_basic_types.hpp>
#include <graph/distributed_graph.hpp>
#include <serialization/serialization_includes.hpp>
#include <graphlab/vertex_program/op_plus_eq_concept.hpp>

#include <graphlab/macros_def.hpp>

#if defined(__cplusplus) && __cplusplus >= 201103L
// for whatever reason boost concept is broken under C++11. 
// Temporary workaround. TOFIX
#undef BOOST_CONCEPT_ASSERT
#define BOOST_CONCEPT_ASSERT(unused)
#endif


namespace graphlab {
  
  /**
   * \brief The ivertex_program class defines the vertex program
   * interface that all vertex programs should extend and implement.
   * The vertex-program is used to encode the user-define computation
   * in a GraphLab program.
   *
   * Overview
   * ==================
   *
   * A vertex program represents the primary user defined computation
   * in GraphLab.  A unique instance of the vertex program is run on
   * each vertex in the graph and can interact with neighboring vertex
   * programs through the gather and scatter functions as well as by
   * signaling neighboring vertex-programs.  Conceptually the
   * vertex-program is a class which represents the parts of an
   * update-function in the original formulation of the GraphLab
   * abstraction.  Moreover many graph-structured programs can be
   * written in the following pattern:
   *
   * \code
   * graphlab::update_function(Vertex center, Neighborhood nbrs) {
   *   // nbrs represents the state of neighboring vertices and edges
   *
   *   // Gather Phase: 
   *   sum = EMPTY;
   *   for(edge in nbrs.in_edges()) {
   *      // The sum is a general commutative associative operation
   *      if(sum == EMPTY) sum = gather_function(center, edge, edge.neighbor());
   *      else sum += gather_function(center, edge, edge.neighbor());
   *   }
   *
   *   // Apply Phase:
   *   center = apply_function(center, sum);
   *
   *   // Scatter Phase:
   *   for(edge in nbrs.out_edges()) {
   *     edge = scatter_function(center, edge, edge.neighbor());
   *     if(condition is met) trigger_neighbor();
   *   }
   *
   * }
   * \endcode
   *
   * Vertex programs express computation by implementing what we call
   * the *Gather-Apply-Scatter (GAS)* model which decomposes the
   * vertex program into a parallel gather phase, followed by an
   * atomic apply phase, and finally a parallel scatter phase.  This
   * decomposition allows us to execute a single vertex program on
   * several machines simultaneously and move computation to the data.  
   *
   * We therefore decompose the update function logic into member
   * functions of the vertex-program class that are invoked in the
   * following manner:
   *
   * \code
   * For the center vertex vtx:
   *   vprog.init(ctx, vtx, msg);
   *   // Gather Phase: 
   *   vprog::gather_type sum = EMPTY;
   *   ParallelFor(adjacent edges in direction vprog.gather_edges(ctx, vtx) )
   *     if(sum == EMPTY) sum = vprog.gather(ctx, vtx, edge);
   *     else sum += vprog.gather(ctx, vtx, edge);
   *   // Apply Phase
   *   vprog.apply(ctx, vtx, sum);
   *   // Scatter Phase
   *   ParallelFor(adjacent edges in direction vprog.scatter_edges(ctx, vtx) )
   *     vprog.scatter(ctx, vtx, edge);
   *   // Vertex program is destroyed
   *   vprog = vertex_program();
   * \endcode
   *
   * All user define vertex programs must extend the ivertex_program
   * interface and implement the ivertex_program::apply function.
   * Most vertex programs will also implement the
   * ivertex_program::gather and ivertex_program::scatter functions.
   *
   * The state of a vertex program *does not* persist between
   * invocations of \ref ivertex_program::init.  Moreover prior to
   * each call to init the vertex program's previous state is
   * cleared. Therefore any persistent state must be saved into the
   * vertex data.
   *
   * The vertex program depends on several key types which are
   * template arguments to ivertex_program interface.
   * 
   * \li graph_type: the type of graph used to store the data for this
   * vertex program.  This currently always the distributed_graph.
   *
   * \li gather_type: the type used in the gather phase and must
   * implement the operator+= function.
   *
   * \li message_type: The type used for signaling and is typically
   * empty.  However if a message type is desired it must implement
   * the operator+= to allow message merging across the network.  In
   * addition the message type may also implement the priority()
   * function which returns a double assigning a priority to the
   * reception of the message (used by the asynchronous engines). We
   * provide a basic set of simple prioritized messages in 
   * \ref graphlab::signals.
   *
   * All user-defined types including the vertex data, edge data,
   * vertex-program, gather type, and message type must be
   * serializable (see \ref sec_serializable) and default
   * constructible to enable movement between machines.
   *
   * Advanced Features
   * ======================
   *
   * While the basic Gather-Apply-Scatter approach to graph structure
   * computation can express a wide range of algorithms there are some
   * situation where additional features could either simplify the
   * design or provide additional efficiency. 
   *
   *
   * Messaging 
   * ----------------------
   * 
   * Vertex-programs can trigger adjacent vertex programs by sending a
   * signal which can contain a message to neighbor vertices.  By
   * default the message type is empty however it is possible for the
   * user to define a message type.  For example the following
   * message_type could be used to implement pagerank:
   *
   * \code
   * struct pagerank_message : public graphlab::IS_POD_TYPE {
   *   double value;
   *   double priority() const { return std::fabs(value); }
   *   message_type& operator+=(const message_type& other) {
   *     value += other.value; 
   *     return *this;
   *   }
   * }; 
   * \endcode
   *
   * Unlike other messaging abstractions, GraphLab always _merges_
   * messages destined to the same vertex.  This allows the GraphLab
   * engines to minimize network communication and more evenly balance
   * computation.  Messages are combined using the operator+=
   * function.
   *
   * As mentioned earlier some engines may prioritize the _reception_
   * of messages.  Messages can optionally (it is not required)
   * provide a priority function which is used to prioritize message
   * reception.  The engine then attempts to prioritize the reception
   * of higher priority messages first.
   *
   * The message is received in the \ref ivertex_program::init
   * function.  The single message passed into 
   * \ref ivertex_program::init represents the sum of all messages 
   * destined to that vertex since the vertex-program was last invoked.
   * 
   * The GraphLab messaging framework allows us to write
   * Pregel-like vertex-programs of the form:
   *
   * \code
   * typedef graphlab::empty gather_type;
   * class pregel_pagerank : 
   *   public ivertex_program<graph_type, gather_type, pagerank_message>,
   *   public graphlab::IS_POD_TYPE {
   *
   *   // Store a local copy of the message data
   *   double message_value;
   *
   *   // Receive the inbound message (sum of messages)
   *   void init(icontext_type& context, const vertex_type& vertex, 
   *             const message_type& msg) { 
   *     message_value = message.value;
   *   }
   *
   *   // Skip the gather phase
   *   edge_dir_type gather_edges(icontext_type& context,
   *                              const vertex_type& vertex) const { 
   *     return graphlab::NO_EDGES; 
   *   }
   *
   *   // Update the pagerank using the message
   *   void apply(icontext_type& context, vertex_type& vertex, 
   *              const gather_type& total) {
   *     vertex.data() += message_value;      
   *   }
   *
   *   // Scatter along out edges
   *   edge_dir_type scatter_edges(icontext_type& context,
   *                               const vertex_type& vertex) const { 
   *     return OUT_EDGES; 
   *   }
   *
   *   // Compute new messages encoding the change in the pagerank of
   *   // adjacent vertices.
   *   void scatter(icontext_type& context, const vertex_type& vertex, 
   *                edge_type& edge) const { 
   *     pagerank_message msg;
   *     msg.value = message_value * (1 - RESET_PROBABILITY);
   *     context.signal(edge.target(), msg);
   *   }
   * }; 
   * \endcode
   *
   * Notice that the gather phase is skipped and instead the gather
   * computation is accomplished using the messages.  However unlike
   * Pregel the scatter function which computs and sends the new
   * message is actually run on the machine that is receiving the
   * message.  
   *
   * The message abstraction is surprisingly powerful and can often
   * often express computation that can be written using the Gather
   * operation.  However, the message combination is done outside of
   * the consistency model and so can lead to more confusing code.
   *
   * Gather Caching
   * ---------------------
   *
   * In many applications the gather computation can be costly and
   * high-degree vertices will be signaled often even only a small
   * fraction of its neighbors values have changed.  In this case
   * running the gather function on all neighbors can be wasteful.  To
   * address this important issue the GraphLab engines expose a gather
   * caching mechanism.  However to take advantage of the gather
   * caching the vertex-program must notify the engine when a cache is
   * no longer valid and can even correct the cache to ensure that it
   * remains valid.
   *
   * \todo finish documenting gather caching
   *
   * 
   * 
   */
  template<typename Graph,
           typename GatherType,
           typename MessageType = graphlab::empty> 
  class ivertex_program {    
  public:

    // User defined type members ==============================================
    /**
     * \brief The user defined vertex data associated with each vertex
     * in the graph (see \ref distributed_graph::vertex_data_type).
     *
     * The vertex data is the data associated with each vertex in the
     * graph.  Unlike the vertex-program the vertex data of adjacent
     * vertices is visible to other vertex programs during the gather
     * and scatter phases and persists between executions of the
     * vertex-program.
     *
     * The vertex data type must be serializable 
     * (see \ref sec_serializable)
     */
    typedef typename Graph::vertex_data_type vertex_data_type;

    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab Requires that the vertex data type be Serializable.  See
     * \ref sec_serializable for details. 
     */
    BOOST_CONCEPT_ASSERT((graphlab::Serializable<vertex_data_type>));
    /// \endcond



    /**
     * \brief The user defined edge data associated with each edge in
     * the graph.
     *
     * The edge data type must be serializable 
     * (see \ref sec_serializable)
     *
     */
    typedef typename Graph::edge_data_type edge_data_type;

    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab Requires that the edge data type be Serializable.  See
     * \ref sec_serializable for details. 
     */
    BOOST_CONCEPT_ASSERT((graphlab::Serializable<edge_data_type>));
    /// \endcond


    /**
     * \brief The user defined gather type is used to accumulate the
     * results of the gather function during the gather phase and must
     * implement the operator += operation.
     *
     * The gather type plays the following role in the vertex program:
     * 
     * \code
     * gather_type sum = EMPTY;
     * for(edges in vprog.gather_edges()) {
     *   if(sum == EMPTY) sum = vprog.gather(...);
     *   else sum += vprog.gather( ... );
     * }
     * vprog.apply(..., sum);
     * \endcode
     *
     * In addition to implementing the operator+= operation the gather
     * type must also be serializable (see \ref sec_serializable).
     */
    typedef GatherType gather_type;
    
    /**
     * \cond GRAPHLAB_INTERNAL
     * \brief GraphLab Requires that the gather type be default
     * constructible.
     *
     * \code
     * class gather_type {
     * public:
     *   gather_type() { }
     * };  
     * \endcode
     */
    BOOST_CONCEPT_ASSERT((boost::DefaultConstructible<GatherType>));
    /// \endcond

    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab Requires that gather type be Serializable.  See
     * \ref sec_serializable for detials
     */
    BOOST_CONCEPT_ASSERT((graphlab::Serializable<GatherType>));
    /// \endcond

    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab Requires that gather type support operator+=.
     */
    BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<GatherType>));
    /// \endcond



    /**
     * \cond GRAPHLAB_INTERNAL
     *
     *  \brief GraphLab Requires that the gather type be serializable
     *
     * \code
     * class gather_type {
     * public:
     *   gather_type() { }
     * };  
     * \endcode
     */
    BOOST_CONCEPT_ASSERT((boost::DefaultConstructible<GatherType>));
    /// \endcond



    /**
     * The message type which must be provided by the vertex_program
     */
    typedef MessageType message_type;


    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab requires that the message type be default
     * constructible.
     *
     * \code
     * class message_type {
     * public:
     *   message_type() { }
     * };  
     * \endcode
     * 
     */
    BOOST_CONCEPT_ASSERT((boost::DefaultConstructible<MessageType>));
    /// \endcond 

    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab requires that the message type be Serializable.
     * See \ref sec_serializable for detials
     */
    BOOST_CONCEPT_ASSERT((graphlab::Serializable<MessageType>));
    /// \endcond

    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab requires that message type support operator+=.
     */
    BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<MessageType>));
    /// \endcond


    // Graph specific type members ============================================
    /**
     * \brief The graph type associative with this vertex program. 
     *
     * The graph type is specified as a template argument and will
     * usually be \ref distributed_graph.
     */
    typedef Graph graph_type;

    /**
     * \brief The unique integer id used to reference vertices in the graph.
     * 
     * See \ref graphlab::vertex_id_type for details.
     */
    typedef typename graph_type::vertex_id_type vertex_id_type;
    
    /**
     * \brief The opaque vertex object type used to get vertex
     * information.
     *
     * The vertex type is defined by the graph.  
     * See \ref distributed_graph::vertex_type for details.
     */
    typedef typename graph_type::vertex_type vertex_type;
    
    /**
     * \brief The opaque edge_object type used to access edge
     * information.
     * 
     * The edge type is defined by the graph.  
     * See \ref distributed_graph::edge_type for details.
     */
    typedef typename graph_type::edge_type edge_type;

    /**
     * \brief The type used to define the direction of edges used in
     * gather and scatter.
     * 
     * Possible values include:
     *
     * \li graphlab::NO_EDGES : Do not process any edges
     * 
     * \li graphlab::IN_EDGES : Process only inbound edges to this
     * vertex
     * 
     * \li graphlab::OUT_EDGES : Process only outbound edges to this
     * vertex
     *
     * \li graphlab::ALL_EDGES : Process both inbound and outbound
     * edges on this vertes.
     * 
     * See \ref graphlab::edge_dir_type for details.
     */
    typedef graphlab::edge_dir_type edge_dir_type;

    // Additional Types =======================================================
    
    /**
     * \brief The context type is used by the vertex program to
     * communicate with the engine.
     *
     * The context and provides facilities for signaling adjacent
     * vertices (sending messages), interacting with the GraphLab
     * gather cache (posting deltas), and accessing engine state.
     *
     */
    typedef icontext<graph_type, gather_type, message_type> icontext_type;
   
    // Functions ==============================================================
    /**
     * \brief Standard virtual destructor for an abstract class.
     */
    virtual ~ivertex_program() { }

    /**
     * \brief This called by the engine to receive a message to this
     * vertex program.  The vertex program can use this to initialize
     * any state before entering the gather phase.  The init function
     * is invoked _once_ per execution of the vertex program.
     *
     * If the vertex program does not implement this function then the
     * default implementation (NOP) is used.
     *
     * \param [in,out] context The context is used to interact with the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified within the init function.  If there is some
     * message state that is needed then it must be saved to the
     * vertex-program and not the vertex data.
     *
     * \param [in] message The sum of all the signal calls to this
     * vertex since it was last run.
     */
    virtual void init(icontext_type& context,
                      const vertex_type& vertex, 
                      const message_type& msg) { /** NOP */ }
    

    /**
     * \brief Returns the set of edges on which to run the gather
     * function.  The default edge direction is in edges.
     *
     * The gather_edges function is invoked after the init function
     * has completed.
     *
     * \warning The gather_edges function may be invoked multiple
     * times for the same execution of the vertex-program and should
     * return the same value.  In addition it cannot modify the
     * vertex-programs state or the vertex data.
     *
     * Possible return values include:
     *
     * \li graphlab::NO_EDGES : The gather phase is completely skipped
     * potentially reducing network communication.
     * 
     * \li graphlab::IN_EDGES : The gather function is only run on
     * inbound edges to this vertex.
     * 
     * \li graphlab::OUT_EDGES : The gather function is only run on
     * outbound edges to this vertex.
     *
     * \li graphlab::ALL_EDGES : The gather function is run on both
     * inbound and outbound edges to this vertes.
     * 
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     * 
     * \return One of graphlab::NO_EDGES, graphlab::IN_EDGES,
     * graphlab::OUT_EDGES, or graphlab::ALL_EDGES.
     * 
     */
    virtual edge_dir_type gather_edges(icontext_type& context,
                                       const vertex_type& vertex) const { 
      return IN_EDGES; 
    }


    /**
     * \brief The gather function is called on all the 
     * \ref ivertex_program::gather_edges in parallel and returns the 
     * \ref gather_type which are added to compute the final output of 
     * the gather phase.  
     *
     * The gather function is the core computational element of the
     * Gather phase and is responsible for collecting the information
     * about the state of adjacent vertices and edges.  
     *
     * \warning The gather function is executed in parallel on
     * multiple machines and therefore cannot modify the
     * vertex-program's state or the vertex data.
     *
     * A default implementation of the gather function is provided
     * which will fail if invoked. 
     *
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     *
     * \param [in,out] edge The adjacent edge to be processed.  The
     * edge is not constant and therefore the edge data can be
     * modified. 
     *
     * \return the result of the gather computation which will be
     * "summed" to produce the input to the apply operation.  The
     * behavior of the "sum" is defined by the \ref gather_type.
     * 
     */
    virtual gather_type gather(icontext_type& context, 
                               const vertex_type& vertex, 
                               edge_type& edge) const {
      logstream(LOG_FATAL) << "Gather not implemented!" << std::endl;
      return gather_type();
    };


    /**
     * \brief The apply function is called once the gather phase has
     * completed and must be implemented by all vertex programs.
     *
     * The apply function is responsible for modifying the vertex data
     * and is run only once per vertex per execution of a vertex
     * program.  In addition the apply function can modify the state
     * of the vertex program.
     *
     * If a vertex has no neighbors than the apply function is called
     * passing the default value for the gather_type.
     *
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in,out] vertex The vertex on which this vertex-program is
     * running. 
     *
     * \param [in] total The result of the gather phase.  If a vertex
     * has no neighbors then the total is the default value (i.e.,
     * gather_type()) of the gather type.
     * 
     */
    virtual void apply(icontext_type& context, 
                       vertex_type& vertex, 
                       const gather_type& total) = 0;

    /**
     * \brief Returns the set of edges on which to run the scatter
     * function.  The default edge direction is out edges.
     *
     * The scatter_edges function is invoked after the apply function
     * has completed.
     *
     * \warning The scatter_edges function may be invoked multiple
     * times for the same execution of the vertex-program and should
     * return the same value.  In addition it cannot modify the
     * vertex-programs state or the vertex data.
     *
     * Possible return values include:
     *
     * \li graphlab::NO_EDGES : The scatter phase is completely
     * skipped potentially reducing network communication.
     * 
     * \li graphlab::IN_EDGE : The scatter function is only run on
     * inbound edges to this vertex.
     * 
     * \li graphlab::OUT_EDGES : The scatter function is only run on
     * outbound edges to this vertex.
     *
     * \li graphlab::ALL_EDGES : The scatter function is run on both
     * inbound and outbound edges to this vertes.
     * 
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     * 
     * \return One of graphlab::NO_EDGES, graphlab::IN_EDGES,
     * graphlab::OUT_EDGES, or graphlab::ALL_EDGES.
     * 
     */
    virtual edge_dir_type scatter_edges(icontext_type& context,
                                        const vertex_type& vertex) const { 
      return OUT_EDGES; 
    }

    /**
     * \brief Scatter is called on all scatter_edges() in parallel
     * after the apply function has completed and is typically
     * responsible for updating edge data, signaling (messaging)
     * adjacent vertices, and updating the gather cache state when
     * caching is enabled.
     *
     * The scatter function is almost identical to the gather function
     * except that nothing is returned. 
     *
     * \warning The scatter function is executed in parallel on
     * multiple machines and therefore cannot modify the
     * vertex-program's state or the vertex data.
     *
     * A default implementation of the gather function is provided
     * which will fail if invoked. 
     *
     * \param [in,out] context The context is used to interact with
     * the engine
     *
     * \param [in] vertex The vertex on which this vertex-program is
     * running. Note that the vertex is constant and its value should
     * not be modified.
     *
     * \param [in,out] edge The adjacent edge to be processed.  The
     * edge is not constant and therefore the edge data can be
     * modified. 
     *
     */
    virtual void scatter(icontext_type& context, const vertex_type& vertex, 
                         edge_type& edge) const { 
      logstream(LOG_FATAL) << "Scatter not implemented!" << std::endl;
    };


    /** 
     * \internal
     * Used to signal the start of a local gather.
     * Called on each machine which is doing a gather operation.
     * Semantics are that, a complete gather involves
     * 
     * \code
     * On each machine with edges adjacent to vertex being updated:
     *   vprogram.pre_local_gather(g) // passed by reference
     *   foreach edge adjacent to vertex:
     *     if ( ... first gather ... ) g = vprogram.gather(edge)
     *     else g += vprogram.gather(edge)
     *   end
     *   vprogram.post_local_gather(g) // passed by reference
     * \endcode
     */
    virtual void pre_local_gather(gather_type&) const {
    }

    /** 
     * \internal
     * Used to signal the end of a local gather.
     * Called on each machine which is doing a gather operation.
     * Semantics are that, a complete gather involves
     * 
     * \code
     * On each machine with edges adjacent to vertex being updated:
     *   vprogram.pre_local_gather(g) // passed by reference
     *   foreach edge adjacent to vertex:
     *     if ( ... first gather ... ) g = vprogram.gather(edge)
     *     else g += vprogram.gather(edge)
     *   end
     *   vprogram.post_local_gather(g) // passed by reference
     * \endcode
     */
    virtual void post_local_gather(gather_type&) const {
    }

  };  // end of ivertex_program
 
}; //end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif
