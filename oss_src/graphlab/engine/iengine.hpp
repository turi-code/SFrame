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



/**
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved.  
 *
 * Contributed under the iCLA for:
 *    Joseph Gonzalez (jegonzal@yahoo-inc.com) 
 *
 */

#ifndef GRAPHLAB_IENGINE_HPP
#define GRAPHLAB_IENGINE_HPP

#include <boost/bind.hpp>
#include <boost/functional.hpp>

#include <graphlab/vertex_program/icontext.hpp>
#include <graphlab/engine/execution_status.hpp>
#include <graphlab/options/graphlab_options.hpp>
#include <serialization/serialization_includes.hpp>
#include <graphlab/aggregation/distributed_aggregator.hpp>
#include <graphlab/vertex_program/op_plus_eq_concept.hpp>
#include <graph/vertex_set.hpp>


#if defined(__cplusplus) && __cplusplus >= 201103L
// for whatever reason boost concept is broken under C++11. 
// Temporary workaround. TOFIX
#undef BOOST_CONCEPT_ASSERT
#define BOOST_CONCEPT_ASSERT(unused)
#endif

namespace graphlab {
  

  /**
   * \ingroup engine
   *
   * \brief The abstract interface of a GraphLab engine.  
   * 
   * A GraphLab engine is responsible for executing vertex programs in
   * parallel on one or more machines.  GraphLab has a collection of
   * different engines with different guarantees on how
   * vertex-programs are executed.  However each engine must implement
   * the iengine interface to allow them to be used "interchangeably."
   *
   * In addition to executing vertex programs GraphLab engines also
   * expose a synchronous aggregation framework. This allows users to
   * attach "map-reduce" style jobs that are run periodically on all
   * edges or vertices while GraphLab programs are actively running.
   *
   * Example Usage
   * =================
   *
   * One can use the iengine interface to select between different
   * engines at runtime:
   *
   * \code
   * iengine<pagerank>* engine_ptr = NULL;
   * if(cmdline_arg == "synchronous") {
   *   engine_ptr = new synchronous_engine<pagerank>(dc, graph, cmdopts);  
   * } else {
   *   engine_ptr = new async_consistent_engine<pagerank>(dc, graph, cmdopts);  
   * }
   * // Attach an aggregator
   * engine_ptr->add_edge_aggregator<float>("edge_map", 
   *                                        edge_map_fun, finalize_fun);
   * // Make it run every 3 seconds
   * engine_ptr->aggregate_periodic("edge_map");
   * // Signal all vertices
   * engine_ptr->signal_all();
   * // Run the engine
   * engine_ptr->start();
   * // do something interesting
   * delete engine_ptr; engine_ptr = NULL;
   * \endcode  
   *
   * @tparam VertexProgram The user defined vertex program which should extend the
   * \ref ivertex_program interface.
   */
  template<typename VertexProgram>
  class iengine {
  public:
    /**
     * \brief The user defined vertex program type which should extend
     * ivertex_program.
     */
    typedef VertexProgram vertex_program_type;

    /**
     * \cond GRAPHLAB_INTERNAL
     * \brief GraphLab Requires that vertex programs be default
     * constructible.
     *
     * \code
     * class vertex_program {
     * public:
     *   vertex_program() { }
     * };  
     * \endcode
     */
    BOOST_CONCEPT_ASSERT((boost::DefaultConstructible<vertex_program_type>));
    /// \endcond



    /**
     * \cond GRAPHLAB_INTERNAL
     *
     * \brief GraphLab requires that the vertex programx type be
     * Serializable.  See \ref sec_serializable for detials.
     */
    BOOST_CONCEPT_ASSERT((graphlab::Serializable<vertex_program_type>));
    /// \endcond


    /**
     * \brief The user defined message type which is defined in
     * ivertex_program::message_type. 
     *
     */
    typedef typename vertex_program_type::message_type message_type;

    /**
     * \brief The graph type which is defined in
     * ivertex_program::graph_type and will typically be
     * \ref distributed_graph.
     */
    typedef typename vertex_program_type::graph_type graph_type;

    /**
     * \brief The vertex identifier type defined in 
     * \ref graphlab::vertex_id_type.
     */
    typedef typename graph_type::vertex_id_type vertex_id_type;  

    /**
     * \brief the vertex object type which contains a reference to the
     * vertex data and is defined in the iengine::graph_type 
     * (see for example \ref distributed_graph::vertex_type).
     */
    typedef typename graph_type::vertex_type    vertex_type;

    /**
     * \brief the edge object type which contains a reference to the
     * edge data and is defined in the iengine::graph_type (see for
     * example \ref distributed_graph::edge_type).
     */
    typedef typename graph_type::edge_type      edge_type;

    /**
     * \brief The context type which is passed into vertex programs as
     * a callback to the engine.  
     *
     * Most engines use the \ref graphlab::context implementation.
     */
    typedef typename vertex_program_type::icontext_type icontext_type;

    /**
     * \brief The type of the distributed aggregator used by each engine to
     * implement distributed aggregation.
     */   
    typedef distributed_aggregator<graph_type, icontext_type> aggregator_type;


    /**
     * \internal
     * \brief Virtual destructor required for inheritance
     */ 
    virtual ~iengine() {};
    
    /**
     * \brief Start the engine execution.
     *
     * Behavior details depend on the engine implementation. See the
     * implementation documentation for specifics.
     * 
     * @return the reason for termination
     */
    virtual execution_status::status_enum start() = 0;
   
    /**
     * \brief Compute the total number of updates (calls to apply)
     * executed since start was last invoked.
     *
     * \return Total number of updates
     */
    virtual size_t num_updates() const = 0;

    /**
     * \brief Get the elapsed time in seconds since start was last
     * called.
     * 
     * \return elapsed time in seconds
     */
    virtual float elapsed_seconds() const = 0;

    /**
     * \brief get the current iteration number.  This is not defined
     * for all engines in which case -1 is returned.
     *
     * \return the current iteration or -1 if not supported.
     */
    virtual int iteration() const { return -1; }

     
    /**
     * \brief Signals single a vertex with an optional message.
     * 
     * This function sends a message to particular vertex which will
     * receive that message on start. The signal function must be
     * invoked on all machines simultaneously.  For example:
     *
     * \code
     * graphlab::synchronous_engine<vprog> engine(dc, graph, opts);
     * engine.signal(0); // signal vertex zero
     * \endcode
     *
     * and _not_:
     *
     * \code
     * graphlab::synchronous_engine<vprog> engine(dc, graph, opts);
     * if(dc.procid() == 0) engine.signal(0); // signal vertex zero
     * \endcode
     *
     * Since signal is executed synchronously on all machines it
     * should only be used to schedule a small set of vertices. The
     * preferred method to signal a large set of vertices (e.g., all
     * vertices that are a certain type) is to use either the vertex
     * program init function or the aggregation framework.  For
     * example to signal all vertices that have a particular value one
     * could write:
     *
     * \code
     * struct bipartite_opt : 
     *   public graphlab::ivertex_program<graph_type, gather_type> {
     *   // The user defined init function
     *   void init(icontext_type& context, vertex_type& vertex) {
     *     // Signal myself if I am a certain type
     *     if(vertex.data().on_left) context.signal(vertex);
     *   }
     *   // other vastly more interesting code
     * };
     * \endcode
     *
     * @param [in] vid the vertex id to signal
     * @param [in] message the message to send to that vertex.  The
     * default message is sent if no message is provided. 
     * (See ivertex_program::message_type for details about the
     * message_type). 
     */
    virtual void signal(vertex_id_type vertex,
                        const message_type& message = message_type()) = 0;
    
    /**
     * \brief Signal all vertices with a particular message.
     * 
     * This function sends the same message to all vertices which will
     * receive that message on start. The signal_all function must be
     * invoked on all machines simultaneously.  For example:
     *
     * \code
     * graphlab::synchronous_engine<vprog> engine(dc, graph, opts);
     * engine.signal_all(); // signal all vertices
     * \endcode
     *
     * and _not_:
     *
     * \code
     * graphlab::synchronous_engine<vprog> engine(dc, graph, opts);
     * if(dc.procid() == 0) engine.signal_all(); // signal vertex zero
     * \endcode
     *
     * The signal_all function is the most common way to send messages
     * to the engine.  For example in the pagerank application we want
     * all vertices to be active on the first round.  Therefore we
     * would write:
     *
     * \code
     * graphlab::synchronous_engine<pagerank> engine(dc, graph, opts);
     * engine.signal_all();
     * engine.start();
     * \endcode
     *
     * @param [in] message the message to send to all vertices.  The
     * default message is sent if no message is provided
     * (See ivertex_program::message_type for details about the
     * message_type). 
     */
    virtual void signal_all(const message_type& message = message_type(),
                            const std::string& order = "shuffle") = 0;
  
    /**
     * \brief Signal a set of vertices with a particular message.
     * 
     * This function sends the same message to a set of vertices which will
     * receive that message on start. The signal_vset function must be
     * invoked on all machines simultaneously.  For example:
     *
     * \code
     * graphlab::synchronous_engine<vprog> engine(dc, graph, opts);
     * engine.signal_vset(vset); // signal a subset of vertices
     * \endcode
     *
     * signal_all() is conceptually equivalent to:
     *
     * \code
     * engine.signal_vset(graph.complete_set());
     * \endcode
     *
     * @param [in] vset The set of vertices to signal 
     * @param [in] message the message to send to all vertices.  The
     * default message is sent if no message is provided
     * (See ivertex_program::message_type for details about the
     * message_type). 
     */
    virtual void signal_vset(const vertex_set& vset,
                             const message_type& message = message_type(),
                             const std::string& order = "shuffle") = 0;


     /** 
     * \brief Creates a vertex aggregator. Returns true on success.
     *        Returns false if an aggregator of the same name already
     *        exists.
     *
     * Creates a vertex aggregator associated to a particular key.
     * The map_function is called over every vertex in the graph, and the
     * return value of the map is summed. The finalize_function is then called
     * on the result of the reduction. The finalize_function is called on
     * all machines. The map_function should only read the graph data,
     * and should not make any modifications.
     *
     * ### Basic Usage 
     * For instance, if the graph has float vertex data, and float edge data:
     * \code
     *   typedef graphlab::distributed_graph<float, float> graph_type;
     * \endcode
     *
     * An aggregator can be constructed to compute the absolute sum of all the
     * vertex data. To do this, we define two functions.
     * \code
     * float absolute_vertex_data(engine_type::icontext_type& context,
     *                            graph_type::vertex_type vertex) {
     *   return std::fabs(vertex.data());
     * }
     *
     * void print_finalize(engine_type::icontext_type& context, 
     *                     float total) {
     *   std::cout << total << "\n";
     * }
     * \endcode
     * 
     * Next, we define the aggregator in the engine by calling 
     * add_vertex_aggregator(). We must assign it a unique
     * name which will be used to reference this particular aggregate
     * operation. We shall call it "absolute_vertex_sum".
     * \code
     * engine.add_vertex_aggregator<float>("absolute_vertex_sum",
     *                                     absolute_vertex_data, 
     *                                     print_finalize);
     * \endcode
     *
     * When executed, the engine execute <code>absolute_vertex_data()</code>
     * on each vertex in the graph. <code>absolute_vertex_data()</code> 
     * reads the vertex data, and returns its absolute value. All return 
     * values are then summing them together using the float's += operator.
     * The final result is than passed to the <code>print_finalize</code>
     * function.  The template argument <code><float></code> is necessary to
     * provide information about the return type of
     * <code>absolute_vertex_data</code>.
     * 
     *
     * This aggregator can be run immediately by calling 
     * aggregate_now() with the name of the aggregator.
     * \code
     * engine.aggregate_now("absolute_vertex_sum");
     * \endcode
     *
     * Or can be arranged to run periodically together with the engine 
     * execution (in this example, every 1.5 seconds).
     * \code
     * engine.aggregate_periodic("absolute_vertex_sum", 1.5);
     * \endcode
     * 
     * Note that since finalize is called on <b>all machines</b>, multiple
     * copies of the total will be printed. If only one copy is desired,
     * see \ref graphlab::icontext::cout() "context.cout()" or to get 
     * the actual process ID using 
     * \ref graphlab::icontext::procid() "context.procid()"
     *
     * In practice, the reduction type can be any arbitrary user-defined type
     * as long as a += operator is defined. This permits great flexibility
     * in the type of operations the aggregator can perform.
     *
     * ### Details
     * The add_vertex_aggregator() function is also templatized over both
     * function types and there is no strong enforcement of the exact argument
     * types of the map function and the reduce function. For instance, in the
     * above example, the following print_finalize() variants may also be
     * accepted.
     *
     * \code
     * void print_finalize(engine_type::icontext_type& context, double total) {
     *   std::cout << total << "\n";
     * }
     *
     * void print_finalize(engine_type::icontext_type& context, float& total) {
     *   std::cout << total << "\n";
     * }
     *
     * void print_finalize(engine_type::icontext_type& context, const float& total) {
     *   std::cout << total << "\n";
     * }
     * \endcode
     * In particlar, the last variation may be useful for performance reasons
     * if the reduction type is large.
     *
     * ### Distributed Behavior
     * To obtain consistent distributed behavior in the distributed setting,
     * we designed the aggregator to minimize the amount of asymmetry among 
     * the machines. In particular, the finalize operation is guaranteed to be
     * called on all machines. This therefore permits global variables to be
     * modified on finalize since all machines are ensured to be eventually
     * consistent. 
     *
     * For instance, in the above example, print_finalize could
     * store the result in a global variable:
     * \code
     * void print_finalize(engine_type::icontext_type& context, float total) {
     *   GLOBAL_TOTAL = total;
     * }
     * \endcode 
     * which will make it accessible to all other running update functions.
     *
     * \tparam ReductionType The output of the map function. Must have
     *                        operator+= defined, and must be \ref sec_serializable.
     * \tparam VertexMapperType The type of the map function. 
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \tparam FinalizerType The type of the finalize function. 
     *                       Not generally needed.
     *                       Can be inferred by the compiler.
     *
     * \param [in] map_function The Map function to use. Must take an
     * \param [in] key The name of this aggregator. Must be unique.
     *                          \ref icontext_type& as its first argument, and
     *                          a \ref vertex_type, or a reference to a 
     *                          \ref vertex_type as its second argument.
     *                          Returns a ReductionType which must be summable
     *                          and \ref sec_serializable .
     * \param [in] finalize_function The Finalize function to use. Must take
     *                               an \ref icontext_type& as its first
     *                               argument and a ReductionType, or a
     *                               reference to a ReductionType as its second
     *                               argument.
     */
    template <typename ReductionType,
              typename VertexMapType,
              typename FinalizerType>
    bool add_vertex_aggregator(const std::string& key,
                               VertexMapType map_function,
                               FinalizerType finalize_function) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));

      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!" 
                             << std::endl;
        return false; // does not return
      }
      return aggregator->template add_vertex_aggregator<ReductionType>(key, map_function, 
                                                              finalize_function);
    } // end of add vertex aggregator

#if defined(__cplusplus) && __cplusplus >= 201103L
    /**
     * \brief An overload of add_vertex_aggregator for C++11 which does not
     *        require the user to provide the reduction type.
     *
     * This function is available only if the compiler has C++11 support.
     * Specifically, it uses C++11's decltype operation to infer the
     * reduction type, thus eliminating the need for the function
     * call to be templatized over the reduction type. For instance,
     * in the add_vertex_aggregator() example, it allows the following
     * code to be written:
     * \code
     * engine.add_vertex_aggregator("absolute_vertex_sum",
     *                              absolute_vertex_data, 
     *                              print_finalize);
     * \endcode
     *
     * \tparam VertexMapperType The type of the map function. 
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \tparam FinalizerType The type of the finalize function. 
     *                       Not generally needed.
     *                       Can be inferred by the compiler.
     *
     * \param [in] key The name of this aggregator. Must be unique.
     * \param [in] map_function The Map function to use. Must take an
     *                          \ref icontext_type& as its first argument, and
     *                          a \ref vertex_type, or a reference to a 
     *                          \ref vertex_type as its second argument.
     *                          Returns a ReductionType which must be summable
     *                          and \ref sec_serializable .
     * \param [in] finalize_function The Finalize function to use. Must take
     *                               an \ref icontext_type& as its first
     *                               argument and a ReductionType, or a
     *                               reference to a ReductionType as its second
     *                               argument.
     */
    template <typename VertexMapType,
              typename FinalizerType>
    bool add_vertex_aggregator(const std::string& key,
                               VertexMapType map_function,
                               FinalizerType finalize_function) {
      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!" 
                             << std::endl;
        return false; // does not return
      }
      return aggregator->add_vertex_aggregator(key, map_function, 
                                               finalize_function);
    } // end of add vertex aggregator

#endif
   

    /** 
     * \brief Creates an edge aggregator. Returns true on success.
     *        Returns false if an aggregator of the same name already
     *        exists.
     *
     * Creates a edge aggregator associated to a particular key.
     * The map_function is called over every edge in the graph, and the
     * return value of the map is summed. The finalize_function is then called
     * on the result of the reduction. The finalize_function is called on
     * all machines. The map_function should only read the graph data,
     * and should not make any modifications.

     *
     * ### Basic Usage 
     * For instance, if the graph has float vertex data, and float edge data:
     * \code
     *   typedef graphlab::distributed_graph<float, float> graph_type;
     * \endcode
     *
     * An aggregator can be constructed to compute the absolute sum of all the
     * edge data. To do this, we define two functions.
     * \code
     * float absolute_edge_data(engine_type::icontext_type& context,
     *                          graph_type::edge_type edge) {
     *   return std::fabs(edge.data());
     * }
     *
     * void print_finalize(engine_type::icontext_type& context, float total) {
     *   std::cout << total << "\n";
     * }
     * \endcode
     * 
     * Next, we define the aggregator in the engine by calling 
     * add_edge_aggregator(). We must assign it a unique
     * name which will be used to reference this particular aggregate
     * operation. We shall call it "absolute_edge_sum".
     * \code
     * engine.add_edge_aggregator<float>("absolute_edge_sum",
     *                                     absolute_edge_data, 
     *                                     print_finalize);
     * \endcode
     *
      *
     * When executed, the engine execute <code>absolute_edge_data()</code>
     * on each edge in the graph. <code>absolute_edge_data()</code> 
     * reads the edge data, and returns its absolute value. All return 
     * values are then summing them together using the float's += operator.
     * The final result is than passed to the <code>print_finalize</code>
     * function.  The template argument <code><float></code> is necessary to
     * provide information about the return type of
     * <code>absolute_edge_data</code>.
     * 
     *
     * This aggregator can be run immediately by calling 
     * aggregate_now() with the name of the aggregator.
     * \code
     * engine.aggregate_now("absolute_edge_sum");
     * \endcode
     *
     * Or can be arranged to run periodically together with the engine 
     * execution (in this example, every 1.5 seconds).
     * \code
     * engine.aggregate_periodic("absolute_edge_sum", 1.5);
     * \endcode
     * 
     * Note that since finalize is called on <b>all machines</b>, multiple
     * copies of the total will be printed. If only one copy is desired,
     * see \ref graphlab::icontext::cout() "context.cout()" or to get 
     * the actual process ID using 
     * \ref graphlab::icontext::procid() "context.procid()"
     *
     * ### Details
     * The add_edge_aggregator() function is also templatized over both
     * function types and there is no strong enforcement of the exact argument
     * types of the map function and the reduce function. For instance, in the
     * above example, the following print_finalize() variants may also be
     * accepted.
     *
     * \code
     * void print_finalize(engine_type::icontext_type& context, double total) {
     *   std::cout << total << "\n";
     * }
     *
     * void print_finalize(engine_type::icontext_type& context, float& total) {
     *   std::cout << total << "\n";
     * }
     *
     * void print_finalize(engine_type::icontext_type& context, const float& total) {
     *   std::cout << total << "\n";
     * }
     * \endcode
     * In particlar, the last variation may be useful for performance reasons
     * if the reduction type is large.
     *
     * ### Distributed Behavior
     * To obtain consistent distributed behavior in the distributed setting,
     * we designed the aggregator to minimize the amount of asymmetry among 
     * the machines. In particular, the finalize operation is guaranteed to be
     * called on all machines. This therefore permits global variables to be
     * modified on finalize since all machines are ensured to be eventually
     * consistent. 
     *
     * For instance, in the above example, print_finalize could
     * store the result in a global variable:
     * \code
     * void print_finalize(engine_type::icontext_type& context, float total) {
     *   GLOBAL_TOTAL = total;
     * }
     * \endcode 
     * which will make it accessible to all other running update functions.
     *
     * \tparam ReductionType The output of the map function. Must have
     *                        operator+= defined, and must be \ref sec_serializable.
     * \tparam EdgeMapperType The type of the map function. 
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \tparam FinalizerType The type of the finalize function. 
     *                       Not generally needed.
     *                       Can be inferred by the compiler.
     *
     * \param [in] key The name of this aggregator. Must be unique.
     * \param [in] map_function The Map function to use. Must take an
     *                          \ref icontext_type& as its first argument, and
     *                          a \ref edge_type, or a reference to a 
     *                          \ref edge_type as its second argument.
     *                          Returns a ReductionType which must be summable
     *                          and \ref sec_serializable .
     * \param [in] finalize_function The Finalize function to use. Must take
     *                               an \ref icontext_type& as its first
     *                               argument and a ReductionType, or a
     *                               reference to a ReductionType as its second
     *                               argument.
     */
 
    template <typename ReductionType,
              typename EdgeMapType,
              typename FinalizerType>
    bool add_edge_aggregator(const std::string& key,
                             EdgeMapType map_function,
                             FinalizerType finalize_function) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!"
                             << std::endl;
        return false; // does not return
      }
      return aggregator->template add_edge_aggregator<ReductionType>
        (key, map_function, finalize_function);
    } // end of add edge aggregator


#if defined(__cplusplus) && __cplusplus >= 201103L

    /**
     * \brief An overload of add_edge_aggregator for C++11 which does not
     *        require the user to provide the reduction type.
     *
     * This function is available only if the compiler has C++11 support.
     * Specifically, it uses C++11's decltype operation to infer the
     * reduction type, thus eliminating the need for the function
     * call to be templatized over the reduction type. For instance,
     * in the add_edge_aggregator() example, it allows the following
     * code to be written:
     * \code
     * engine.add_edge_aggregator("absolute_edge_sum",
     *                              absolute_edge_data, 
     *                              print_finalize);
     * \endcode
     *
     * \tparam EdgeMapperType The type of the map function. 
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \tparam FinalizerType The type of the finalize function. 
     *                       Not generally needed.
     *                       Can be inferred by the compiler.
     *
     * \param [in] key The name of this aggregator. Must be unique.
     * \param [in] map_function The Map function to use. Must take an
     *                          \ref icontext_type& as its first argument, and
     *                          a \ref vertex_type, or a reference to a 
     *                          \ref vertex_type as its second argument.
     *                          Returns a ReductionType which must be summable
     *                          and \ref sec_serializable .
     * \param [in] finalize_function The Finalize function to use. Must take
     *                               an \ref icontext_type& as its first
     *                               argument and a ReductionType, or a
     *                               reference to a ReductionType as its second
     *                               argument.
     */
    template <typename EdgeMapType,
              typename FinalizerType>
    bool add_edge_aggregator(const std::string& key,
                             EdgeMapType map_function,
                             FinalizerType finalize_function) {
      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!" 
                             << std::endl;
        return false; // does not return
      }
      return aggregator->add_edge_aggregator(key, map_function, 
                                             finalize_function);
    } // end of add edge aggregator
#endif

    /**
     * \brief Performs an immediate aggregation on a key
     *
     * Performs an immediate aggregation on a key. All machines must
     * call this simultaneously. If the key is not found,
     * false is returned. Otherwise returns true on success.
     *
     * For instance, the following code will run the aggregator
     * with the name "absolute_vertex_sum" immediately.
     * \code
     * engine.aggregate_now("absolute_vertex_sum");
     * \endcode
     *
     * \param[in] key Key to aggregate now. Must be a key
     *                 previously created by add_vertex_aggregator()
     *                 or add_edge_aggregator().
     * \return False if key not found, True on success.
     */
    bool aggregate_now(const std::string& key) {
      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!" 
                             << std::endl;
        return false; // does not return
      }
      return aggregator->aggregate_now(key);
    } // end of aggregate_now


   /**
    * \brief Performs a map-reduce operation on each vertex in the 
    * graph returning the result.
    * 
    * Given a map function, map_reduce_vertices() call the map function on all
    * vertices in the graph. The return values are then summed together and the
    * final result returned. The map function should only read the vertex data
    * and should not make any modifications. map_reduce_vertices() must be
    * called on all machines simultaneously.
    *
    * ### Basic Usage 
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    * 
    * To compute an absolute sum over all the vertex data, we would write
    * a function which reads in each a vertex, and returns the absolute
    * value of the data on the vertex.
    * \code
    * float absolute_vertex_data(engine_type::icontext_type& context,
    *                            graph_type::vertex_type vertex) {
    *   return std::fabs(vertex.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = engine.map_reduce_vertices<float>(absolute_vertex_data);
    * \endcode
    * will call the <code>absolute_vertex_data()</code> function
    * on each vertex in the graph. <code>absolute_vertex_data()</code>
    * reads the value of the vertex and returns the absolute result.
    * This return values are then summed together and returned. 
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the mapfunction.
    *
    * ### Signalling
    * Another common use for the map_reduce_vertices() function is 
    * in signalling. Since the map function is passed a context, it
    * can be used to perform signalling of vertices for execution
    * during a later \ref start() "engine.start()" call.
    *
    * For instance, the following code will signal all vertices
    * with value >= 1
    * \code
    * graphlab::empty signal_vertices(engine_type::icontext_type& context,
    *                                 graph_type::vertex_type vertex) {
    *   if (vertex.data() >= 1) context.signal(vertex);
    *   return graphlab::empty()
    * }
    * \endcode
    * Note that in this case, we are not interested in a reduction
    * operation, and thus we return a graphlab::empty object.
    * Calling:
    * \code
    * engine.map_reduce_vertices<graphlab::empty>(signal_vertices);
    * \endcode
    * will run <code>signal_vertices()</code> on all vertices,
    * signalling all vertices with value <= 1
    *
    * ### Relations
    * The map function has the same structure as that in
    * add_vertex_aggregator() and may be reused in an aggregator.
    * This function is also very similar to 
    * graphlab::distributed_graph::map_reduce_vertices()
    * with the difference that this takes a context and thus
    * can be used to perform signalling.
    * Finally transform_vertices() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam VertexMapperType The type of the map function. 
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param mapfunction The map function to use. Must take an
    *                   \ref icontext_type& as its first argument, and
    *                   a \ref vertex_type, or a reference to a 
    *                   \ref vertex_type as its second argument.
    *                   Returns a ReductionType which must be summable
    *                   and \ref sec_serializable .
    */
    template <typename ReductionType, typename VertexMapperType>
    ReductionType map_reduce_vertices(VertexMapperType mapfunction) {
      aggregator_type* aggregator = get_aggregator();
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));

      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!"
                             << std::endl;
        return ReductionType(); // does not return
      }
      return aggregator->template map_reduce_vertices<ReductionType>(mapfunction);      
    }

   /**
    * \brief Performs a map-reduce operation on each edge in the 
    * graph returning the result.
    * 
    * Given a map function, map_reduce_edges() call the map function on all
    * edges in the graph. The return values are then summed together and the
    * final result returned. The map function should only read data
    * and should not make any modifications. map_reduce_edges() must be
    * called on all machines simultaneously.
    *
    * ### Basic Usage 
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    * 
    * To compute an absolute sum over all the edge data, we would write
    * a function which reads in each a edge, and returns the absolute
    * value of the data on the edge.
    * \code
    * float absolute_edge_data(engine_type::icontext_type& context,
    *                          graph_type::edge_type edge) {
    *   return std::fabs(edge.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = engine.map_reduce_edges<float>(absolute_edge_data);
    * \endcode
    * will call the <code>absolute_edge_data()</code> function
    * on each edge in the graph. <code>absolute_edge_data()</code>
    * reads the value of the edge and returns the absolute result.
    * This return values are then summed together and returned. 
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the mapfunction.
    *
    * ### Signalling
    * Another common use for the map_reduce_edges() function is 
    * in signalling. Since the map function is passed a context, it
    * can be used to perform signalling of edges for execution
    * during a later \ref start() "engine.start()" call.
    *
    * For instance, the following code will signal the source
    * vertex of each edge.
    * \code
    * graphlab::empty signal_source(engine_type::icontext_type& context,
    *                               graph_type::edge_type edge) {
    *   context.signal(edge.source());
    *   return graphlab::empty()
    * }
    * \endcode
    * Note that in this case, we are not interested in a reduction
    * operation, and thus we return a graphlab::empty object.
    * Calling:
    * \code
    * engine.map_reduce_edges<graphlab::empty>(signal_source);
    * \endcode
    * will run <code>signal_source()</code> on all edges,
    * signalling all source vertices.
    *
    * ### Relations
    * The map function has the same structure as that in
    * add_edge_aggregator() and may be reused in an aggregator.
    * This function is also very similar to 
    * graphlab::distributed_graph::map_reduce_edges()
    * with the difference that this takes a context and thus
    * can be used to perform signalling.
    * Finally transform_edges() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam EdgeMapperType The type of the map function. 
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param mapfunction The map function to use. Must take an
    *                   \ref icontext_type& as its first argument, and
    *                   a \ref edge_type, or a reference to a 
    *                   \ref edge_type as its second argument.
    *                   Returns a ReductionType which must be summable
    *                   and \ref sec_serializable .
    */
    template <typename ReductionType, typename EdgeMapperType>
    ReductionType map_reduce_edges(EdgeMapperType mapfunction) {
      aggregator_type* aggregator = get_aggregator();
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!" 
                             << std::endl;
        return ReductionType(); // does not return
      }
      return aggregator->template map_reduce_edges<ReductionType>(mapfunction);      
    }
    
   
    /**
     * \brief Performs a transformation operation on each vertex in the graph.
     *
     * Given a mapfunction, transform_vertices() calls mapfunction on 
     * every vertex in graph. The map function may make modifications
     * to the data on the vertex. transform_vertices() must be called by all
     * machines simultaneously.
     *
     * ### Basic Usage 
     * For instance, if the graph has integer vertex data, and integer edge
     * data: 
     * \code
     *   typedef graphlab::distributed_graph<size_t, size_t> graph_type;
     * \endcode
     * 
     * To set each vertex value to be the number of out-going edges,
     * we may write the following function:     
     * \code
     * void set_vertex_value(engine_type::icontext_type& context,
     *                          graph_type::vertex_type vertex) {
     *   vertex.data() = vertex.num_out_edges();
     * }
     * \endcode
     *
     * Calling transform_vertices():
     * \code
     *   engine.transform_vertices(set_vertex_value);
     * \endcode
     * will run the <code>set_vertex_value()</code> function
     * on each vertex in the graph, setting its new value. 
     *
     * ### Signalling
     * Since the mapfunction is provided with a context, the mapfunction
     * can also be used to perform signalling. For instance, the 
     * <code>set_vertex_value</code> function above may be modified to set 
     * the value of the vertex, but to also signal the vertex if
     * it has more than 5 outgoing edges.
     *
     * \code
     * void set_vertex_value(engine_type::icontext_type& context,
     *                          graph_type::vertex_type vertex) {
     *   vertex.data() = vertex.num_out_edges();
     *   if (vertex.num_out_edges() > 5) context.signal(vertex);
     * }
     * \endcode
     *
     * However, if the purpose of the function is to only signal
     * without making modifications, map_reduce_vertices() will be
     * more efficient as this function will additionally perform
     * distributed synchronization of modified data.
     *
     * ### Relations
     * map_reduce_vertices() provide similar signalling functionality, 
     * but should not make modifications to graph data. 
     * graphlab::distributed_graph::transform_vertices() provide
     * the same graph modification capabilities, but without a context
     * and thus cannot perform signalling.
     *
     * \tparam VertexMapperType The type of the map function. 
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \param mapfunction The map function to use. Must take an
     *                   \ref icontext_type& as its first argument, and
     *                   a \ref vertex_type, or a reference to a 
     *                   \ref vertex_type as its second argument.
     *                   Returns void.
     */ 
    template <typename VertexMapperType>
    void transform_vertices(VertexMapperType mapfunction) {
      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!"
                             << std::endl;
        return;  // does not return
      }
      aggregator->transform_vertices(mapfunction);      
    }

    /**
     * \brief Performs a transformation operation on each edge in the graph.
     *
     * Given a mapfunction, transform_edges() calls mapfunction on 
     * every edge in graph. The map function may make modifications
     * to the data on the edge. transform_edges() must be called on
     * all machines simultaneously.
     *
     * ### Basic Usage 
     * For instance, if the graph has integer vertex data, and integer edge
     * data: 
     * \code
     *   typedef graphlab::distributed_graph<size_t, size_t> graph_type;
     * \endcode
     * 
     * To set each edge value to be the number of out-going edges
     * of the target vertex, we may write the following:
     * \code
     * void set_edge_value(engine_type::icontext_type& context,
     *                          graph_type::edge_type edge) {
     *   edge.data() = edge.target().num_out_edges();
     * }
     * \endcode
     *
     * Calling transform_edges():
     * \code
     *   engine.transform_edges(set_edge_value);
     * \endcode
     * will run the <code>set_edge_value()</code> function
     * on each edge in the graph, setting its new value. 
     *
     * ### Signalling
     * Since the mapfunction is provided with a context, the mapfunction
     * can also be used to perform signalling. For instance, the 
     * <code>set_edge_value</code> function above may be modified to set 
     * the value of the edge, but to also signal the target vertex. 
     *
     * \code
     * void set_edge_value(engine_type::icontext_type& context,
     *                          graph_type::edge_type edge) {
     *   edge.data() = edge.target().num_out_edges();
     *   context.signal(edge.target());
     * }
     * \endcode
     *
     * However, if the purpose of the function is to only signal
     * without making modifications, map_reduce_edges() will be
     * more efficient as this function will additionally perform
     * distributed synchronization of modified data.
     *
     * ### Relations
     * map_reduce_edges() provide similar signalling functionality, 
     * but should not make modifications to graph data. 
     * graphlab::distributed_graph::transform_edges() provide
     * the same graph modification capabilities, but without a context
     * and thus cannot perform signalling.
     *
     * \tparam EdgeMapperType The type of the map function. 
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \param mapfunction The map function to use. Must take an
     *                   \ref icontext_type& as its first argument, and
     *                   a \ref edge_type, or a reference to a 
     *                   \ref edge_type as its second argument.
     *                   Returns void.
     */ 
    template <typename EdgeMapperType>
    void transform_edges(EdgeMapperType mapfunction) {
      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!" 
                             << std::endl;
        return; // does not return
      }
      aggregator->transform_edges(mapfunction);      
    }
    
    /**
     * \brief Requests that a particular aggregation key
     * be recomputed periodically when the engine is running.
     *
     * Requests that the aggregator with a given key be aggregated
     * every certain number of seconds when the engine is running.
     * Note that the period is prescriptive: in practice the actual
     * period will be larger than the requested period. 
     * Seconds must be >= 0;
     *
     * For instance, the following code will schedule the aggregator
     * with the name "absolute_vertex_sum" to run every 1.5 seconds.
     * \code
     * engine.aggregate_periodic("absolute_vertex_sum", 1.5);
     * \endcode
     *
     * \param [in] key Key to schedule. Must be a key
     *                 previously created by add_vertex_aggregator()
     *                 or add_edge_aggregator().
     * \param [in] seconds How frequently to schedule. Must be >=
     *    0. seconds == 0 will ensure that this key is continously
     *    recomputed.
     * 
     * All machines must call simultaneously.
     * \return Returns true if key is found and seconds >= 0,
     *         and false otherwise.
     */
    bool aggregate_periodic(const std::string& key, float seconds) {
      aggregator_type* aggregator = get_aggregator();
      if(aggregator == NULL) {
        logstream(LOG_FATAL) << "Aggregation not supported by this engine!" 
                             << std::endl;
        return false; // does not return 
      }
      return aggregator->aggregate_periodic(key, seconds);
    } // end of aggregate_periodic



    /**
     * \cond GRAPHLAB_INTERNAL
     * \internal
     * \brief This is used by iengine to get the 
     * \ref distributed_aggregator from the derived class to support
     * the local templated aggregator interface. 
     *
     * \return a pointer to the distributed aggregator for that
     * engine. If no aggregator is available or aggregation is not
     * supported then return NULL.
     */
    virtual aggregator_type* get_aggregator() = 0;
     /// \endcond
  }; // end of iengine interface

} // end of namespace graphlab

#endif

