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


#ifndef GRAPHLAB_ICONTEXT_HPP
#define GRAPHLAB_ICONTEXT_HPP

#include <set>
#include <vector>
#include <cassert>
#include <iostream>

#include <graphlab/macros_def.hpp>
namespace graphlab {



  /**
   * \brief The context object mediates the interaction between the
   * vertex program and the graphlab execution environment.
   *
   * Each of the vertex program (see \ref ivertex_program) methods is
   * passed a reference to the engine's context.  The context allows
   * vertex programs to access information about the current execution
   * and send information (through icontext::signal,
   * icontext::post_delta, and icontext::clear_gather_cache) to the
   * graphlab engines (see \ref iengine).
   *
   * \tparam GraphType the type of graph (typically \ref distributed_graph)
   * \tparam GatherType the user defined gather type (see 
   * \ref ivertex_program::gather_type).
   * \tparam MessageType the user defined message type (see 
   * \ref ivertex_program::message_type).
   */
  template<typename GraphType,
           typename GatherType, 
           typename MessageType>
  class icontext {
  public:
    // Type members ===========================================================
    
    /**
     * \brief the user graph type (typically \ref distributed_graph)
     */
    typedef GraphType graph_type;   

    /**
     * \brief the opaque vertex_type defined in the ivertex_program::graph_type
     * (typically distributed_graph::vertex_type)
     */
    typedef typename graph_type::vertex_type vertex_type;

    /**
     * \brief the global vertex identifier (see
     * graphlab::vertex_id_type).
     */
    typedef typename graph_type::vertex_id_type vertex_id_type;

    /**
     * The message type specified by the user-defined vertex-program.
     * (see ivertex_program::message_type)
     */
    typedef MessageType message_type;

    /**
     * The type returned by the gather operation.  (see
     * ivertex_program::gather_type)
     */
    typedef GatherType gather_type;

   
  public:        
    /** \brief icontext destructor */
    virtual ~icontext() { }
    
    /**
     * \brief Get the total number of vertices in the graph.
     *
     * \return the total number of vertices in the entire graph.
     */
    virtual size_t num_vertices() const { return 0; }

    /**
     * \brief Get the number of edges in the graph.
     *
     * Each direction counts as a separate edge.
     *
     * \return the total number of edges in the entire graph.
     */
    virtual size_t num_edges() const { return 0; }

    /**
     * \brief Get the id of this process.
     *
     * The procid is a number between 0 and 
     * \ref graphlab::icontext::num_procs
     * 
     * \warning Each process may have many threads
     *
     * @return the process of this machine.
     */
    virtual size_t procid() const { return 0; }

    /**
     * \brief Returns a standard output object (like cout)
     *        which only prints once even when running distributed.
     * 
     * This returns a C++ standard output stream object
     * which maps directly to std::cout on machine with 
     * process ID 0, and to empty output streamss
     * on all other processes. Calling,
     * \code
     *   context.cout() << "Hello World!";
     * \endcode
     * will therefore only print if the code is run on machine 0.
     * This is useful in the finalize operation in aggregators.
     */
    virtual std::ostream& cout() const { return std::cout; }

    /**
     * \brief Returns a standard error object (like cerr)
     *        which only prints once even when running distributed.
     * 
     * This returns a C++ standard output stream object
     * which maps directly to std::cerr on machine with 
     * process ID 0, and to empty output streamss
     * on all other processes. Calling,
     * \code
     *   context.cerr() << "Hello World!";
     * \endcode
     * will therefore only print if the code is run on machine 0.
     * This is useful in the finalize operation in aggregators.
     */
    virtual std::ostream& cerr() const { return std::cerr; } 

    /**
     * \brief Get the number of processes in the current execution.
     *
     * This is typically the number of mpi jobs created:
     * \code
     * %> mpiexec -n 16 ./pagerank
     * \endcode
     * would imply that num_procs() returns 16.
     *
     * @return the number of processes in the current execution
     */
    virtual size_t num_procs() const { return 0; }

    /**
     * \brief Get the elapsed time in seconds since start was called.
     * 
     * \return runtine in seconds
     */
    virtual float elapsed_seconds() const { return 0.0; }

    /**
     * \brief Return the current interation number (if supported).
     *
     * \return the current interation number if support or -1
     * otherwise.
     */
    virtual int iteration() const { return -1; } 

    /**
     * \brief Signal the engine to stop executing additional update
     * functions.
     *
     * \warning The execution engine will stop *eventually* and
     * additional update functions may be executed prior to when the
     * engine stops. For-example the synchronous engine (see \ref
     * synchronous_engine) will complete the current super-step before
     * terminating.
     */
    virtual void stop() { } 

    /**
     * \brief Signal a vertex with a particular message.
     *
     * This function is an essential part of the GraphLab abstraction
     * and is used to encode iterative computation. Typically a vertex
     * program will signal neighboring vertices during the scatter
     * phase.  A vertex program may choose to signal neighbors on when
     * changes made during the previos phases break invariants or warrant
     * future computation on neighboring vertices.
     * 
     * The signal function takes two arguments. The first is mandatory
     * and specifies which vertex to signal.  The second argument is
     * optional and is used to send a message.  If no message is
     * provided then the default message is used.
     *
     * \param vertex [in] The vertex to send the message to
     * \param message [in] The message to send, defaults to message_type(). 
     */
    virtual void signal(const vertex_type& vertex, 
                        const message_type& message = message_type()) { }

    /**
     * \brief Send a message to a vertex ID.
     *
     * \warning This function will be slow since the current machine
     * do not know the location of the vertex ID.  If possible use the
     * the icontext::signal call instead.
     *
     * \param gvid [in] the vertex id of the vertex to signal
     * \param message [in] the message to send to that vertex, 
     * defaults to message_type().
     */
    virtual void signal_vid(vertex_id_type gvid, 
                            const message_type& message = message_type()) { }

    /**
     * \brief Post a change to the cached sum for the vertex
     * 
     * Often a vertex program will be signaled due to a change in one
     * or a few of its neighbors.  However the gather operation will
     * be rerun on all neighbors potentially producing the same value
     * as previous invocations and wasting computation time.  To
     * address this some engines support caching (see \ref
     * gather_caching for details) of the gather phase.
     *
     * When caching is enabled the engines save a copy of the previous
     * gather for each vertex.  On subsequent calls to gather if their
     * is a cached gather then the gather phase is skipped and the
     * cached value is passed to the ivertex_program::apply function.
     * Therefore it is the responsibility of the vertex program to
     * update the cache values for neighboring vertices. This is
     * accomplished by using the icontext::post_delta function.
     * Posted deltas are atomically added to the cache.
     *
     * \param vertex [in] the vertex whose cache we want to update
     * \param delta [in] the change that we want to *add* to the
     * current cache.
     *
     */
    virtual void post_delta(const vertex_type& vertex, 
                            const gather_type& delta) { } 

    /**
     * \brief Invalidate the cached gather on the vertex.
     *
     * When caching is enabled clear_gather_cache clears the cache
     * entry forcing a complete invocation of the subsequent gather.
     *
     * \param vertex [in] the vertex whose cache to clear.
     */
    virtual void clear_gather_cache(const vertex_type& vertex) { } 

  }; // end of icontext
  
} // end of namespace
#include <graphlab/macros_undef.hpp>

#endif

