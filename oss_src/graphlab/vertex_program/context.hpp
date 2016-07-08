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


#ifndef GRAPHLAB_CONTEXT_HPP
#define GRAPHLAB_CONTEXT_HPP

#include <set>
#include <vector>
#include <cassert>

#include <graphlab/macros_def.hpp>
namespace graphlab {



  /**
   * \brief The context object mediates the interaction between the
   * vertex program and the graphlab execution environment and
   * implements the \ref icontext interface.
   *
   * \tparam Engine the engine that is using this context.
   */
  template<typename Engine>
  class context : 
    public icontext<typename Engine::graph_type,
                    typename Engine::gather_type,
                    typename Engine::message_type> {
  public:
    // Type members ===========================================================
    /** The engine that created this context object */
    typedef Engine engine_type;

    /** The parent type */
    typedef icontext<typename Engine::graph_type,
                     typename Engine::gather_type,
                     typename Engine::message_type> icontext_type;
    typedef typename icontext_type::graph_type graph_type;
    typedef typename icontext_type::vertex_id_type vertex_id_type;
    typedef typename icontext_type::vertex_type vertex_type;   
    typedef typename icontext_type::message_type message_type;
    typedef typename icontext_type::gather_type gather_type;



  private:
    /** A reference to the engine that created this context */
    engine_type& engine;
    /** A reference to the graph that is being operated on by the engine */
    graph_type& graph;
       
  public:        
    /** 
     * \brief Construct a context for a particular engine and graph pair.
     */
    context(engine_type& engine, graph_type& graph) : 
      engine(engine), graph(graph) { }

    size_t num_vertices() const { return graph.num_vertices(); }

    /**
     * Get the number of edges in the graph
     */
    size_t num_edges() const { return graph.num_edges(); }

    // /**
    //  * Get an estimate of the number of update functions executed up
    //  * to this point.
    //  */
    // size_t num_updates() const { return engine.num_updates(); }

    size_t procid() const { return graph.procid(); }
      
    size_t num_procs() const { return graph.numprocs(); }

    std::ostream& cout() const {
      return graph.dc().cout();
    }

    std::ostream& cerr() const {
      return graph.dc().cerr();
    }

    /**
     * Get the elapsed time in seconds
     */
    float elapsed_seconds() const { return engine.elapsed_seconds(); }

    /**
     * Return the current interation number (if supported).
     */
    int iteration() const { return engine.iteration(); }

    /**
     * Force the engine to stop executing additional update functions.
     */
    void stop() { engine.internal_stop(); }

    /**
     * Send a message to a vertex.
     */
    void signal(const vertex_type& vertex, 
                const message_type& message = message_type()) {
      engine.internal_signal(vertex, message);
    }

    /**
     * Send a message to an arbitrary vertex ID.
     * \warning If sending to neighboring vertices, the \ref signal()
     * function is more efficientas it permits sender side message combining.
     */
    void signal_vid(vertex_id_type vid, 
                    const message_type& message = message_type()) {
      engine.internal_signal_gvid(vid, message);
    }


    /**
     * Post a change to the cached sum for the vertex
     */
    void post_delta(const vertex_type& vertex, 
                    const gather_type& delta) {
      engine.internal_post_delta(vertex, delta);
    }

    /**
     * Invalidate the cached gather on the vertex.
     */
    virtual void clear_gather_cache(const vertex_type& vertex) { 
      engine.internal_clear_gather_cache(vertex);      
    }


                                                

  }; // end of context
  
} // end of namespace
#include <graphlab/macros_undef.hpp>

#endif

