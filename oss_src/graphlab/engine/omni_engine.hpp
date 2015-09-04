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


#ifndef GRAPHLAB_OMNI_ENGINE_HPP
#define GRAPHLAB_OMNI_ENGINE_HPP



#include <graphlab/options/graphlab_options.hpp>

#include <graphlab/engine/iengine.hpp>
#include <graphlab/engine/synchronous_engine.hpp>
#include <graphlab/engine/async_consistent_engine.hpp>

namespace graphlab {


  /**
   * \ingroup engines
   * 
   * \brief The omni engine encapsulates all the GraphLab engines
   * allowing the user to select which engine to use at runtime. 
   *
   * The actual engine type is set as a string argument to the
   * constructor of the omni_engine.  Forexample:
   *
   * \code
   * std::string exec_model = "synchronous";
   * // do something to determine the exec_model (possibly command
   * // line processing)
   * // Create the engine
   * graphlab::omni_engine<pagerank_vprog> engine(dc, graph, opts, exec_model);
   * \endcode
   *
   * The specific engine type can be overriden by command line
   * arguments (engine_opts="type=<type>"):
   *
   * \code
   * graphlab::omni_engine<pagerank_vprog> engine(dc, graph, opts, "synchronous");
   * \endcode
   *
   * then calling the progam with the command line options:
   * 
   \verbatim
   %> mpiexec -n 16 ./pagerank --engine_opts="type=synchronous"
   \endverbatim
   * 
   * The currently supproted types are:
   * 
   *  \li "synchronous" or "sync": uses the synchronous engine 
   *  (\ref synchronous_engine)
   *  \li "asynchronous" or "async": uses the asynchronous engine
   *  (\ref async_consistent_engine)
*
   * \see graphlab::synchronous_engine
   * \see graphlab::async_consistent_engine
   *
   */
  template<typename VertexProgram>
  class omni_engine : public iengine<VertexProgram> {
  public:
    /** \brief The type of the iengine */
    typedef iengine<VertexProgram> iengine_type;

    /**
     * \brief The user defined vertex program type which should extend
     * ivertex_program.
     */
    typedef VertexProgram vertex_program_type;

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
     * \brief The user defined type returned by the gather function.
     *
     * The gather type is defined in the \ref graphlab::ivertex_program
     * interface and is the value returned by the
     * \ref graphlab::ivertex_program::gather function.  The
     * gather type must have an <code>operator+=(const gather_type&
     * other)</code> function and must be \ref sec_serializable.
     */
    typedef typename VertexProgram::gather_type gather_type;

    /**
     * \brief The vertex identifier type defined in 
     * \ref graphlab::vertex_id_type.
     */
    typedef typename graph_type::vertex_id_type vertex_id_type;  

    /**
     * \brief The type of the distributed aggregator used by each engine to
     * implement distributed aggregation.
     */   
    typedef typename iengine_type::aggregator_type aggregator_type;    

    /**
     * \brief the type of synchronous engine
     */
    typedef synchronous_engine<VertexProgram> synchronous_engine_type;

    /**
     * \brief the type of asynchronous engine
     */
    typedef async_consistent_engine<VertexProgram> async_consistent_engine_type;



  private:

    /**
     * \brief A pointer to the actual engine in use.
     */
    iengine_type* engine_ptr;

    /**
     * \brief omni engines are not default constructible
     */
    omni_engine() { }

    /**
     * \brief omni engines are not copyable
     */
    omni_engine(const omni_engine& other ) { }


  public:

    /**
     * \brief Construct an omni engine for a given graph with the
     * default_engine_type unless the engine options contain an
     * alternative type.
     *
     * \param [in] dc a distributed control object that is used to
     * connect this engine with it's counter parts on other machines.
     * \param [in,out] graph the graph object that this engine will
     * transform.
     * \param [in] options the command line options which are used to
     * configure the engine.  Note that the engine option "type" can
     * be used to select the engine to use (synchronous or
     * asynchronous).
     * \param [in] default_engine_type The user must specify what
     * engine type to use if no command line option is given.
     */
    omni_engine(distributed_control& dc, graph_type& graph,
                const std::string& default_engine_type,
                const graphlab_options& options = graphlab_options()) :
      engine_ptr(NULL) {
      graphlab_options new_options = options;
      std::string engine_type = default_engine_type;
      options_map& engine_options = new_options.get_engine_args();
      if(engine_options.get_option("type", engine_type)) {
        // the engine option was set so use it instead
        // clear from the options map
        engine_options.options.erase("type");
      }
      // Process the engine types
      if(engine_type == "sync" || engine_type == "synchronous") {
        logstream(LOG_INFO) << "Using the Synchronous engine." << std::endl;
        engine_ptr = new synchronous_engine_type(dc, graph, new_options);
      } else if(engine_type == "async" || engine_type == "asynchronous") {
        logstream(LOG_INFO) << "Using the Synchronous engine." << std::endl;
        engine_ptr = new async_consistent_engine_type(dc, graph, new_options);
      } else {
        logstream(LOG_FATAL) << "Invalid engine type: " << engine_type << std::endl;
      }
    } // end of constructor

    /**
     * \brief Destroy the internal engine destroying all vertex
     * programs associated with this engine.
     */
    ~omni_engine() {
      if(engine_ptr != NULL) {
        delete engine_ptr; engine_ptr = NULL;
      }
    } // end of destructor

    execution_status::status_enum start( ) { return engine_ptr->start(); }

    size_t num_updates() const { return engine_ptr->num_updates(); }
    float elapsed_seconds() const { return engine_ptr->elapsed_seconds(); }
    int iteration() const { return engine_ptr->iteration(); }
    void signal(vertex_id_type vertex,
                const message_type& message = message_type()) {
      engine_ptr->signal(vertex, message);
    }
    void signal_all(const message_type& message = message_type(),
                    const std::string& order = "shuffle") {
      engine_ptr->signal_all(message, order);
    }
    void signal_vset(const vertex_set& vset,
                     const message_type& message = message_type(),
                     const std::string& order = "shuffle") {
      engine_ptr->signal_vset(vset, message, order);
    }


    aggregator_type* get_aggregator() { return engine_ptr->get_aggregator(); }


  }; // end of omni_engine




}; // end of namespace graphlab

#endif

