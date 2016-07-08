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




#ifndef GRAPHLAB_WARP_ENGINE
#define GRAPHLAB_WARP_ENGINE

#include <deque>
#include <boost/bind.hpp>
#include <graphlab/util/empty.hpp>
#include <graphlab/scheduler/ischeduler.hpp>
#include <graphlab/scheduler/scheduler_factory.hpp>
#include <graphlab/scheduler/get_message_priority.hpp>
#include <graphlab/engine/iengine.hpp>
#include <graphlab/engine/execution_status.hpp>
#include <graphlab/options/graphlab_options.hpp>
#include <rpc/dc_dist_object.hpp>
#include <graphlab/engine/distributed_chandy_misra.hpp>
#include <graphlab/engine/message_array.hpp>
#include <serialization/serialize_to_from_string.hpp>
#include <perf/tracepoint.hpp>
#include <perf/memory_info.hpp>
#include <graphlab/util/generics/conditional_addition_wrapper.hpp>
#include <rpc/distributed_event_log.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_control.hpp>
#include <fiber/fiber_async_consensus.hpp>
#include <graphlab/aggregation/distributed_aggregator.hpp>
#include <fiber/fiber_remote_request.hpp>
#include <graphlab/engine/warp_event_log.hpp>
#include <graphlab/macros_def.hpp>



namespace graphlab {

namespace warp {

  /**
   * \ingroup warp
   *
   * \brief The warp engine executed update functions
   * asynchronously and can ensure mutual exclusion such that adjacent vertices
   * are never executed simultaneously. The default mode is "factorized"
   * consistency in which only individual gathers/applys/
   * scatters are guaranteed to be consistent, but this can be strengthened to
   * provide full mutual exclusion.
   *
   * ### Execution Semantics
   * The update function is a simple user defined function of the type
   *
   * \code
   * void update_function(engine_type::context& context,
   *                      graph_type::vertex_type vertex) {
   * }
   * \endcode
   *
   * Based on a scheduler, update functions are executed on each scheduled 
   * vertex. All computation is performed from within fine-grained threads
   * called fibers, which allows to create thousands of such fibers, thus
   * hiding distributed communication latency.
   *
   * Within the update function, All blocking warp functions such as 
   * \ref graphlab::warp::map_reduce_neighborhood, 
   * \ref graphlab::warp::transform_neighborhood, and 
   * \ref graphlab::warp::broadcast_neighborhood
   * can be used to make changes to the graph data, and to schedule other 
   * vertices for computation.
   *
   * The engine stops when the scheduler is empty.
   *
   * ### Construction
   *
   * The warp engine is constructed by passing in a
   * \ref graphlab::distributed_control object which manages coordination
   * between engine threads and a \ref graphlab::distributed_graph object
   * which is the graph on which the engine should be run.  
   * 
   * Computation is initiated by signaling vertices using either
   * \ref graphlab::warp_engine::signal or
   * \ref graphlab::warp_engine::signal_all.  In either case all
   * machines should invoke signal or signal all at the same time.  Finally,
   * computation is initiated by calling the
   * \ref graphlab::warp_engine::start function.
   *
   * \see warp::map_reduce_neighborhood()
   * \see warp::transform_neighborhood()
   * \see warp::broadcast_neighborhood()
   *
   * ### Example Usage
   *
   * The following is a simple example demonstrating how to use the engine:
   * \code
   * #include <graphlab.hpp>
   *
   * struct vertex_data {
   *   // code
   * };
   * struct edge_data {
   *   // code
   * };
   * typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;
   * typedef graphlab::warp_engine<graph_type> engine_type;
   *
   * void pagerank(engine_type::context& context,
   *               graph_type::vertex_type vertex) {
   *   ... 
   * } 
   *
   *
   * int main(int argc, char** argv) {
   *   // Initialize control plain using mpi
   *   graphlab::mpi_tools::init(argc, argv);
   *   graphlab::distributed_control dc;
   *   // Parse command line options
   *   graphlab::command_line_options clopts("PageRank algorithm.");
   *   std::string graph_dir;
   *   clopts.attach_option("graph", graph_dir,
   *                        "The graph file.");
   *   if(!clopts.parse(argc, argv)) {
   *     std::cout << "Error in parsing arguments." << std::endl;
   *     return EXIT_FAILURE;
   *   }
   *   graph_type graph(dc, clopts);
   *   graph.load_structure(graph_dir, "tsv");
   *   graph.finalize();
   *   std::cout << "#vertices: " << graph.num_vertices()
   *             << " #edges:" << graph.num_edges() << std::endl;
   *   engine_type engine(dc, graph, clopts);
   *   engine.set_update_function(pagerank);
   *   engine.signal_all();
   *   engine.start();
   *   std::cout << "Runtime: " << engine.elapsed_time();
   *   graphlab::mpi_tools::finalize();
   * }
   * \endcode
   *
   *
   * <a name=engineopts>Engine Options</a>
   * =========================
   * The warp engine supports several engine options which can
   * be set as command line arguments using \c --engine_opts :
   *
   * \li \b timeout (default: infinity) Maximum time in seconds the engine will
   * run for. The actual runtime may be marginally greater as the engine
   * waits for all threads and processes to flush all active tasks before
   * returning.
   * \li \b factorized (default: true) Set to true to weaken the consistency
   * model to factorized consistency where only individual gather/apply/scatter
   * calls are guaranteed to be locally consistent. Can produce massive
   * increases in throughput at a consistency penalty.
   * \li \b nfibers (default: 10000) Number of fibers to use
   * \li \b stacksize (default: 16384) Stacksize of each fiber.
   */
  template <typename GraphType, typename MessageType = graphlab::empty>
  class warp_engine {

  public:
    /**
     * \brief The user defined message type used to signal neighboring
     * vertex programs.
     */
    typedef MessageType message_type;

    /**
     * The type of the graph associated with this engine.
     */
    typedef GraphType graph_type;

    /**
     * \brief The type of data associated with each vertex in the graph
     *
     * The vertex data type must be \ref sec_serializable.
     */
    typedef typename graph_type::vertex_data_type vertex_data_type;

    /**
     * \brief The type of data associated with each edge in the graph
     *
     * The edge data type must be \ref sec_serializable.
     */
    typedef typename graph_type::edge_data_type edge_data_type;


     /**
     * \brief The type used to represent a vertex in the graph.
     * See \ref graphlab::distributed_graph::vertex_type for details
     *
     * The vertex type contains the function
     * \ref graphlab::distributed_graph::vertex_type::data which
     * returns a reference to the vertex data as well as other functions
     * like \ref graphlab::distributed_graph::vertex_type::num_in_edges
     * which returns the number of in edges.
     *
     */
    typedef typename graph_type::vertex_type          vertex_type;

    /**
     * \brief The type used to represent an edge in the graph.
     * See \ref graphlab::distributed_graph::edge_type for details.
     *
     * The edge type contains the function
     * \ref graphlab::distributed_graph::edge_type::data which returns a
     * reference to the edge data.  In addition the edge type contains
     * the function \ref graphlab::distributed_graph::edge_type::source and
     * \ref graphlab::distributed_graph::edge_type::target.
     *
     */
    typedef typename graph_type::edge_type            edge_type;




    struct context {
      typedef warp_engine engine_type;
      typedef typename engine_type::graph_type graph_type;
      typedef typename graph_type::vertex_type vertex_type;
      typedef typename graph_type::edge_type edge_type;
      typedef typename graph_type::local_vertex_type local_vertex_type;

      warp_engine& engine;
      graph_type& graph;
      std::string original_value;
      vertex_type vtx;
      bool vtx_set;
      message_type message;

      context(warp_engine& engine, graph_type& graph, 
              vertex_type vtx):
          engine(engine), 
          graph(graph), 
          vtx(vtx),
          vtx_set(true) { 
            set_synchronized();
        }
      

      context(warp_engine& engine, graph_type& graph):
          engine(engine), 
          graph(graph), 
          vtx(graph, 0),
          vtx_set(false) { 
        }
      
      /**
       * \brief Get the total number of vertices in the graph.
       *
       * \return the total number of vertices in the entire graph.
       */
      size_t num_vertices() const { return graph.num_vertices(); }

      /**
       * \brief Get the number of edges in the graph.
       *
       * Each direction counts as a separate edge.
       *
       * \return the total number of edges in the entire graph.
       */ 
      size_t num_edges() const { return graph.num_edges(); }

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
      size_t procid() const { return graph.procid(); }

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
      size_t num_procs() const { return graph.numprocs(); }

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
      std::ostream& cout() const {
        return graph.dc().cout();
      }

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

      std::ostream& cerr() const {
        return graph.dc().cerr();
      }

      /**
       * \brief Get the elapsed time in seconds since start was called.
       * 
       * \return runtine in seconds
       */      
      float elapsed_seconds() const { return engine.elapsed_seconds(); }

      /**
       * \brief Return the current interation number (if supported).
       *
       * \return the current interation number if support or -1
       * otherwise.
       */
      int iteration() const { return -1; }

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
      void stop() { engine.internal_stop(); }

      /**
       * \brief Signal a vertex with a particular message.
       *
       * This function is an essential part of the GraphLab abstraction
       * and is used to encode iterative computation. Typically a vertex
       * program will signal neighboring vertices during the scatter
       * phase.  A vertex program may choose to signal neighbors on when
       * changes made during the previous phases break invariants or warrant
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
      void signal(const vertex_type& vertex, 
                  const message_type& message = message_type()) {
        engine.internal_signal(vertex, message);
      }



      /**
       * \brief Signal an arbitrary vertex ID with a particular message.
       *
       * This function is an essential part of the GraphLab abstraction
       * and is used to encode iterative computation. Typically a vertex
       * program will signal neighboring vertices during the scatter
       * phase.  A vertex program may choose to signal neighbors on when
       * changes made during the previous phases break invariants or warrant
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
      void signal(vertex_id_type gvid, 
                  const message_type& message = message_type()) {
        engine.internal_signal_gvid(gvid, message);
      }


      /**
       * \internal
       * \brief Flags that this vertex was synchronized.
       */
      void set_synchronized() {
        if (vtx_set && graph.l_is_master(vtx.local_id())) {
          original_value = serialize_to_string(vtx.data());
        }
      }

      /**
       * \brief Synchronizes all copies of this vertex
       * 
       * If the current vertex value has changed, copy the vertex value to
       * all mirrors. This is for advanced use!
       * Under most circumstances you should not need to use 
       * this function directly.
       */
      void synchronize() {
        if (vtx_set && graph.l_is_master(vtx.local_id())) {
          std::string new_value = serialize_to_string(vtx.data());
          if (original_value != new_value) {
            // synchronize this vertex's value
            engine.synchronize_one_vertex_wait(vtx);
          }
          std::swap(original_value, new_value);
        }
      }

      /**
       * \brief Gets the stored mesage if any
       */
      message_type& get_message() {
        return message;
      }
    };

    /**
     * The type of the context.
     */
    typedef context context_type;

    /// \internal
    typedef context icontext_type;

    /// The type of the update function
    typedef boost::function<void(context_type&, vertex_type)> update_function_type;

  private:
    /// \internal \brief The base type of all schedulers
    message_array<message_type> messages;

    // context needs access to internal functions
    friend struct context;

    /// \internal \brief The type used to refer to vertices in the local graph
    typedef typename graph_type::local_vertex_type    local_vertex_type;
    /// \internal \brief The type used to refer to edges in the local graph
    typedef typename graph_type::local_edge_type      local_edge_type;
    /// \internal \brief The type used to refer to vertex IDs in the local graph
    typedef typename graph_type::lvid_type            lvid_type;

    /// \internal \brief The type of the current engine instantiation
    typedef warp_engine engine_type;

    
    /// The RPC interface
    dc_dist_object<warp_engine> rmi;

    /// A reference to the active graph
    graph_type* graph;

    /// A pointer to the lock implementation
    distributed_chandy_misra<graph_type>* cmlocks;

    /// Per vertex data locks
    std::vector<simple_spinlock> vertexlocks;


    /**
     * \brief A bit indicating if the local gather for that vertex is
     * available.
     */
    dense_bitset has_cache;

    /// Engine threads.
    fiber_group thrgroup;

    //! The scheduler
    ischeduler* scheduler_ptr;

    typedef distributed_aggregator<graph_type, context_type>  aggregator_type;
    aggregator_type aggregator;

    /// Number of kernel threads
    size_t ncpus;
    /// Size of each fiber stack
    size_t stacksize;
    /// Number of fibers
    size_t nfibers;
    /// set to true if engine is started
    bool started;
    /// A pointer to the distributed consensus object
    fiber_async_consensus* consensus;

    /**
     * Used only by the locking subsystem.
     * to allow the fiber to go to sleep when waiting for the locks to
     * be ready.
     */
    struct vertex_fiber_cm_handle {
      mutex lock;
      bool philosopher_ready;
      size_t fiber_handle;
    };
    std::vector<vertex_fiber_cm_handle*> cm_handles;

    dense_bitset program_running;
    dense_bitset hasnext;

    // Various counters.
    atomic<uint64_t> programs_executed;

    timer launch_timer;

    /// Defaults to (-1), defines a timeout
    size_t timed_termination;
 
    /// engine option. Sets to true if factorized consistency is used
    bool factorized_consistency;

    bool endgame_mode;

    /// Time when engine is started
    float engine_start_time;

    /// True when a force stop is triggered (possibly via a timeout)
    bool force_stop;

    graphlab_options opts_copy; // local copy of options to pass to
                                // scheduler construction

    execution_status::status_enum termination_reason;

    std::vector<mutex> aggregation_lock;
    std::vector<std::deque<std::string> > aggregation_queue;

    update_function_type update_fn;
  public:

    /**
     * Constructs an asynchronous consistent distributed engine.
     * The number of threads to create are read from
     * \ref graphlab_options::get_ncpus "opts.get_ncpus()". The scheduler to
     * construct is read from
     * \ref graphlab_options::get_scheduler_type() "opts.get_scheduler_type()".
     * The default scheduler
     * is the queued_fifo scheduler. For details on the scheduler types
     * \see scheduler_types
     *
     *  See the <a href=#engineopts> main class documentation</a> for the
     *  available engine options.
     *
     * \param dc Distributed controller to associate with
     * \param graph The graph to schedule over. The graph must be fully
     *              constructed and finalized.
     * \param opts A graphlab::graphlab_options object containing options and
     *             parameters for the scheduler and the engine.
     */
    warp_engine(distributed_control &dc,
                            graph_type& graph,
                            const graphlab_options& opts = graphlab_options()) :
        rmi(dc, this), graph(&graph), scheduler_ptr(NULL),
        aggregator(dc, graph, new context_type(*this, graph)), started(false),
        engine_start_time(timer::approx_time_seconds()), force_stop(false) {
      rmi.barrier();
      initialize_counters();

      nfibers = 10000;
      stacksize = 16384;
      factorized_consistency = true;
      update_fn = NULL;
      timed_termination = (size_t)(-1);
      termination_reason = execution_status::UNSET;
      set_options(opts);
      init();
      rmi.barrier();
    }


    warp_engine(distributed_control &dc,
                const graphlab_options& opts = graphlab_options()) :
        rmi(dc, this), graph(NULL), scheduler_ptr(NULL),
        aggregator(dc, NULL), started(false),
        engine_start_time(timer::approx_time_seconds()), force_stop(false) {
      rmi.barrier();
      initialize_counters();

      nfibers = 10000;
      stacksize = 16384;
      factorized_consistency = true;
      update_fn = NULL;
      timed_termination = (size_t)(-1);
      termination_reason = execution_status::UNSET;
      set_options(opts);
      rmi.barrier();
    }




    /** \internal
     * For the warp engine to find the remote instances of this class
     */
    size_t get_rpc_obj_id() {
      return rmi.get_obj_id();
    }

  private:

    /**
     * \internal
     * Configures the engine with the provided options.
     * The number of threads to create are read from
     * opts::get_ncpus(). The scheduler to construct is read from
     * graphlab_options::get_scheduler_type(). The default scheduler
     * is the queued_fifo scheduler. For details on the scheduler types
     * \see scheduler_types
     */
    void set_options(const graphlab_options& opts) {
      rmi.barrier();
      ncpus = opts.get_ncpus();
      ASSERT_GT(ncpus, 0);
      aggregation_lock.resize(opts.get_ncpus());
      aggregation_queue.resize(opts.get_ncpus());
      std::vector<std::string> keys = opts.get_engine_args().get_option_keys();
      foreach(std::string opt, keys) {
        if (opt == "timeout") {
          opts.get_engine_args().get_option("timeout", timed_termination);
          if (rmi.procid() == 0)
            logstream(LOG_EMPH) << "Engine Option: timeout = " << timed_termination << std::endl;
        } else if (opt == "factorized") {
          opts.get_engine_args().get_option("factorized", factorized_consistency);
          if (rmi.procid() == 0)
            logstream(LOG_EMPH) << "Engine Option: factorized = " << factorized_consistency << std::endl;
        } else if (opt == "nfibers") {
          opts.get_engine_args().get_option("nfibers", nfibers);
          if (rmi.procid() == 0)
            logstream(LOG_EMPH) << "Engine Option: nfibers = " << nfibers << std::endl;
        } else if (opt == "stacksize") {
          opts.get_engine_args().get_option("stacksize", stacksize);
          if (rmi.procid() == 0)
            logstream(LOG_EMPH) << "Engine Option: stacksize= " << stacksize << std::endl;
        } else {
          logstream(LOG_FATAL) << "Unexpected Engine Option: " << opt << std::endl;
        }
      }
      opts_copy = opts;
      // set a default scheduler if none
      if (opts_copy.get_scheduler_type() == "") {
        opts_copy.set_scheduler_type("queued_fifo");
      }

      // construct scheduler passing in the copy of the options from set_options
      if (graph) {
        scheduler_ptr = scheduler_factory::
            new_scheduler(graph->num_local_vertices(),
                          opts_copy);
      } else {
        scheduler_ptr = scheduler_factory::
            new_scheduler(1,
                          opts_copy);
      }
      rmi.barrier();

      // create initial fork arrangement based on the alternate vid mapping
      if (factorized_consistency == false) {
        if (graph) {
          cmlocks = new distributed_chandy_misra<graph_type>(rmi.dc(), *graph,
                                                             boost::bind(&engine_type::lock_ready, this, _1));
        } else {
          cmlocks = new distributed_chandy_misra<graph_type>(rmi.dc(), 
                                                             boost::bind(&engine_type::lock_ready, this, _1));
        }
                                                    
      }
      else {
        cmlocks = NULL;
      }

      // construct the termination consensus object
      consensus = new fiber_async_consensus(rmi.dc(), nfibers);
    }


    /**
     * \brief Resizes the engine's internal datastructures to match the graph.
     * Clears all messages. Must be called before signaling functions if the
     * size of the graph is changed.
     */
    void init() {
      if (graph) {
        // construct all the required datastructures
        // deinitialize performs the reverse
        graph->finalize();
        scheduler_ptr->set_num_vertices(graph->num_local_vertices());
        messages.resize(graph->num_local_vertices());
        vertexlocks.resize(graph->num_local_vertices());
        program_running.resize(graph->num_local_vertices());
        hasnext.resize(graph->num_local_vertices());

        if (!factorized_consistency) {
          cm_handles.resize(graph->num_local_vertices());
        }
      }
      rmi.barrier();
    }




  public:
    ~warp_engine() {
      rmi.dc().full_barrier();
      delete consensus;
      delete cmlocks;
      delete scheduler_ptr;
      rmi.dc().full_barrier();
    }


    /**
     * Sets the update function to use for execution.
     * The update function must be of the type void(context_type&, vertex_type),
     * but more generally, may be a 
     * boost::function<void(context_type&, vertex_type)>
     */
    void set_update_function(update_function_type update_function) {
      update_fn = update_function;
    }


    /**
     * Returns the number of fibers
     */
    size_t get_nfibers() {
      return nfibers;
    }

    /**
     * Sets the number of fibers
     */
    void set_nfibers(size_t new_nfibers) {
      nfibers = new_nfibers;
    }


    /**
     * Returns the stacksize of each fiber
     */
    size_t get_stacksize() {
      return stacksize;
    }

    /**
     * Sets the stacksize of each fiber
     */
    void set_stacksize(size_t new_stacksize) {
      stacksize = new_stacksize;
    }


    /**
     * \brief Associates the engine with a new graph and resizes the engine's
     * internal datastructures to match the graph.  Clears all messages. Must
     * be called before signaling functions if the size of the graph is
     * changed. graph can be NULL.
     */
    void init_with_new_graph(graph_type* new_graph) {
      graph = new_graph;
      if (graph) {
        // construct all the required datastructures
        // deinitialize performs the reverse
        graph->finalize();
        aggregator.init(*graph, new context_type(*this, *graph));
        scheduler_ptr->set_num_vertices(graph->num_local_vertices());
        messages.clear();
        messages.resize(graph->num_local_vertices());
        vertexlocks.resize(graph->num_local_vertices());
        program_running.clear();
        program_running.resize(graph->num_local_vertices());
        hasnext.clear();
        hasnext.resize(graph->num_local_vertices());

        if (!factorized_consistency) {
          cm_handles.resize(graph->num_local_vertices());
          cmlocks->init(*new_graph);
        }
      }
      rmi.barrier();
    }


    /**
     * \brief Compute the total number of updates (calls to apply)
     * executed since start was last invoked.
     *
     * \return Total number of updates
     */
    size_t num_updates() const {
      return programs_executed.value;
    }





    /**
     * \brief Get the elapsed time in seconds since start was last
     * called.
     * 
     * \return elapsed time in seconds
     */
    float elapsed_seconds() const {
      return timer::approx_time_seconds() - engine_start_time;
    }


    /**
     * \brief Not meaningful for the asynchronous engine. Returns -1.
     */
    int iteration() const { return -1; }


/**************************************************************************
 *                           Signaling Interface                          *
 **************************************************************************/

  private:

    /**
     * \internal
     * This is used to receive a message forwarded from another machine
     */
    void rpc_signal(vertex_id_type vid,
                            const message_type& message) {
      if (force_stop) return;
      const lvid_type local_vid = graph->local_vid(vid);
      double priority;
      messages.add(local_vid, message, &priority);
      scheduler_ptr->schedule(local_vid, priority);
      consensus->cancel();
    }

    /**
     * \internal
     * \brief Signals a vertex with an optional message
     *
     * Signals a vertex, and schedules it to be executed in the future.
     * must be called on a vertex accessible by the current machine.
     */
    void internal_signal(const vertex_type& vtx,
                         const message_type& message = message_type()) {
      INCREMENT_EVENT(EVENT_WARP_ENGINE_SIGNAL, 1);
      if (force_stop) return;
      if (started) {
        const typename graph_type::vertex_record& rec = graph->l_get_vertex_record(vtx.local_id());
        const procid_t owner = rec.owner;
        if (endgame_mode) {
          // fast signal. push to the remote machine immediately
          if (owner != rmi.procid()) {
            const vertex_id_type vid = rec.gvid;
            rmi.remote_call(owner, &engine_type::rpc_signal, vid, message);
          }
          else {
            double priority;
            messages.add(vtx.local_id(), message, &priority);
            scheduler_ptr->schedule(vtx.local_id(), priority);
            consensus->cancel();
          }
        }
        else {

          double priority;
          messages.add(vtx.local_id(), message, &priority);
          scheduler_ptr->schedule(vtx.local_id(), priority);
          consensus->cancel();
        }
      }
      else {
        double priority;
        messages.add(vtx.local_id(), message, &priority);
        scheduler_ptr->schedule(vtx.local_id(), priority);
        consensus->cancel();
      }
    } // end of schedule


    /**
     * \internal
     * \brief Signals a vertex with an optional message
     *
     * Signals a global vid, and schedules it to be executed in the future.
     * If current machine does not contain the vertex, it is ignored.
     */
    void internal_signal_gvid(vertex_id_type gvid,
                              const message_type& message = message_type()) {
      if (force_stop) return;
      if (graph->is_master(gvid)) {
        internal_signal(graph->vertex(gvid), message);
      } else {
        procid_t proc = graph->master(gvid);
        rmi.remote_call(proc, &warp_engine::internal_signal_gvid,
                        gvid, message);
      }
    } 



    void rpc_internal_stop() {
      force_stop = true;
      termination_reason = execution_status::FORCED_ABORT;
    }

    /**
     * \brief Force engine to terminate immediately.
     *
     * This function is used to stop the engine execution by forcing
     * immediate termination.
     */
    void internal_stop() {
      for (procid_t i = 0;i < rmi.numprocs(); ++i) {
        rmi.remote_call(i, &warp_engine::rpc_internal_stop);
      }
    }

  public:


    /**
     * \brief Signals single a vertex with an optional message.
     * 
     * This function sends a message to particular vertex which will
     * receive that message on start. The signal function must be
     * invoked on all machines simultaneously.  For example:
     *
     * \code
     * graphlab::warp_engine<graph_type> engine(dc, graph, opts);
     * engine.signal(0); // signal vertex zero
     * \endcode
     *
     * and _not_:
     *
     * \code
     * graphlab::warp_engine<graph_type> engine(dc, graph, opts);
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
    void signal(vertex_id_type gvid,
                const message_type& message = message_type()) {
      rmi.barrier();
      internal_signal_gvid(gvid, message);
      rmi.barrier();
    }

    /**
     * \brief Signal all vertices with a particular message.
     * 
     * This function sends the same message to all vertices which will
     * receive that message on start. The signal_all function must be
     * invoked on all machines simultaneously.  For example:
     *
     * \code
     * graphlab::warp_engine<graph_type> engine(dc, graph, opts);
     * engine.signal_all(); // signal all vertices
     * \endcode
     *
     * and _not_:
     *
     * \code
     * graphlab::warp_engine<graph_type> engine(dc, graph, opts);
     * if(dc.procid() == 0) engine.signal_all(); // signal vertex zero
     * \endcode
     *
     * The signal_all function is the most common way to send messages
     * to the engine.  For example in the pagerank application we want
     * all vertices to be active on the first round.  Therefore we
     * would write:
     *
     * \code
     * graphlab::warp_engine<graph_type> engine(dc, graph, opts);
     * engine.signal_all();
     * engine.start();
     * \endcode
     *
     * @param [in] message the message to send to all vertices.  The
     * default message is sent if no message is provided
     * (See ivertex_program::message_type for details about the
     * message_type). 
     */
    void signal_all(const message_type& message = message_type(),
                    const std::string& order = "shuffle") {
      vertex_set vset = graph->complete_set();
      signal_vset(vset, message, order);
    } // end of schedule all


    /**
     * \brief Signal a set of vertices with a particular message.
     * 
     * This function sends the same message to a set of vertices which will
     * receive that message on start. The signal_vset function must be
     * invoked on all machines simultaneously.  For example:
     *
     * \code
     * graphlab::warp_engine<graph_type> engine(dc, graph, opts);
     * engine.signal_vset(vset); // signal a subset of vertices
     * \endcode
     *
     * signal_all() is conceptually equivalent to:
     *
     * \code
     * engine.signal_vset(graph->complete_set());
     * \endcode
     *
     * @param [in] vset The set of vertices to signal 
     * @param [in] message the message to send to all vertices.  The
     * default message is sent if no message is provided
     * (See ivertex_program::message_type for details about the
     * message_type). 
     */
    void signal_vset(const vertex_set& vset,
                    const message_type& message = message_type(),
                    const std::string& order = "shuffle") {
      logstream(LOG_DEBUG) << rmi.procid() << ": Schedule All" << std::endl;
      // allocate a vector with all the local owned vertices
      // and schedule all of them.
      std::vector<vertex_id_type> vtxs;
      vtxs.reserve(graph->num_local_own_vertices());
      for(lvid_type lvid = 0;
          lvid < graph->get_local_graph().num_vertices();
          ++lvid) {
        if (graph->l_vertex(lvid).owner() == rmi.procid() &&
            vset.l_contains(lvid)) {
          vtxs.push_back(lvid);
        }
      }

      if(order == "shuffle") {
        graphlab::random::shuffle(vtxs.begin(), vtxs.end());
      }
      foreach(lvid_type lvid, vtxs) {
        double priority;
        messages.add(lvid, message, &priority);
        scheduler_ptr->schedule(lvid, priority);
      }
      rmi.barrier();
    }


  private: 

    /**
     * Gets a task from the scheduler and the associated message
     */
    sched_status::status_enum get_next_sched_task(size_t threadid,
                                                  lvid_type& lvid,
                                                  message_type& msg) {
      while (1) {
        sched_status::status_enum stat = 
            scheduler_ptr->get_next(threadid % ncpus, lvid);
        if (stat == sched_status::NEW_TASK) {
          if (messages.get(lvid, msg)) return stat;
          else continue;
        }
        return stat;
      }
    }

    void set_endgame_mode() {
        if (!endgame_mode) logstream(LOG_EMPH) << "Endgame mode\n";
        endgame_mode = true;
        rmi.dc().set_fast_track_requests(true);
    } 

    /**
     * \internal
     * Called when get_a_task returns no internal task not a scheduler task.
     * This rechecks the status of the internal task queue and the scheduler
     * inside a consensus critical section.
     */
    bool try_to_quit(size_t threadid,
                     bool& has_sched_msg,
                     lvid_type& sched_lvid,
                     message_type &msg) {
      if (timer::approx_time_seconds() - engine_start_time > timed_termination) {
        termination_reason = execution_status::TIMEOUT;
        force_stop = true;
      }
      logstream(LOG_DEBUG) << rmi.procid() << "-" << threadid << ": " << "Termination Attempt " << std::endl;
      has_sched_msg = false;
      fiber_control::yield();
      consensus->begin_done_critical_section(threadid);
      sched_status::status_enum stat = 
          get_next_sched_task(threadid, sched_lvid, msg);
      if (stat == sched_status::EMPTY || force_stop) {
        logstream(LOG_DEBUG) << rmi.procid() << "-" << threadid <<  ": "
                             << "\tTermination Double Checked" << std::endl;

        if (!endgame_mode) logstream(LOG_EMPH) << "Endgame mode\n";
        endgame_mode = true;
        // put everyone in endgame
        for (procid_t i = 0;i < rmi.dc().numprocs(); ++i) {
          rmi.remote_call(i, &warp_engine::set_endgame_mode);
        } 
        bool ret = consensus->end_done_critical_section(threadid);
        if (ret == false) {
          logstream(LOG_DEBUG) << rmi.procid() << "-" << threadid <<  ": "
                             << "\tCancelled" << std::endl;
        } else {
          logstream(LOG_DEBUG) << rmi.procid() << "-" << threadid <<  ": "
                             << "\tDying" << " (" << fiber_control::get_tid() << ")" << std::endl;
        }
        return ret;
      } else {
        logstream(LOG_DEBUG) << rmi.procid() << "-" << threadid <<  ": "
                             << "\tCancelled by Scheduler Task" << std::endl;
        consensus->cancel_critical_section(threadid);
        has_sched_msg = true;
        return false;
      }
    } // end of try to quit


    /**
     * \internal
     * When all distributed locks are acquired, this function is called
     * from the chandy misra implementation on the master vertex.
     * Here, we perform initialization
     * of the task and switch the vertex to a gathering state
     */
    void lock_ready(lvid_type lvid) {
      cm_handles[lvid]->lock.lock();
      cm_handles[lvid]->philosopher_ready = true;
      fiber_control::schedule_tid(cm_handles[lvid]->fiber_handle);
      cm_handles[lvid]->lock.unlock();
    }


    // make sure I am the only person running.
    // if returns false, the message has been dropped into the message array.
    // quit
    bool get_exclusive_access_to_vertex(const lvid_type lvid,
                                        const message_type& msg) {
      vertexlocks[lvid].lock();
      bool someone_else_running = program_running.set_bit(lvid);
      if (someone_else_running) {
        // bad. someone else is here.
        // drop it into the message array
        messages.add(lvid, msg);
        hasnext.set_bit(lvid);
      } 
      vertexlocks[lvid].unlock();
      return !someone_else_running;
    }



    // make sure I am the only person running.
    // if returns false, the message has been dropped into the message array.
    // quit
    void release_exclusive_access_to_vertex(const lvid_type lvid) {
      vertexlocks[lvid].lock();
      // someone left a next message for me
      // reschedule it at high priority
      if (hasnext.get(lvid)) {
        scheduler_ptr->schedule(lvid, 10000.0);
        consensus->cancel();
        hasnext.clear_bit(lvid);
      }
      program_running.clear_bit(lvid);
      vertexlocks[lvid].unlock();
    }

    void update_vertex_value(vertex_id_type vid,
                             vertex_data_type& vdata) {
      local_vertex_type lvtx(graph->l_vertex(graph->local_vid(vid)));
      lvtx.data() = vdata;
    }

    void synchronize_one_vertex(vertex_type vtx) {
      local_vertex_type lvtx(vtx);
      foreach(procid_t mirror, lvtx.mirrors()) {
        rmi.remote_call(mirror, &warp_engine::update_vertex_value, vtx.id(), vtx.data());
      }
    }


    void synchronize_one_vertex_wait(vertex_type vtx) {
      local_vertex_type lvtx(vtx);
      std::vector<request_future<void> > futures;
      foreach(procid_t mirror, lvtx.mirrors()) {
        futures.push_back(object_fiber_remote_request(rmi, 
                                                      mirror, 
                                                      &warp_engine::update_vertex_value, 
                                                      vtx.id(), 
                                                      vtx.data()));
      }
      for (size_t i = 0;i < futures.size(); ++i) {
        futures[i]();
      }
    }

    /**
     * \internal
     * Called when the scheduler returns a vertex to run.
     * If this function is called with vertex locks acquired, prelocked
     * should be true. Otherwise it should be false.
     */
    void eval_sched_task(const lvid_type lvid,
                         const message_type& msg) {
      const typename graph_type::vertex_record& rec = graph->l_get_vertex_record(lvid);
      vertex_id_type vid = rec.gvid;
      // if this is another machine's forward it
      if (rec.owner != rmi.procid()) {
        rmi.remote_call(rec.owner, &engine_type::rpc_signal, vid, msg);
        return;
      }
      // I have to run this myself
      
      if (!get_exclusive_access_to_vertex(lvid, msg)) return;

      /**************************************************************************/
      /*                             Acquire Locks                              */
      /**************************************************************************/
      if (!factorized_consistency) {
        // begin lock acquisition
        cm_handles[lvid] = new vertex_fiber_cm_handle;
        cm_handles[lvid]->philosopher_ready = false;
        cm_handles[lvid]->fiber_handle = fiber_control::get_tid();
        cmlocks->make_philosopher_hungry(lvid);
        cm_handles[lvid]->lock.lock();
        while (!cm_handles[lvid]->philosopher_ready) {
          fiber_control::deschedule_self(&(cm_handles[lvid]->lock.m_mut));
          cm_handles[lvid]->lock.lock();
        }
        cm_handles[lvid]->lock.unlock();
      }

      

      local_vertex_type l_vtx(graph->l_vertex(lvid));
      local_vertex_type vtx(l_vtx);


      context ctx(*this, *graph, vtx);
      ctx.message = msg;
      INCREMENT_EVENT(EVENT_WARP_ENGINE_UF_COUNT, 1);
      rdtsc_time time;
      update_fn(ctx, vtx);
      INCREMENT_EVENT(EVENT_WARP_ENGINE_UF_TIME, time.ms());
      ctx.synchronize();
      /************************************************************************/
      /*                           Release Locks                              */
      /************************************************************************/
      // cleanup
      if (!factorized_consistency) {
        cmlocks->philosopher_stops_eating(lvid);
        delete cm_handles[lvid];
        cm_handles[lvid] = NULL;
      }
      release_exclusive_access_to_vertex(lvid);
      programs_executed.inc(); 
    }


    /**
     * \internal
     * Per thread main loop
     */
    void thread_start(size_t threadid) {
      bool has_sched_msg = false;
      std::vector<std::vector<lvid_type> > internal_lvid;
      lvid_type sched_lvid;

      message_type msg;
      float last_aggregator_check = timer::approx_time_seconds();
      timer ti; ti.start();
      while(1) {
        if (timer::approx_time_seconds() != last_aggregator_check && !endgame_mode) {
          last_aggregator_check = timer::approx_time_seconds();
          std::string key = aggregator.tick_asynchronous();
          if (key != "") {
            for (size_t i = 0;i < aggregation_lock.size(); ++i) {
              aggregation_lock[i].lock();
              aggregation_queue[i].push_back(key);
              aggregation_lock[i].unlock();
            }
          }
        }

        // test the aggregator
        while(!aggregation_queue[fiber_control::get_worker_id()].empty()) {
          size_t wid = fiber_control::get_worker_id();
          ASSERT_LT(wid, ncpus);
          aggregation_lock[wid].lock();
          std::string key = aggregation_queue[wid].front();
          aggregation_queue[wid].pop_front();
          aggregation_lock[wid].unlock();
          aggregator.tick_asynchronous_compute(wid, key);
        }

        sched_status::status_enum stat = get_next_sched_task(threadid, sched_lvid, msg);


        has_sched_msg = stat != sched_status::EMPTY;
        if (stat != sched_status::EMPTY) {
          eval_sched_task(sched_lvid, msg);
          if (endgame_mode) rmi.dc().flush();
        }
        else if (!try_to_quit(threadid, has_sched_msg, sched_lvid, msg)) {
          /*
           * We failed to obtain a task, try to quit
           */
          if (has_sched_msg) {
            eval_sched_task(sched_lvid, msg);
          }
        } else { 
          break; 
        }
        if (fiber_control::worker_has_priority_fibers_on_queue()) fiber_control::yield();
      }
    } // end of thread start

/**************************************************************************
 *                         Main engine start()                            *
 **************************************************************************/

  public:


    /**
      * \brief Start the engine execution.
      *
      * This function starts the engine and does not
      * return until the scheduler has no tasks remaining.
      *
      * \return the reason for termination
      */
    execution_status::status_enum start() {
      bool old_fasttrack = rmi.dc().set_fast_track_requests(false);
      logstream(LOG_INFO) << "Spawning " << nfibers << " threads" << std::endl;
      ASSERT_TRUE(scheduler_ptr != NULL);
      consensus->reset();
      consensus->set_nfibers(nfibers);

      // now. It is of critical importance that we match the number of 
      // actual workers
     

      // start the aggregator
      aggregator.start(ncpus);
      aggregator.aggregate_all_periodic();

      started = true;

      rmi.barrier();
      size_t allocatedmem = memory_info::allocated_bytes();
      rmi.all_reduce(allocatedmem);

      engine_start_time = timer::approx_time_seconds();
      force_stop = false;
      endgame_mode = false;
      programs_executed = 0;
      launch_timer.start();

      termination_reason = execution_status::RUNNING;
      if (rmi.procid() == 0) {
        logstream(LOG_INFO) << "Total Allocated Bytes: " << allocatedmem << std::endl;
      }
      fiber_group::affinity_type affinity;
      affinity.clear();
      for (size_t i = 0; i < ncpus; ++i) {
        affinity.set_bit(i);
      }
      thrgroup.set_affinity(affinity);
      thrgroup.set_stacksize(stacksize);

      for (size_t i = 0; i < nfibers ; ++i) {
        thrgroup.launch(boost::bind(&engine_type::thread_start, this, i));
      }
      thrgroup.join();
      aggregator.stop();
      // if termination reason was not changed, then it must be depletion
      if (termination_reason == execution_status::RUNNING) {
        termination_reason = execution_status::TASK_DEPLETION;
      }

      size_t ctasks = programs_executed.value;
      rmi.all_reduce(ctasks);
      programs_executed.value = ctasks;

      rmi.cerr() << "Completed Tasks: " << programs_executed.value << std::endl;


      size_t numjoins = messages.num_joins();
      rmi.all_reduce(numjoins);
      rmi.cerr() << "Schedule Joins: " << numjoins << std::endl;

      size_t numadds = messages.num_adds();
      rmi.all_reduce(numadds);
      rmi.cerr() << "Schedule Adds: " << numadds << std::endl;


      ASSERT_TRUE(scheduler_ptr->empty());
      started = false;

      rmi.dc().set_fast_track_requests(old_fasttrack);
      rmi.dc().full_barrier();
      return termination_reason;
    } // end of start


  public:
    aggregator_type* get_aggregator() { return &aggregator; }

  }; // end of class

} // namespace warp
} // namespace

#include <graphlab/macros_undef.hpp>
#include <graphlab/engine/warp_graph_broadcast.hpp>
#include <graphlab/engine/warp_graph_mapreduce.hpp>
#include <graphlab/engine/warp_graph_transform.hpp>
#endif 

