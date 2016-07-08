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




#ifndef GRAPHLAB_ASYNC_CONSISTENT_ENGINE
#define GRAPHLAB_ASYNC_CONSISTENT_ENGINE

#include <deque>
#include <boost/bind.hpp>

#include <graphlab/scheduler/ischeduler.hpp>
#include <graphlab/scheduler/scheduler_factory.hpp>
#include <graphlab/scheduler/get_message_priority.hpp>
#include <graphlab/vertex_program/ivertex_program.hpp>
#include <graphlab/vertex_program/icontext.hpp>
#include <graphlab/vertex_program/context.hpp>
#include <graphlab/engine/iengine.hpp>
#include <graphlab/engine/execution_status.hpp>
#include <graphlab/options/graphlab_options.hpp>
#include <rpc/dc_dist_object.hpp>
#include <graphlab/engine/distributed_chandy_misra.hpp>
#include <graphlab/engine/message_array.hpp>

#include <perf/tracepoint.hpp>
#include <perf/memory_info.hpp>
#include <graphlab/util/generics/conditional_addition_wrapper.hpp>
#include <rpc/distributed_event_log.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_control.hpp>
#include <fiber/fiber_async_consensus.hpp>
#include <graphlab/aggregation/distributed_aggregator.hpp>
#include <fiber/fiber_remote_request.hpp>
#include <graphlab/macros_def.hpp>



namespace graphlab {


  /**
   * \ingroup engines
   *
   * \brief The asynchronous consistent engine executed vertex programs
   * asynchronously and can ensure mutual exclusion such that adjacent vertices
   * are never executed simultaneously. The default mode is "factorized"
   * consistency in which only individual gathers/applys/
   * scatters are guaranteed to be consistent, but this can be strengthened to
   * provide full mutual exclusion.
   *
   *
   * \tparam VertexProgram
   * The user defined vertex program type which should implement the
   * \ref graphlab::ivertex_program interface.
   *
   * ### Execution Semantics
   *
   * On start() the \ref graphlab::ivertex_program::init function is invoked
   * on all vertex programs in parallel to initialize the vertex program,
   * vertex data, and possibly signal vertices.
   *
   * After which, the engine spawns a collection of threads where each thread
   * individually performs the following tasks:
   * \li Extract a message from the scheduler.
   * \li Perform distributed lock acquisition on the vertex which is supposed
   * to receive the message. The lock system enforces that no neighboring
   * vertex is executing at the same time. The implementation is based
   * on the Chandy-Misra solution to the dining philosophers problem.
   * (Chandy, K.M.; Misra, J. (1984). The Drinking Philosophers Problem.
   *  ACM Trans. Program. Lang. Syst)
   * \li Once lock acquisition is complete,
   *  \ref graphlab::ivertex_program::init is called on the vertex
   * program. As an optimization, any messages sent to this vertex
   * before completion of lock acquisition is merged into original message
   * extracted from the scheduler.
   * \li Execute the gather on the vertex program by invoking
   * the user defined \ref graphlab::ivertex_program::gather function
   * on the edge direction returned by the
   * \ref graphlab::ivertex_program::gather_edges function.  The gather
   * functions can modify edge data but cannot modify the vertex
   * program or vertex data and can be executed on multiple
   * edges in parallel.
   * * \li Execute the apply function on the vertex-program by
   * invoking the user defined \ref graphlab::ivertex_program::apply
   * function passing the sum of the gather functions.  If \ref
   * graphlab::ivertex_program::gather_edges returns no edges then
   * the default gather value is passed to apply.  The apply function
   * can modify the vertex program and vertex data.
   * \li Execute the scatter on the vertex program by invoking
   * the user defined \ref graphlab::ivertex_program::scatter function
   * on the edge direction returned by the
   * \ref graphlab::ivertex_program::scatter_edges function.  The scatter
   * functions can modify edge data but cannot modify the vertex
   * program or vertex data and can be executed on multiple
   * edges in parallel.
   * \li Release all locks acquired in the lock acquisition stage,
   * and repeat until the scheduler is empty.
   *
   * The engine threads multiplexes the above procedure through a secondary
   * internal queue, allowing an arbitrary large number of vertices to
   * begin processing at the same time.
   *
   * ### Construction
   *
   * The asynchronous consistent engine is constructed by passing in a
   * \ref graphlab::distributed_control object which manages coordination
   * between engine threads and a \ref graphlab::distributed_graph object
   * which is the graph on which the engine should be run.  The graph should
   * already be populated and cannot change after the engine is constructed.
   * In the distributed setting all program instances (running on each machine)
   * should construct an instance of the engine at the same time.
   *
   * Computation is initiated by signaling vertices using either
   * \ref graphlab::async_consistent_engine::signal or
   * \ref graphlab::async_consistent_engine::signal_all.  In either case all
   * machines should invoke signal or signal all at the same time.  Finally,
   * computation is initiated by calling the
   * \ref graphlab::async_consistent_engine::start function.
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
   * typedef float gather_type;
   * struct pagerank_vprog :
   *   public graphlab::ivertex_program<graph_type, gather_type> {
   *   // code
   * };
   *
   * int main(int argc, char** argv) {
   *   // Initialize control plain using mpi
   *   graphlab::mpi_tools::init(argc, argv);
   *   graphlab::distributed_control dc;
   *   // Parse command line options
   *   graphlab::command_line_options clopts("PageRank algorithm.");
   *   std::string graph_dir;
   *   clopts.attach_option("graph", &graph_dir, graph_dir,
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
   *   graphlab::async_consistent_engine<pagerank_vprog> engine(dc, graph, clopts);
   *   engine.signal_all();
   *   engine.start();
   *   std::cout << "Runtime: " << engine.elapsed_time();
   *   graphlab::mpi_tools::finalize();
   * }
   * \endcode
   *
   * \see graphlab::omni_engine
   * \see graphlab::synchronous_engine
   *
   * <a name=engineopts>Engine Options</a>
   * =========================
   * The asynchronous engine supports several engine options which can
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
  template<typename VertexProgram>
  class async_consistent_engine: public iengine<VertexProgram> {

  public:
    /**
     * \brief The user defined vertex program type. Equivalent to the
     * VertexProgram template argument.
     *
     * The user defined vertex program type which should implement the
     * \ref graphlab::ivertex_program interface.
     */
    typedef VertexProgram vertex_program_type;

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
     * \brief The user defined message type used to signal neighboring
     * vertex programs.
     *
     * The message type is defined in the \ref graphlab::ivertex_program
     * interface and used in the call to \ref graphlab::icontext::signal.
     * The message type must have an
     * <code>operator+=(const gather_type& other)</code> function and
     * must be \ref sec_serializable.
     */
    typedef typename VertexProgram::message_type message_type;

    /**
     * \brief The type of data associated with each vertex in the graph
     *
     * The vertex data type must be \ref sec_serializable.
     */
    typedef typename VertexProgram::vertex_data_type vertex_data_type;

    /**
     * \brief The type of data associated with each edge in the graph
     *
     * The edge data type must be \ref sec_serializable.
     */
    typedef typename VertexProgram::edge_data_type edge_data_type;

    /**
     * \brief The type of graph supported by this vertex program
     *
     * See graphlab::distributed_graph
     */
    typedef typename VertexProgram::graph_type graph_type;

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

    /**
     * \brief The type of the callback interface passed by the engine to vertex
     * programs.  See \ref graphlab::icontext for details.
     *
     * The context callback is passed to the vertex program functions and is
     * used to signal other vertices, get the current iteration, and access
     * information about the engine.
     */
    typedef icontext<graph_type, gather_type, message_type> icontext_type;

  private:
    /// \internal \brief The base type of all schedulers
    message_array<message_type> messages;

    /** \internal
     * \brief The true type of the callback context interface which
     * implements icontext. \see graphlab::icontext graphlab::context
     */
    typedef context<async_consistent_engine> context_type;

    // context needs access to internal functions
    friend class context<async_consistent_engine>;

    /// \internal \brief The type used to refer to vertices in the local graph
    typedef typename graph_type::local_vertex_type    local_vertex_type;
    /// \internal \brief The type used to refer to edges in the local graph
    typedef typename graph_type::local_edge_type      local_edge_type;
    /// \internal \brief The type used to refer to vertex IDs in the local graph
    typedef typename graph_type::lvid_type            lvid_type;

    /// \internal \brief The type of the current engine instantiation
    typedef async_consistent_engine<VertexProgram> engine_type;

    typedef conditional_addition_wrapper<gather_type> conditional_gather_type;
    
    /// The RPC interface
    dc_dist_object<async_consistent_engine<VertexProgram> > rmi;

    /// A reference to the active graph
    graph_type& graph;

    /// A pointer to the lock implementation
    distributed_chandy_misra<graph_type>* cmlocks;

    /// Per vertex data locks
    std::vector<simple_spinlock> vertexlocks;

    /// Total update function completion time
    std::vector<double> total_completion_time;

    /**
     * \brief This optional vector contains caches of previous gather
     * contributions for each machine.
     *
     * Caching is done locally and therefore a high-degree vertex may
     * have multiple caches (one per machine).
     */
    std::vector<gather_type>  gather_cache;

    /**
     * \brief A bit indicating if the local gather for that vertex is
     * available.
     */
    dense_bitset has_cache;

    bool use_cache;

    /// Engine threads.
    fiber_group thrgroup;

    //! The scheduler
    ischeduler* scheduler_ptr;

    typedef typename iengine<VertexProgram>::aggregator_type aggregator_type;
    aggregator_type aggregator;

    /// Number of kernel threads
    size_t ncpus;
    /// Size of each fiber stack
    size_t stacksize;
    /// Number of fibers
    size_t nfibers;
    /// set to true if engine is started
    bool started;

    bool track_task_time;
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
    async_consistent_engine(distributed_control &dc,
                            graph_type& graph,
                            const graphlab_options& opts = graphlab_options()) :
        rmi(dc, this), graph(graph), scheduler_ptr(NULL),
        aggregator(dc, graph, new context_type(*this, graph)), started(false),
        engine_start_time(timer::approx_time_seconds()), force_stop(false) {
      rmi.barrier();

      nfibers = 10000;
      stacksize = 131072;
      use_cache = false;
      factorized_consistency = true;
      track_task_time = false;
      timed_termination = (size_t)(-1);
      termination_reason = execution_status::UNSET;
      set_options(opts);
      total_completion_time.resize(fiber_control::get_instance().num_workers());
      init();
      rmi.barrier();
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
        } else if (opt == "track_task_time") {
          opts.get_engine_args().get_option("track_task_time", track_task_time);
          if (rmi.procid() == 0)
            logstream(LOG_EMPH) << "Engine Option: track_task_time = " << track_task_time<< std::endl;
        }else if (opt == "stacksize") {
          opts.get_engine_args().get_option("stacksize", stacksize);
          if (rmi.procid() == 0)
            logstream(LOG_EMPH) << "Engine Option: stacksize= " << stacksize << std::endl;
        } else if (opt == "use_cache") {
          opts.get_engine_args().get_option("use_cache", use_cache);
          if (rmi.procid() == 0)
            logstream(LOG_EMPH) << "Engine Option: use_cache = " << use_cache << std::endl;
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
      scheduler_ptr = scheduler_factory::
                    new_scheduler(graph.num_local_vertices(),
                                  opts_copy);
      rmi.barrier();

      // create initial fork arrangement based on the alternate vid mapping
      if (factorized_consistency == false) {
        cmlocks = new distributed_chandy_misra<graph_type>(rmi.dc(), graph,
                                                    boost::bind(&engine_type::lock_ready, this, _1));
                                                    
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
      // construct all the required datastructures
      // deinitialize performs the reverse
      graph.finalize();
      scheduler_ptr->set_num_vertices(graph.num_local_vertices());
      messages.resize(graph.num_local_vertices());
      vertexlocks.resize(graph.num_local_vertices());
      program_running.resize(graph.num_local_vertices());
      hasnext.resize(graph.num_local_vertices());
      if (use_cache) {
        gather_cache.resize(graph.num_local_vertices(), gather_type());
        has_cache.resize(graph.num_local_vertices());
        has_cache.clear();
      }
      if (!factorized_consistency) {
        cm_handles.resize(graph.num_local_vertices());
      }
      rmi.barrier();
    }



  public:
    ~async_consistent_engine() {
      delete consensus;
      delete cmlocks;
      delete scheduler_ptr;
    }




    // documentation inherited from iengine
    size_t num_updates() const {
      return programs_executed.value;
    }





    // documentation inherited from iengine
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
      const lvid_type local_vid = graph.local_vid(vid);
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
      if (force_stop) return;
      if (started) {
        const typename graph_type::vertex_record& rec = graph.l_get_vertex_record(vtx.local_id());
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
      if (graph.is_master(gvid)) {
        internal_signal(graph.vertex(gvid), message);
      } else {
        procid_t proc = graph.master(gvid);
        rmi.remote_call(proc, &async_consistent_engine::internal_signal_gvid,
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
        rmi.remote_call(i, &async_consistent_engine::rpc_internal_stop);
      }
    }



    /**
     * \brief Post a to a previous gather for a give vertex.
     *
     * This function is called by the \ref graphlab::context.
     *
     * @param [in] vertex The vertex to which to post a change in the sum
     * @param [in] delta The change in that sum
     */
    void internal_post_delta(const vertex_type& vertex,
                             const gather_type& delta) {
      if(use_cache) {
        const lvid_type lvid = vertex.local_id();
        vertexlocks[lvid].lock();
        if( has_cache.get(lvid) ) {
          gather_cache[lvid] += delta;
        } else {
          // You cannot add a delta to an empty cache.  A complete
          // gather must have been run.
          // gather_cache[lvid] = delta;
          // has_cache.set_bit(lvid);
        }
        vertexlocks[lvid].unlock();
      }
    }

    /**
     * \brief Clear the cached gather for a vertex if one is
     * available.
     *
     * This function is called by the \ref graphlab::context.
     *
     * @param [in] vertex the vertex for which to clear the cache
     */
    void internal_clear_gather_cache(const vertex_type& vertex) {
      const lvid_type lvid = vertex.local_id();
      if(use_cache && has_cache.get(lvid)) {
        vertexlocks[lvid].lock();
        gather_cache[lvid] = gather_type();
        has_cache.clear_bit(lvid);
        vertexlocks[lvid].unlock();
      }

    }

  public:



    void signal(vertex_id_type gvid,
                const message_type& message = message_type()) {
      rmi.barrier();
      internal_signal_gvid(gvid, message);
      rmi.barrier();
    }


    void signal_all(const message_type& message = message_type(),
                    const std::string& order = "shuffle") {
      vertex_set vset = graph.complete_set();
      signal_vset(vset, message, order);
    } // end of schedule all

    void signal_vset(const vertex_set& vset,
                    const message_type& message = message_type(),
                    const std::string& order = "shuffle") {
      logstream(LOG_DEBUG) << rmi.procid() << ": Schedule All" << std::endl;
      // allocate a vector with all the local owned vertices
      // and schedule all of them.
      std::vector<vertex_id_type> vtxs;
      vtxs.reserve(graph.num_local_own_vertices());
      for(lvid_type lvid = 0;
          lvid < graph.get_local_graph().num_vertices();
          ++lvid) {
        if (graph.l_vertex(lvid).owner() == rmi.procid() &&
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
    sched_status::status_enum get_next_sched_task( size_t threadid,
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
      fiber_control::yield();
      logstream(LOG_DEBUG) << rmi.procid() << "-" << threadid << ": " << "Termination Attempt " << std::endl;
      has_sched_msg = false;
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
          rmi.remote_call(i, &async_consistent_engine::set_endgame_mode);
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


    conditional_gather_type perform_gather(vertex_id_type vid,
                               vertex_program_type& vprog_) {
      vertex_program_type vprog = vprog_;
      lvid_type lvid = graph.local_vid(vid);
      local_vertex_type local_vertex(graph.l_vertex(lvid));
      vertex_type vertex(local_vertex);
      context_type context(*this, graph);
      edge_dir_type gather_dir = vprog.gather_edges(context, vertex);
      conditional_gather_type accum;

      //check against the cache
      if( use_cache && has_cache.get(lvid) ) {
          accum.set(gather_cache[lvid]);
          return accum;
      }
      // do in edges
      if(gather_dir == IN_EDGES || gather_dir == ALL_EDGES) {
        foreach(local_edge_type local_edge, local_vertex.in_edges()) {
          edge_type edge(local_edge);
          lvid_type a = edge.source().local_id(), b = edge.target().local_id();
          vertexlocks[std::min(a,b)].lock();
          vertexlocks[std::max(a,b)].lock();
          accum += vprog.gather(context, vertex, edge);
          vertexlocks[a].unlock();
          vertexlocks[b].unlock();
        }
      } 
      // do out edges
      if(gather_dir == OUT_EDGES || gather_dir == ALL_EDGES) {
        foreach(local_edge_type local_edge, local_vertex.out_edges()) {
          edge_type edge(local_edge);
          lvid_type a = edge.source().local_id(), b = edge.target().local_id();
          vertexlocks[std::min(a,b)].lock();
          vertexlocks[std::max(a,b)].lock();
          accum += vprog.gather(context, vertex, edge);
          vertexlocks[a].unlock();
          vertexlocks[b].unlock();
        }
      } 
      if (use_cache) {
        gather_cache[lvid] = accum.value; has_cache.set_bit(lvid);
      }
      return accum;
    }


    void perform_scatter_local(lvid_type lvid,
                               vertex_program_type& vprog) {
      local_vertex_type local_vertex(graph.l_vertex(lvid));
      vertex_type vertex(local_vertex);
      context_type context(*this, graph);
      edge_dir_type scatter_dir = vprog.scatter_edges(context, vertex);
      if(scatter_dir == IN_EDGES || scatter_dir == ALL_EDGES) {
        foreach(local_edge_type local_edge, local_vertex.in_edges()) {
          edge_type edge(local_edge);
          lvid_type a = edge.source().local_id(), b = edge.target().local_id();
          vertexlocks[std::min(a,b)].lock();
          vertexlocks[std::max(a,b)].lock();
          vprog.scatter(context, vertex, edge);
          vertexlocks[a].unlock();
          vertexlocks[b].unlock();
        }
      } 
      if(scatter_dir == OUT_EDGES || scatter_dir == ALL_EDGES) {
        foreach(local_edge_type local_edge, local_vertex.out_edges()) {
          edge_type edge(local_edge);
          lvid_type a = edge.source().local_id(), b = edge.target().local_id();
          vertexlocks[std::min(a,b)].lock();
          vertexlocks[std::max(a,b)].lock();
          vprog.scatter(context, vertex, edge);
          vertexlocks[a].unlock();
          vertexlocks[b].unlock();
        }
      } 

      // release locks
      if (!factorized_consistency) {
        cmlocks->philosopher_stops_eating_per_replica(lvid);
      }
    }


    void perform_scatter(vertex_id_type vid,
                    vertex_program_type& vprog_,
                    const vertex_data_type& newdata) {
      vertex_program_type vprog = vprog_;
      lvid_type lvid = graph.local_vid(vid);
      vertexlocks[lvid].lock();
      graph.l_vertex(lvid).data() = newdata;
      vertexlocks[lvid].unlock();
      perform_scatter_local(lvid, vprog);
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


    /**
     * \internal
     * Called when the scheduler returns a vertex to run.
     * If this function is called with vertex locks acquired, prelocked
     * should be true. Otherwise it should be false.
     */
    void eval_sched_task(const lvid_type lvid,
                         const message_type& msg) {
      const typename graph_type::vertex_record& rec = graph.l_get_vertex_record(lvid);
      vertex_id_type vid = rec.gvid;
      char task_time_data[sizeof(timer)];
      timer* task_time;
      if (track_task_time) {
        // placement new to create the timer
        task_time = reinterpret_cast<timer*>(task_time_data);
        new (task_time) timer();
      }
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

      /**************************************************************************/
      /*                             Begin Program                              */
      /**************************************************************************/
      context_type context(*this, graph);
      vertex_program_type vprog = vertex_program_type();
      local_vertex_type local_vertex(graph.l_vertex(lvid));
      vertex_type vertex(local_vertex);

      /**************************************************************************/
      /*                               init phase                               */
      /**************************************************************************/
      vprog.init(context, vertex, msg);

      /**************************************************************************/
      /*                              Gather Phase                              */
      /**************************************************************************/
      conditional_gather_type gather_result;
      std::vector<request_future<conditional_gather_type> > gather_futures;
      foreach(procid_t mirror, local_vertex.mirrors()) {
        gather_futures.push_back(
            object_fiber_remote_request(rmi, 
                                        mirror, 
                                        &async_consistent_engine::perform_gather, 
                                        vid,
                                        vprog));
      }
      gather_result += perform_gather(vid, vprog);

      for(size_t i = 0;i < gather_futures.size(); ++i) {
        gather_result += gather_futures[i]();
      }

     /**************************************************************************/
     /*                              apply phase                               */
     /**************************************************************************/
     vertexlocks[lvid].lock();
     vprog.apply(context, vertex, gather_result.value);      
     vertexlocks[lvid].unlock();


     /**************************************************************************/
     /*                            scatter phase                               */
     /**************************************************************************/

     // should I wait for the scatter? nah... but in case you want to
     // the code is commented below
     /*foreach(procid_t mirror, local_vertex.mirrors()) {
       rmi.remote_call(mirror, 
                       &async_consistent_engine::perform_scatter, 
                       vid,
                       vprog,
                       local_vertex.data());
     }*/

     std::vector<request_future<void> > scatter_futures;
     foreach(procid_t mirror, local_vertex.mirrors()) {
       scatter_futures.push_back(
           object_fiber_remote_request(rmi, 
                                       mirror, 
                                       &async_consistent_engine::perform_scatter, 
                                       vid,
                                       vprog,
                                       local_vertex.data()));
     }
     perform_scatter_local(lvid, vprog);
     for(size_t i = 0;i < scatter_futures.size(); ++i) 
       scatter_futures[i]();

      /************************************************************************/
      /*                           Release Locks                              */
      /************************************************************************/
      // the scatter is used to release the chandy misra
      // here I cleanup
      if (!factorized_consistency) {
        delete cm_handles[lvid];
        cm_handles[lvid] = NULL;
      }
      release_exclusive_access_to_vertex(lvid);
      if (track_task_time) {
        total_completion_time[fiber_control::get_worker_id()] += 
            task_time->current_time();
        task_time->~timer();
      }
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

        if (fiber_control::worker_has_priority_fibers_on_queue()) {
          fiber_control::yield();
        }
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
      thrgroup.set_stacksize(stacksize);
        
      size_t effncpus = std::min(ncpus, fiber_control::get_instance().num_workers());
      for (size_t i = 0; i < nfibers ; ++i) {
        thrgroup.launch(boost::bind(&engine_type::thread_start, this, i), 
                        i % effncpus);
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

      if (track_task_time) {
        double total_task_time = 0;
        for (size_t i = 0;i < total_completion_time.size(); ++i) {
          total_task_time += total_completion_time[i];
        }
        rmi.all_reduce(total_task_time);
        rmi.cerr() << "Average Task Completion Time = " 
                   << total_task_time / programs_executed.value << std::endl;
      }


      ASSERT_TRUE(scheduler_ptr->empty());
      started = false;

      rmi.dc().set_fast_track_requests(old_fasttrack);
      return termination_reason;
    } // end of start


  public:
    aggregator_type* get_aggregator() { return &aggregator; }

  }; // end of class
} // namespace

#include <graphlab/macros_undef.hpp>

#endif // GRAPHLAB_DISTRIBUTED_ENGINE_HPP

