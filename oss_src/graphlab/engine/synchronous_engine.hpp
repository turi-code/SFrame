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



#ifndef GRAPHLAB_SYNCHRONOUS_ENGINE_HPP
#define GRAPHLAB_SYNCHRONOUS_ENGINE_HPP

#include <deque>
#include <boost/bind.hpp>

#include <graphlab/engine/iengine.hpp>

#include <graphlab/vertex_program/ivertex_program.hpp>
#include <graphlab/vertex_program/icontext.hpp>
#include <graphlab/vertex_program/context.hpp>

#include <graphlab/engine/execution_status.hpp>
#include <graphlab/options/graphlab_options.hpp>




#include <parallel/pthread_tools.hpp>
#include <fiber/fiber_barrier.hpp>
#include <perf/tracepoint.hpp>
#include <perf/memory_info.hpp>

#include <rpc/dc_dist_object.hpp>
#include <rpc/distributed_event_log.hpp>
#include <rpc/fiber_buffered_exchange.hpp>






#include <graphlab/macros_def.hpp>

namespace graphlab {


  /**
   * \ingroup engines
   *
   * \brief The synchronous engine executes all active vertex program
   * synchronously in a sequence of super-step (iterations) in both the
   * shared and distributed memory settings.
   *
   * \tparam VertexProgram The user defined vertex program which
   * should implement the \ref graphlab::ivertex_program interface.
   *
   *
   * ### Execution Semantics
   *
   * On start() the \ref graphlab::ivertex_program::init function is invoked
   * on all vertex programs in parallel to initialize the vertex program,
   * vertex data, and possibly signal vertices.
   * The engine then proceeds to execute a sequence of
   * super-steps (iterations) each of which is further decomposed into a
   * sequence of minor-steps which are also executed synchronously:
   * \li Receive all incoming messages (signals) by invoking the
   * \ref graphlab::ivertex_program::init function on all
   * vertex-programs that have incoming messages.  If a
   * vertex-program does not have any incoming messages then it is
   * not active during this super-step.
   * \li Execute all gathers for active vertex programs by invoking
   * the user defined \ref graphlab::ivertex_program::gather function
   * on the edge direction returned by the
   * \ref graphlab::ivertex_program::gather_edges function.  The gather
   * functions can modify edge data but cannot modify the vertex
   * program or vertex data and therefore can be executed on multiple
   * edges in parallel.  The gather type is used to accumulate (sum)
   * the result of the gather function calls.
   * \li Execute all apply functions for active vertex-programs by
   * invoking the user defined \ref graphlab::ivertex_program::apply
   * function passing the sum of the gather functions.  If \ref
   * graphlab::ivertex_program::gather_edges returns no edges then
   * the default gather value is passed to apply.  The apply function
   * can modify the vertex program and vertex data.
   * \li Execute all scatters for active vertex programs by invoking
   * the user defined \ref graphlab::ivertex_program::scatter function
   * on the edge direction returned by the
   * \ref graphlab::ivertex_program::scatter_edges function.  The scatter
   * functions can modify edge data but cannot modify the vertex
   * program or vertex data and therefore can be executed on multiple
   * edges in parallel.
   *
   * ### Construction
   *
   * The synchronous engine is constructed by passing in a
   * \ref graphlab::distributed_control object which manages coordination
   * between engine threads and a \ref graphlab::distributed_graph object
   * which is the graph on which the engine should be run.  The graph should
   * already be populated and cannot change after the engine is constructed.
   * In the distributed setting all program instances (running on each machine)
   * should construct an instance of the engine at the same time.
   *
   * Computation is initiated by signaling vertices using either
   * \ref graphlab::synchronous_engine::signal or
   * \ref graphlab::synchronous_engine::signal_all.  In either case all
   * machines should invoke signal or signal all at the same time.  Finally,
   * computation is initiated by calling the
   * \ref graphlab::synchronous_engine::start function.
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
   *   graphlab::synchronous_engine<pagerank_vprog> engine(dc, graph, clopts);
   *   engine.signal_all();
   *   engine.start();
   *   std::cout << "Runtime: " << engine.elapsed_time();
   *   graphlab::mpi_tools::finalize();
   * }
   * \endcode
   *
   *
   *
   * <a name=engineopts>Engine Options</a>
   * =====================
   * The synchronous engine supports several engine options which can
   * be set as command line arguments using \c --engine_opts :
   *
   * \li <b>max_iterations</b>: (default: infinity) The maximum number
   * of iterations (super-steps) to run.
   *
   * \li <b>timeout</b>: (default: infinity) The maximum time in
   * seconds that the engine may run. When the time runs out the
   * current iteration is completed and then the engine terminates.
   *
   * \li <b>use_cache</b>: (default: false) This is used to enable
   * caching.  When caching is enabled the gather phase is skipped for
   * vertices that already have a cached value.  To use caching the
   * vertex program must either clear (\ref icontext::clear_gather_cache)
   * or update (\ref icontext::post_delta) the cache values of
   * neighboring vertices during the scatter phase.
   *
   * \li \b snapshot_interval If set to a positive value, a snapshot
   * is taken every this number of iterations. If set to 0, a snapshot
   * is taken before the first iteration. If set to a negative value,
   * no snapshots are taken. Defaults to -1. A snapshot is a binary
   * dump of the graph.
   *
   * \li \b snapshot_path If snapshot_interval is set to a value >=0,
   * this option must be specified and should contain a target basename
   * for the snapshot. The path including folder and file prefix in
   * which the snapshots should be saved.
   *
   * \see graphlab::omni_engine
   * \see graphlab::async_consistent_engine
   * \see graphlab::semi_synchronous_engine
   */
  template<typename VertexProgram>
  class synchronous_engine :
    public iengine<VertexProgram> {

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
    typedef typename VertexProgram::graph_type  graph_type;

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

    /**
     * \brief Local vertex type used by the engine for fast indexing
     */
    typedef typename graph_type::local_vertex_type    local_vertex_type;

    /**
     * \brief Local edge type used by the engine for fast indexing
     */
    typedef typename graph_type::local_edge_type      local_edge_type;

    /**
     * \brief Local vertex id type used by the engine for fast indexing
     */
    typedef typename graph_type::lvid_type            lvid_type;

    std::vector<double> per_thread_compute_time;
    /**
     * \brief The actual instance of the context type used by this engine.
     */
    typedef context<synchronous_engine> context_type;
    friend class context<synchronous_engine>;


    /**
     * \brief The type of the distributed aggregator inherited from iengine
     */
    typedef typename iengine<vertex_program_type>::aggregator_type aggregator_type;

    /**
     * \brief The object used to communicate with remote copies of the
     * synchronous engine.
     */
    dc_dist_object< synchronous_engine<VertexProgram> > rmi;

    /**
     * \brief A reference to the distributed graph on which this
     * synchronous engine is running.
     */
    graph_type& graph;

    /**
     * \brief The number of CPUs used.
     */
    size_t ncpus;

    /**
     * \brief The local worker threads used by this engine
     */
    fiber_group threads;

    /**
     * \brief A thread barrier that is used to control the threads in the
     * thread pool.
     */
    fiber_barrier thread_barrier;

    /**
     * \brief The maximum number of super-steps (iterations) to run
     * before terminating.  If the max iterations is reached the
     * engine will terminate if their are no messages remaining.
     */
    size_t max_iterations;


   /* 
    * \brief When caching is enabled the gather phase is skipped for
    * vertices that already have a cached value.  To use caching the
    * vertex program must either clear (\ref icontext::clear_gather_cache)
    * or update (\ref icontext::post_delta) the cache values of
    * neighboring vertices during the scatter phase.
    */
    bool use_cache;

    /**
     * \brief A snapshot is taken every this number of iterations.
     * If snapshot_interval == 0, a snapshot is only taken before the first
     * iteration. If snapshot_interval < 0, no snapshots are taken.
     */
    int snapshot_interval;

    /// \brief The target base name the snapshot is saved in.
    std::string snapshot_path;

    /**
     * \brief A counter that tracks the current iteration number since
     * start was last invoked.
     */
    size_t iteration_counter;

    /**
     * \brief The time in seconds at which the engine started.
     */
    float start_time;

    /**
     * \brief The timeout time in seconds
     */
    float timeout;

    /**
     * \brief Schedules all vertices every iteration
     */
    bool sched_allv;

    /**
     * \brief Used to stop the engine prematurely
     */
    bool force_abort;

    /**
     * \brief The vertex locks protect access to vertex specific
     * data-structures including
     * \ref graphlab::synchronous_engine::gather_accum
     * and \ref graphlab::synchronous_engine::messages.
     */
    std::vector<simple_spinlock> vlocks;


    /**
     * \brief The elocks protect individual edges during gather and
     * scatter.  Technically there is a potential race since gather
     * and scatter can modify edge values and can overlap.  The edge
     * lock ensures that only one gather or scatter occurs on an edge
     * at a time.
     */
    std::vector<simple_spinlock> elocks;



    /**
     * \brief The vertex programs associated with each vertex on this
     * machine.
     */
    std::vector<vertex_program_type> vertex_programs;

    /**
     * \brief Vector of messages associated with each vertex.
     */
    std::vector<message_type> messages;

    /**
     * \brief Bit indicating whether a message is present for each vertex.
     */
    dense_bitset has_message;


    /**
     * \brief Gather accumulator used for each master vertex to merge
     * the result of all the machine specific accumulators (or
     * caches).
     *
     * The gather accumulator can be accessed by multiple threads at
     * once and therefore must be guarded by a vertex locks in
     * \ref graphlab::synchronous_engine::vlocks
     */
    std::vector<gather_type>  gather_accum;

    /**
     * \brief Bit indicating if the gather has accumulator contains any
     * values.
     *
     * While dense bitsets are thread safe the value of this bit must
     * change concurrently with the
     * \ref graphlab::synchronous_engine::gather_accum and therefore is
     * set while holding the lock in
     * \ref graphlab::synchronous_engine::vlocks.
     */
    dense_bitset has_gather_accum;


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

    /**
     * \brief A bit (for master vertices) indicating if that vertex is active
     * (received a message on this iteration).
     */
    dense_bitset active_superstep;

    /**
     * \brief  The number of local vertices (masters) that are active on this
     * iteration.
     */
    atomic<size_t> num_active_vertices;

    /**
     * \brief A bit indicating (for all vertices) whether to
     * participate in the current minor-step (gather or scatter).
     */
    dense_bitset active_minorstep;

    /**
     * \brief A counter measuring the number of applys that have been completed
     */
    atomic<size_t> completed_applys;


    /**
     * \brief The shared counter used coordinate operations between
     * threads.
     */
    atomic<size_t> shared_lvid_counter;


    /**
     * \brief The pair type used to synchronize vertex programs across machines.
     */
    typedef std::pair<vertex_id_type, vertex_program_type> vid_prog_pair_type;

    /**
     * \brief The type of the exchange used to synchronize vertex programs
     */
    typedef fiber_buffered_exchange<vid_prog_pair_type> vprog_exchange_type;

    /**
     * \brief The distributed exchange used to synchronize changes to
     * vertex programs.
     */
    vprog_exchange_type vprog_exchange;

    /**
     * \brief The pair type used to synchronize vertex across across machines.
     */
    typedef std::pair<vertex_id_type, vertex_data_type> vid_vdata_pair_type;

    /**
     * \brief The type of the exchange used to synchronize vertex data
     */
    typedef fiber_buffered_exchange<vid_vdata_pair_type> vdata_exchange_type;

    /**
     * \brief The distributed exchange used to synchronize changes to
     * vertex programs.
     */
    vdata_exchange_type vdata_exchange;

    /**
     * \brief The pair type used to synchronize the results of the gather phase
     */
    typedef std::pair<vertex_id_type, gather_type> vid_gather_pair_type;

    /**
     * \brief The type of the exchange used to synchronize gather
     * accumulators
     */
    typedef fiber_buffered_exchange<vid_gather_pair_type> gather_exchange_type;

    /**
     * \brief The distributed exchange used to synchronize gather
     * accumulators.
     */
    gather_exchange_type gather_exchange;

    /**
     * \brief The pair type used to synchronize messages
     */
    typedef std::pair<vertex_id_type, message_type> vid_message_pair_type;

    /**
     * \brief The type of the exchange used to synchronize messages
     */
    typedef fiber_buffered_exchange<vid_message_pair_type> message_exchange_type;

    /**
     * \brief The distributed exchange used to synchronize messages
     */
    message_exchange_type message_exchange;


    /**
     * \brief The distributed aggregator used to manage background
     * aggregation.
     */
    aggregator_type aggregator;

    DECLARE_EVENT(EVENT_APPLIES);
    DECLARE_EVENT(EVENT_GATHERS);
    DECLARE_EVENT(EVENT_SCATTERS);
    DECLARE_EVENT(EVENT_ACTIVE_CPUS);
  public:

    /**
     * \brief Construct a synchronous engine for a given graph and options.
     *
     * The synchronous engine should be constructed after the graph
     * has been loaded (e.g., \ref graphlab::distributed_graph::load)
     * and the graphlab options have been set
     * (e.g., \ref graphlab::command_line_options).
     *
     * In the distributed engine the synchronous engine must be called
     * on all machines at the same time (in the same order) passing
     * the \ref graphlab::distributed_control object.  Upon
     * construction the synchronous engine allocates several
     * data-structures to store messages, gather accumulants, and
     * vertex programs and therefore may require considerable memory.
     *
     * The number of threads to create are read from
     * \ref graphlab_options::get_ncpus "opts.get_ncpus()".
     *
     * See the <a href="#engineopts">main class documentation</a>
     * for details on the available options.
     *
     * @param [in] dc Distributed controller to associate with
     * @param [in,out] graph A reference to the graph object that this
     * engine will modify. The graph must be fully constructed and
     * finalized.
     * @param [in] opts A graphlab::graphlab_options object specifying engine
     *                  parameters.  This is typically constructed using
     *                  \ref graphlab::command_line_options.
     */
    synchronous_engine(distributed_control& dc, graph_type& graph,
                       const graphlab_options& opts = graphlab_options());


    /**
     * \brief Start execution of the synchronous engine.
     *
     * The start function begins computation and does not return until
     * there are no remaining messages or until max_iterations has
     * been reached.
     *
     * The start() function modifies the data graph through the vertex
     * programs and so upon return the data graph should contain the
     * result of the computation.
     *
     * @return The reason for termination
     */
    execution_status::status_enum start();

    // documentation inherited from iengine
    size_t num_updates() const;

    // documentation inherited from iengine
    void signal(vertex_id_type vid,
                const message_type& message = message_type());

    // documentation inherited from iengine
    void signal_all(const message_type& message = message_type(),
                    const std::string& order = "shuffle");

    void signal_vset(const vertex_set& vset,
                    const message_type& message = message_type(),
                    const std::string& order = "shuffle");


    // documentation inherited from iengine
    float elapsed_seconds() const;

    /**
     * \brief Get the current iteration number since start was last
     * invoked.
     *
     *  \return the current iteration
     */
    int iteration() const;


    /**
     * \brief Compute the total memory used by the entire distributed
     * system.
     *
     * @return The total memory used in bytes.
     */
    size_t total_memory_usage() const;

    /**
     * \brief Get a pointer to the distributed aggregator object.
     *
     * This is currently used by the \ref graphlab::iengine interface to
     * implement the calls to aggregation.
     *
     * @return a pointer to the local aggregator.
     */
    aggregator_type* get_aggregator();

    /**
     * \brief Resizes the engine's internal datastructures to match the graph.
     * Clears all messages. Must be called before signaling functions if the
     * size of the graph is changed.
     */
    void init();


  private:


    /**
     * \brief Resize the datastructures to fit the graph size (in case of dynamic graph). Keep all the messages
     * and caches.
     */
    void resize();

    /**
     * \brief This internal stop function is called by the \ref graphlab::context to
     * terminate execution of the engine.
     */
    void internal_stop();

    /**
     * \brief This function is called remote by the rpc to force the
     * engine to stop.
     */
    void rpc_stop();

    /**
     * \brief Signal a vertex.
     *
     * This function is called by the \ref graphlab::context.
     *
     * @param [in] vertex the vertex to signal
     * @param [in] message the message to send to that vertex.
     */
    void internal_signal(const vertex_type& vertex,
                         const message_type& message = message_type());

    /**
     * \brief Called by the context to signal an arbitrary vertex.
     *
     * @param [in] gvid the global vertex id of the vertex to signal
     * @param [in] message the message to send to that vertex.
     */
    void internal_signal_gvid(vertex_id_type gvid,
                              const message_type& message = message_type());

    /**
     * \brief This function tests if this machine is the master of
     * gvid and signals if successful.
     */
    void internal_signal_rpc(vertex_id_type gvid,
                              const message_type& message = message_type());


    /**
     * \brief Post a to a previous gather for a give vertex.
     *
     * This function is called by the \ref graphlab::context.
     *
     * @param [in] vertex The vertex to which to post a change in the sum
     * @param [in] delta The change in that sum
     */
    void internal_post_delta(const vertex_type& vertex,
                             const gather_type& delta);

    /**
     * \brief Clear the cached gather for a vertex if one is
     * available.
     *
     * This function is called by the \ref graphlab::context.
     *
     * @param [in] vertex the vertex for which to clear the cache
     */
    void internal_clear_gather_cache(const vertex_type& vertex);


    // Program Steps ==========================================================


    void thread_launch_wrapped_event_counter(boost::function<void(void)> fn) {
      INCREMENT_EVENT(EVENT_ACTIVE_CPUS, 1);
      fn();
      DECREMENT_EVENT(EVENT_ACTIVE_CPUS, 1);
    }

    /**
     * \brief Executes ncpus copies of a member function each with a
     * unique consecutive id (thread id).
     *
     * This function is used by the main loop to execute each of the
     * stages in parallel.
     *
     * The member function must have the type:
     *
     * \code
     * void synchronous_engine::member_fun(size_t threadid);
     * \endcode
     *
     * This function runs an rmi barrier after termination
     *
     * @tparam the type of the member function.
     * @param [in] member_fun the function to call.
     */
    template<typename MemberFunction>
    void run_synchronous(MemberFunction member_fun) {
      shared_lvid_counter = 0;
      if (ncpus <= 1) {
        INCREMENT_EVENT(EVENT_ACTIVE_CPUS, 1);
      }
      // launch the initialization threads
      for(size_t i = 0; i < ncpus; ++i) {
        fiber_control::affinity_type affinity;
        affinity.clear(); affinity.set_bit(i);
        boost::function<void(void)> invoke = boost::bind(member_fun, this, i);
        threads.launch(boost::bind(
              &synchronous_engine::thread_launch_wrapped_event_counter,
              this,
              invoke), affinity);
      }
      // Wait for all threads to finish
      threads.join();
      rmi.barrier();
      if (ncpus <= 1) {
        DECREMENT_EVENT(EVENT_ACTIVE_CPUS, 1);
      }
    } // end of run_synchronous

    // /**
    //  * \brief Initialize all vertex programs by invoking
    //  * \ref graphlab::ivertex_program::init on all vertices.
    //  *
    //  * @param thread_id the thread to run this as which determines
    //  * which vertices to process.
    //  */
    // void initialize_vertex_programs(size_t thread_id);

    /**
     * \brief Synchronize all message data.
     *
     * @param thread_id the thread to run this as which determines
     * which vertices to process.
     */
    void exchange_messages(size_t thread_id);


    /**
     * \brief Invoke the \ref graphlab::ivertex_program::init function
     * on all vertex programs that have inbound messages.
     *
     * @param thread_id the thread to run this as which determines
     * which vertices to process.
     */
    void receive_messages(size_t thread_id);


    /**
     * \brief Execute the \ref graphlab::ivertex_program::gather function on all
     * vertices that received messages for the edges specified by the
     * \ref graphlab::ivertex_program::gather_edges.
     *
     * @param thread_id the thread to run this as which determines
     * which vertices to process.
     */
    void execute_gathers(size_t thread_id);




    /**
     * \brief Execute the \ref graphlab::ivertex_program::apply function on all
     * all vertices that received messages in this super-step (active).
     *
     * @param thread_id the thread to run this as which determines
     * which vertices to process.
     */
    void execute_applys(size_t thread_id);

    /**
     * \brief Execute the \ref graphlab::ivertex_program::scatter function on all
     * vertices that received messages for the edges specified by the
     * \ref graphlab::ivertex_program::scatter_edges.
     *
     * @param thread_id the thread to run this as which determines
     * which vertices to process.
     */
    void execute_scatters(size_t thread_id);

    // Data Synchronization ===================================================
    /**
     * \brief Send the vertex program for the local vertex id to all
     * of its mirrors.
     *
     * @param [in] lvid the vertex to sync.  This muster must be the
     * master of that vertex.
     */
    void sync_vertex_program(lvid_type lvid, size_t thread_id);

    /**
     * \brief Receive all incoming vertex programs and update the
     * local mirrors.
     *
     * This function returns when there are no more incoming vertex
     * programs and should be called after a flush of the vertex
     * program exchange.
     */
    void recv_vertex_programs();

    /**
     * \brief Send the vertex data for the local vertex id to all of
     * its mirrors.
     *
     * @param [in] lvid the vertex to sync.  This machine must be the master
     * of that vertex.
     */
    void sync_vertex_data(lvid_type lvid, size_t thread_id);

    /**
     * \brief Receive all incoming vertex data and update the local
     * mirrors.
     *
     * This function returns when there are no more incoming vertex
     * data and should be called after a flush of the vertex data
     * exchange.
     */
    void recv_vertex_data();

    /**
     * \brief Send the gather value for the vertex id to its master.
     *
     * @param [in] lvid the vertex to send the gather value to
     * @param [in] accum the locally computed gather value.
     */
    void sync_gather(lvid_type lvid, const gather_type& accum,
                     size_t thread_id);


    /**
     * \brief Receive the gather values from the buffered exchange.
     *
     * This function returns when there is nothing left in the
     * buffered exchange and should be called after the buffered
     * exchange has been flushed
     */
    void recv_gathers();

    /**
     * \brief Send the accumulated message for the local vertex to its
     * master.
     *
     * @param [in] lvid the vertex to send
     */
    void sync_message(lvid_type lvid, const size_t thread_id);

    /**
     * \brief Receive the messages from the buffered exchange.
     *
     * This function returns when there is nothing left in the
     * buffered exchange and should be called after the buffered
     * exchange has been flushed
     */
    void recv_messages();


  }; // end of class synchronous engine

























  /**
   * Constructs an synchronous distributed engine.
   * The number of threads to create are read from
   * opts::get_ncpus().
   *
   * Valid engine options (graphlab_options::get_engine_args()):
   * \arg \c max_iterations Sets the maximum number of iterations the
   * engine will run for.
   * \arg \c use_cache If set to true, partial gathers are cached.
   * See \ref gather_caching to understand the behavior of the
   * gather caching model and how it may be used to accelerate program
   * performance.
   *
   * \param dc Distributed controller to associate with
   * \param graph The graph to schedule over. The graph must be fully
   *              constructed and finalized.
   * \param opts A graphlab_options object containing options and parameters
   *             for the engine.
   */
  template<typename VertexProgram>
  synchronous_engine<VertexProgram>::
  synchronous_engine(distributed_control &dc,
                     graph_type& graph,
                     const graphlab_options& opts) :
    rmi(dc, this), graph(graph),
    ncpus(opts.get_ncpus()),
    threads(2*1024*1024 /* 2MB stack per fiber*/),
    thread_barrier(opts.get_ncpus()),
    max_iterations(-1), snapshot_interval(-1), iteration_counter(0),
    timeout(0), sched_allv(false),
    vprog_exchange(dc),
    vdata_exchange(dc),
    gather_exchange(dc),
    message_exchange(dc),
    aggregator(dc, graph, new context_type(*this, graph)) {
    // Process any additional options
    std::vector<std::string> keys = opts.get_engine_args().get_option_keys();
    per_thread_compute_time.resize(opts.get_ncpus());
    use_cache = false;
    foreach(std::string opt, keys) {
      if (opt == "max_iterations") {
        opts.get_engine_args().get_option("max_iterations", max_iterations);
        if (rmi.procid() == 0)
          logstream(LOG_EMPH) << "Engine Option: max_iterations = "
            << max_iterations << std::endl;
      } else if (opt == "timeout") {
        opts.get_engine_args().get_option("timeout", timeout);
        if (rmi.procid() == 0)
          logstream(LOG_EMPH) << "Engine Option: timeout = "
            << timeout << std::endl;
      } else if (opt == "use_cache") {
        opts.get_engine_args().get_option("use_cache", use_cache);
        if (rmi.procid() == 0)
          logstream(LOG_EMPH) << "Engine Option: use_cache = "
            << use_cache << std::endl;
      } else if (opt == "snapshot_interval") {
        opts.get_engine_args().get_option("snapshot_interval", snapshot_interval);
        if (rmi.procid() == 0)
          logstream(LOG_EMPH) << "Engine Option: snapshot_interval = "
            << snapshot_interval << std::endl;
      } else if (opt == "snapshot_path") {
        opts.get_engine_args().get_option("snapshot_path", snapshot_path);
        if (rmi.procid() == 0)
          logstream(LOG_EMPH) << "Engine Option: snapshot_path = "
            << snapshot_path << std::endl;
      } else if (opt == "sched_allv") {
        opts.get_engine_args().get_option("sched_allv", sched_allv);
        if (rmi.procid() == 0)
          logstream(LOG_EMPH) << "Engine Option: sched_allv = "
            << sched_allv << std::endl;
      } else {
        logstream(LOG_FATAL) << "Unexpected Engine Option: " << opt << std::endl;
      }
    }

    if (snapshot_interval >= 0 && snapshot_path.length() == 0) {
      logstream(LOG_FATAL)
        << "Snapshot interval specified, but no snapshot path" << std::endl;
    }
    INITIALIZE_EVENT_LOG;
    ADD_CUMULATIVE_EVENT(EVENT_APPLIES, "Applies", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_GATHERS , "Gathers", "Calls");
    ADD_CUMULATIVE_EVENT(EVENT_SCATTERS , "Scatters", "Calls");
    ADD_INSTANTANEOUS_EVENT(EVENT_ACTIVE_CPUS, "Active Threads", "Threads");
    graph.finalize();
    init();
  } // end of synchronous engine


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>:: init() {
    resize();
    // Clear up
    force_abort = false;
    iteration_counter = 0;
    completed_applys = 0;
    has_message.clear();
    has_gather_accum.clear();
    has_cache.clear();
    active_superstep.clear();
    active_minorstep.clear();
  }


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>:: resize() {
    memory_info::log_usage("Before Engine Initialization");
    // Allocate vertex locks and vertex programs
    vlocks.resize(graph.num_local_vertices());
    vertex_programs.resize(graph.num_local_vertices());
    // allocate the edge locks
    //elocks.resize(graph.num_local_edges());
    // Allocate messages and message bitset
    messages.resize(graph.num_local_vertices(), message_type());
    has_message.resize(graph.num_local_vertices());
    // Allocate gather accumulators and accumulator bitset
    gather_accum.resize(graph.num_local_vertices(), gather_type());
    has_gather_accum.resize(graph.num_local_vertices());

    // If caching is used then allocate cache data-structures
    if (use_cache) {
      gather_cache.resize(graph.num_local_vertices(), gather_type());
      has_cache.resize(graph.num_local_vertices());
    }
    // Allocate bitset to track active vertices on each bitset.
    active_superstep.resize(graph.num_local_vertices());
    active_minorstep.resize(graph.num_local_vertices());

    // Print memory usage after initialization
    memory_info::log_usage("After Engine Initialization");
  }


  template<typename VertexProgram>
  typename synchronous_engine<VertexProgram>::aggregator_type*
  synchronous_engine<VertexProgram>::get_aggregator() {
    return &aggregator;
  } // end of get_aggregator



  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::internal_stop() {
    for (size_t i = 0; i < rmi.numprocs(); ++i)
      rmi.remote_call(i, &synchronous_engine<VertexProgram>::rpc_stop);
  } // end of internal_stop

  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::rpc_stop() {
    force_abort = true;
  } // end of rpc_stop


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  signal(vertex_id_type gvid, const message_type& message) {
    if (vlocks.size() != graph.num_local_vertices())
      resize();
    rmi.barrier();
    internal_signal_rpc(gvid, message);
    rmi.barrier();
  } // end of signal



  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  signal_all(const message_type& message, const std::string& order) {
    if (vlocks.size() != graph.num_local_vertices())
      resize();
    for(lvid_type lvid = 0; lvid < graph.num_local_vertices(); ++lvid) {
      if(graph.l_is_master(lvid)) {
        internal_signal(vertex_type(graph.l_vertex(lvid)), message);
      }
    }
  } // end of signal all


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  signal_vset(const vertex_set& vset,
             const message_type& message, const std::string& order) {
    if (vlocks.size() != graph.num_local_vertices())
      resize();
    for(lvid_type lvid = 0; lvid < graph.num_local_vertices(); ++lvid) {
      if(graph.l_is_master(lvid) && vset.l_contains(lvid)) {
        internal_signal(vertex_type(graph.l_vertex(lvid)), message);
      }
    }
  } // end of signal all


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  internal_signal(const vertex_type& vertex,
                  const message_type& message) {
    const lvid_type lvid = vertex.local_id();
    vlocks[lvid].lock();
    if( has_message.get(lvid) ) {
      messages[lvid] += message;
    } else {
      messages[lvid] = message;
      has_message.set_bit(lvid);
    }
    vlocks[lvid].unlock();
  } // end of internal_signal


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  internal_signal_gvid(vertex_id_type gvid, const message_type& message) {
    procid_t proc = graph.master(gvid);
    if(proc == rmi.procid()) internal_signal_rpc(gvid, message);
    else rmi.remote_call(proc, 
                         &synchronous_engine<VertexProgram>::internal_signal_rpc,
                         gvid, message);
  } 

  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  internal_signal_rpc(vertex_id_type gvid,
                      const message_type& message) {
    if (graph.is_master(gvid)) {
      internal_signal(graph.vertex(gvid), message);
    }
  } // end of internal_signal_rpc





  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  internal_post_delta(const vertex_type& vertex, const gather_type& delta) {
    const bool caching_enabled = !gather_cache.empty();
    if(caching_enabled) {
      const lvid_type lvid = vertex.local_id();
      vlocks[lvid].lock();
      if( has_cache.get(lvid) ) {
        gather_cache[lvid] += delta;
      } else {
        // You cannot add a delta to an empty cache.  A complete
        // gather must have been run.
        // gather_cache[lvid] = delta;
        // has_cache.set_bit(lvid);
      }
      vlocks[lvid].unlock();
    }
  } // end of post_delta


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  internal_clear_gather_cache(const vertex_type& vertex) {
    const bool caching_enabled = !gather_cache.empty();
    const lvid_type lvid = vertex.local_id();
    if(caching_enabled && has_cache.get(lvid)) {
      vlocks[lvid].lock();
      gather_cache[lvid] = gather_type();
      has_cache.clear_bit(lvid);
      vlocks[lvid].unlock();
    }
  } // end of clear_gather_cache




  template<typename VertexProgram>
  size_t synchronous_engine<VertexProgram>::
  num_updates() const { return completed_applys.value; }

  template<typename VertexProgram>
  float synchronous_engine<VertexProgram>::
  elapsed_seconds() const { return timer::approx_time_seconds() - start_time; }

  template<typename VertexProgram>
  int synchronous_engine<VertexProgram>::
  iteration() const { return iteration_counter; }



  template<typename VertexProgram>
  size_t synchronous_engine<VertexProgram>::total_memory_usage() const {
    size_t allocated_memory = memory_info::allocated_bytes();
    rmi.all_reduce(allocated_memory);
    return allocated_memory;
  } // compute the total memory usage of the GraphLab system


  template<typename VertexProgram> execution_status::status_enum
  synchronous_engine<VertexProgram>::start() {
    if (vlocks.size() != graph.num_local_vertices())
      resize();
    completed_applys = 0;
    rmi.barrier();

    // Initialization code ==================================================
    // Reset event log counters?
    // Start the timer
    graphlab::timer timer; timer.start();
    start_time = timer::approx_time_seconds();
    iteration_counter = 0;
    force_abort = false;
    execution_status::status_enum termination_reason =
      execution_status::UNSET;
    // if (perform_init_vtx_program) {
    //   // Initialize all vertex programs
    //   run_synchronous( &synchronous_engine::initialize_vertex_programs );
    // }
    aggregator.start();
    rmi.barrier();

    if (snapshot_interval == 0) {
      graph.save_binary(snapshot_path);
    }

    float last_print = -5;
    if (rmi.procid() == 0) {
      logstream(LOG_EMPH) << "Iteration counter will only output every 5 seconds."
                        << std::endl;
    }
    // Program Main loop ====================================================
    while(iteration_counter < max_iterations && !force_abort ) {

      // Check first to see if we are out of time
      if(timeout != 0 && timeout < elapsed_seconds()) {
        termination_reason = execution_status::TIMEOUT;
        break;
      }

      bool print_this_round = (elapsed_seconds() - last_print) >= 5;

      if(rmi.procid() == 0 && print_this_round) {
        logstream(LOG_EMPH)
          << rmi.procid() << ": Starting iteration: " << iteration_counter
          << std::endl;
        last_print = elapsed_seconds();
      }
      // Reset Active vertices ----------------------------------------------
      // Clear the active super-step and minor-step bits which will
      // be set upon receiving messages
      active_superstep.clear(); active_minorstep.clear();
      has_gather_accum.clear();
      rmi.barrier();

      // Exchange Messages --------------------------------------------------
      // Exchange any messages in the local message vectors
      // rmi.cerr() << "Exchange messages..." << std::endl;
      run_synchronous( &synchronous_engine::exchange_messages );
      /**
       * Post conditions:
       *   1) only master vertices have messages
       */

      // Receive Messages ---------------------------------------------------
      // Receive messages to master vertices and then synchronize
      // vertex programs with mirrors if gather is required
      //

      // rmi.cerr() << "Receive messages..." << std::endl;
      num_active_vertices = 0;
      run_synchronous( &synchronous_engine::receive_messages );
      if (sched_allv) {
        active_minorstep.fill();
      }
      has_message.clear();
      /**
       * Post conditions:
       *   1) there are no messages remaining
       *   2) All masters that received messages have their
       *      active_superstep bit set
       *   3) All masters and mirrors that are to participate in the
       *      next gather phases have their active_minorstep bit
       *      set.
       *   4) num_active_vertices is the number of vertices that
       *      received messages.
       */

      // Check termination condition  ---------------------------------------
      size_t total_active_vertices = num_active_vertices;
      rmi.all_reduce(total_active_vertices);
      if (rmi.procid() == 0 && print_this_round)
        logstream(LOG_EMPH)
          << "\tActive vertices: " << total_active_vertices << std::endl;
      if(total_active_vertices == 0 ) {
        termination_reason = execution_status::TASK_DEPLETION;
        break;
      }


      // Execute gather operations-------------------------------------------
      // Execute the gather operation for all vertices that are active
      // in this minor-step (active-minorstep bit set).
      // rmi.cerr() << "Gathering..." << std::endl;
      run_synchronous( &synchronous_engine::execute_gathers );
      // Clear the minor step bit since only super-step vertices
      // (only master vertices are required to participate in the
      // apply step)
      active_minorstep.clear(); // rmi.barrier();
      /**
       * Post conditions:
       *   1) gather_accum for all master vertices contains the
       *      result of all the gathers (even if they are drawn from
       *      cache)
       *   2) No minor-step bits are set
       */

      // Execute Apply Operations -------------------------------------------
      // Run the apply function on all active vertices
      // rmi.cerr() << "Applying..." << std::endl;
      run_synchronous( &synchronous_engine::execute_applys );
      /**
       * Post conditions:
       *   1) any changes to the vertex data have been synchronized
       *      with all mirrors.
       *   2) all gather accumulators have been cleared
       *   3) If a vertex program is participating in the scatter
       *      phase its minor-step bit has been set to active (both
       *      masters and mirrors) and the vertex program has been
       *      synchronized with the mirrors.
       */


      // Execute Scatter Operations -----------------------------------------
      // Execute each of the scatters on all minor-step active vertices.
      run_synchronous( &synchronous_engine::execute_scatters );
      /**
       * Post conditions:
       *   1) NONE
       */
      if(rmi.procid() == 0 && print_this_round)
        logstream(LOG_EMPH) << "\t Running Aggregators" << std::endl;
      // probe the aggregator
      aggregator.tick_synchronous();

      ++iteration_counter;

      if (snapshot_interval > 0 && iteration_counter % snapshot_interval == 0) {
        graph.save_binary(snapshot_path);
      }
    }

    if (rmi.procid() == 0) {
      logstream(LOG_EMPH) << iteration_counter
                        << " iterations completed." << std::endl;
    }
    // Final barrier to ensure that all engines terminate at the same time
    double total_compute_time = 0;
    for (size_t i = 0;i < per_thread_compute_time.size(); ++i) {
      total_compute_time += per_thread_compute_time[i];
    }
    std::vector<double> all_compute_time_vec(rmi.numprocs());
    all_compute_time_vec[rmi.procid()] = total_compute_time;
    rmi.all_gather(all_compute_time_vec);

    size_t global_completed = completed_applys;
    rmi.all_reduce(global_completed);
    completed_applys = global_completed;
    rmi.cerr() << "Updates: " << completed_applys.value << "\n";
    if (rmi.procid() == 0) {
      logstream(LOG_INFO) << "Compute Balance: ";
      for (size_t i = 0;i < all_compute_time_vec.size(); ++i) {
        logstream(LOG_INFO) << all_compute_time_vec[i] << " ";
      }
      logstream(LOG_INFO) << std::endl;
    }
    rmi.full_barrier();
    // Stop the aggregator
    aggregator.stop();
    // return the final reason for termination
    return termination_reason;
  } // end of start



  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  exchange_messages(const size_t thread_id) {
    context_type context(*this, graph);
    const size_t TRY_RECV_MOD = 100;
    size_t vcount = 0;
    fixed_dense_bitset<8 * sizeof(size_t)> local_bitset; // a word-size = 64 bit
    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start =
                  shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;
      // get the bit field from has_message
      size_t lvid_bit_block = has_message.containing_word(lvid_block_start);
      if (lvid_bit_block == 0) continue;
      // initialize a word sized bitfield
      local_bitset.clear();
      local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));
      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;
        // if the vertex is not local and has a message send the
        // message and clear the bit
        if(!graph.l_is_master(lvid)) {
          sync_message(lvid, thread_id);
          has_message.clear_bit(lvid);
          // clear the message to save memory
          messages[lvid] = message_type();
        }
        if(++vcount % TRY_RECV_MOD == 0) recv_messages();
      }
    } // end of loop over vertices to send messages
    message_exchange.partial_flush();
    // Finish sending and receiving all messages
    thread_barrier.wait();
    if(thread_id == 0) message_exchange.flush();
    thread_barrier.wait();
    recv_messages();
  } // end of exchange_messages



  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  receive_messages(const size_t thread_id) {
    context_type context(*this, graph);
    const size_t TRY_RECV_MOD = 100;
    size_t vcount = 0;
    size_t nactive_inc = 0;
    fixed_dense_bitset<8 * sizeof(size_t)> local_bitset; // a word-size = 64 bit

    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start =
                  shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;
      // get the bit field from has_message
      size_t lvid_bit_block = has_message.containing_word(lvid_block_start);
      if (lvid_bit_block == 0) continue;
      // initialize a word sized bitfield
      local_bitset.clear();
      local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));

      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;

        // if this is the master of lvid and we have a message
        if(graph.l_is_master(lvid)) {
          // The vertex becomes active for this superstep
          active_superstep.set_bit(lvid);
          ++nactive_inc;
          // Pass the message to the vertex program
          vertex_type vertex = vertex_type(graph.l_vertex(lvid));
          vertex_programs[lvid].init(context, vertex, messages[lvid]);
          // clear the message to save memory
          messages[lvid] = message_type();
          if (sched_allv) continue;
          // Determine if the gather should be run
          const vertex_program_type& const_vprog = vertex_programs[lvid];
          const vertex_type const_vertex = vertex;
          if(const_vprog.gather_edges(context, const_vertex) !=
              graphlab::NO_EDGES) {
            active_minorstep.set_bit(lvid);
            sync_vertex_program(lvid, thread_id);
          }
        }
        if(++vcount % TRY_RECV_MOD == 0) recv_vertex_programs();
      }
    }

    num_active_vertices += nactive_inc;
    vprog_exchange.partial_flush();
    // Flush the buffer and finish receiving any remaining vertex
    // programs.
    thread_barrier.wait();
    if(thread_id == 0) {
      vprog_exchange.flush();
    }
    thread_barrier.wait();

    recv_vertex_programs();

  } // end of receive messages


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  execute_gathers(const size_t thread_id) {
    context_type context(*this, graph);
    const size_t TRY_RECV_MOD = 1000;
    size_t vcount = 0;
    const bool caching_enabled = !gather_cache.empty();
    timer ti;

    fixed_dense_bitset<8 * sizeof(size_t)> local_bitset; // a word-size = 64 bit

    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start =
                  shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;
      // get the bit field from has_message
      size_t lvid_bit_block = active_minorstep.containing_word(lvid_block_start);
      if (lvid_bit_block == 0) continue;
      // initialize a word sized bitfield
      local_bitset.clear();
      local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));

      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;

        bool accum_is_set = false;
        gather_type accum = gather_type();
        // if caching is enabled and we have a cache entry then use
        // that as the accum
        if( caching_enabled && has_cache.get(lvid) ) {
          accum = gather_cache[lvid];
          accum_is_set = true;
        } else {
          // recompute the local contribution to the gather
          const vertex_program_type& vprog = vertex_programs[lvid];
          local_vertex_type local_vertex = graph.l_vertex(lvid);
          const vertex_type vertex(local_vertex);
          const edge_dir_type gather_dir = vprog.gather_edges(context, vertex);
          // Loop over in edges
          size_t edges_touched = 0;
          vprog.pre_local_gather(accum);
          if(gather_dir == IN_EDGES || gather_dir == ALL_EDGES) {
            foreach(local_edge_type local_edge, local_vertex.in_edges()) {
              edge_type edge(local_edge);
              // elocks[local_edge.id()].lock();
              if(accum_is_set) { // \todo hint likely
                accum += vprog.gather(context, vertex, edge);
              } else {
                accum = vprog.gather(context, vertex, edge);
                accum_is_set = true;
              }
              ++edges_touched;
              // elocks[local_edge.id()].unlock();
            }
          } // end of if in_edges/all_edges
            // Loop over out edges
          if(gather_dir == OUT_EDGES || gather_dir == ALL_EDGES) {
            foreach(local_edge_type local_edge, local_vertex.out_edges()) {
              edge_type edge(local_edge);
              // elocks[local_edge.id()].lock();
              if(accum_is_set) { // \todo hint likely
                accum += vprog.gather(context, vertex, edge);
              } else {
                accum = vprog.gather(context, vertex, edge);
                accum_is_set = true;
              }
              // elocks[local_edge.id()].unlock();
              ++edges_touched;
            }
            INCREMENT_EVENT(EVENT_GATHERS, edges_touched);
          } // end of if out_edges/all_edges
          vprog.post_local_gather(accum);
          // If caching is enabled then save the accumulator to the
          // cache for future iterations.  Note that it is possible
          // that the accumulator was never set in which case we are
          // effectively "zeroing out" the cache.
          if(caching_enabled && accum_is_set) {
            gather_cache[lvid] = accum; has_cache.set_bit(lvid);
          } // end of if caching enabled
        }
        // If the accum contains a value for the local gather we put
        // that estimate in the gather exchange.
        if(accum_is_set) sync_gather(lvid, accum, thread_id);
        if(!graph.l_is_master(lvid)) {
          // if this is not the master clear the vertex program
          vertex_programs[lvid] = vertex_program_type();
        }

        // try to recv gathers if there are any in the buffer
        if(++vcount % TRY_RECV_MOD == 0) recv_gathers();
      }
    } // end of loop over vertices to compute gather accumulators
    per_thread_compute_time[thread_id] += ti.current_time();
    gather_exchange.partial_flush();
      // Finish sending and receiving all gather operations
    thread_barrier.wait();
    if(thread_id == 0) gather_exchange.flush();
    thread_barrier.wait();
    recv_gathers();
  } // end of execute_gathers


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  execute_applys(const size_t thread_id) {
    context_type context(*this, graph);
    const size_t TRY_RECV_MOD = 1000;
    size_t vcount = 0;
    timer ti;

    fixed_dense_bitset<8 * sizeof(size_t)> local_bitset;  // allocate a word size = 64bits
    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start =
                  shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;
      // get the bit field from has_message
      size_t lvid_bit_block = active_superstep.containing_word(lvid_block_start);
      if (lvid_bit_block == 0) continue;
      // initialize a word sized bitfield
      local_bitset.clear();
      local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));
      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;

        // Only master vertices can be active in a super-step
        ASSERT_TRUE(graph.l_is_master(lvid));
        vertex_type vertex(graph.l_vertex(lvid));
        // Get the local accumulator.  Note that it is possible that
        // the gather_accum was not set during the gather.
        const gather_type& accum = gather_accum[lvid];
        INCREMENT_EVENT(EVENT_APPLIES, 1);
        vertex_programs[lvid].apply(context, vertex, accum);
        // record an apply as a completed task
        ++completed_applys;
        // Clear the accumulator to save some memory
        gather_accum[lvid] = gather_type();
        // synchronize the changed vertex data with all mirrors
        sync_vertex_data(lvid, thread_id);
        // determine if a scatter operation is needed
        const vertex_program_type& const_vprog = vertex_programs[lvid];
        const vertex_type const_vertex = vertex;
        if(const_vprog.scatter_edges(context, const_vertex) !=
           graphlab::NO_EDGES) {
          active_minorstep.set_bit(lvid);
          sync_vertex_program(lvid, thread_id);
        } else { // we are done so clear the vertex program
          vertex_programs[lvid] = vertex_program_type();
        }
      // try to receive vertex data
        if(++vcount % TRY_RECV_MOD == 0) {
          recv_vertex_programs();
          recv_vertex_data();
        }
      }
    } // end of loop over vertices to run apply

    per_thread_compute_time[thread_id] += ti.current_time();
    vprog_exchange.partial_flush();
    vdata_exchange.partial_flush();
      // Finish sending and receiving all changes due to apply operations
    thread_barrier.wait();
    if(thread_id == 0) { 
      vprog_exchange.flush(); vdata_exchange.flush(); 
    }
    thread_barrier.wait();
    recv_vertex_programs();
    recv_vertex_data();
  } // end of execute_applys




  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  execute_scatters(const size_t thread_id) {
    context_type context(*this, graph);
    timer ti;
    fixed_dense_bitset<8 * sizeof(size_t)> local_bitset; // allocate a word size = 64 bits
    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start =
                  shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;
      // get the bit field from has_message
      size_t lvid_bit_block = active_minorstep.containing_word(lvid_block_start);
      if (lvid_bit_block == 0) continue;
      // initialize a word sized bitfield
      local_bitset.clear();
      local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));
      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;

        const vertex_program_type& vprog = vertex_programs[lvid];
        local_vertex_type local_vertex = graph.l_vertex(lvid);
        const vertex_type vertex(local_vertex);
        const edge_dir_type scatter_dir = vprog.scatter_edges(context, vertex);
				size_t edges_touched = 0;
        // Loop over in edges
        if(scatter_dir == IN_EDGES || scatter_dir == ALL_EDGES) {
          foreach(local_edge_type local_edge, local_vertex.in_edges()) {
            edge_type edge(local_edge);
            // elocks[local_edge.id()].lock();
            vprog.scatter(context, vertex, edge);
            // elocks[local_edge.id()].unlock();
          }
					++edges_touched;
        } // end of if in_edges/all_edges
        // Loop over out edges
        if(scatter_dir == OUT_EDGES || scatter_dir == ALL_EDGES) {
          foreach(local_edge_type local_edge, local_vertex.out_edges()) {
            edge_type edge(local_edge);
            // elocks[local_edge.id()].lock();
            vprog.scatter(context, vertex, edge);
            // elocks[local_edge.id()].unlock();
          }
					++edges_touched;
        } // end of if out_edges/all_edges
				INCREMENT_EVENT(EVENT_SCATTERS, edges_touched);
        // Clear the vertex program
        vertex_programs[lvid] = vertex_program_type();
      } // end of if active on this minor step
    } // end of loop over vertices to complete scatter operation

    per_thread_compute_time[thread_id] += ti.current_time();
  } // end of execute_scatters



  // Data Synchronization ===================================================
  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  sync_vertex_program(lvid_type lvid, const size_t thread_id) {
    ASSERT_TRUE(graph.l_is_master(lvid));
    const vertex_id_type vid = graph.global_vid(lvid);
    local_vertex_type vertex = graph.l_vertex(lvid);
    foreach(const procid_t& mirror, vertex.mirrors()) {
      vprog_exchange.send(mirror,
                          std::make_pair(vid, vertex_programs[lvid]));
    }
  } // end of sync_vertex_program



  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  recv_vertex_programs() {
    typename vprog_exchange_type::recv_buffer_type recv_buffer;
    while(vprog_exchange.recv(recv_buffer)) {
      for (size_t i = 0;i < recv_buffer.size(); ++i) {
        typename vprog_exchange_type::buffer_type& buffer = recv_buffer[i].buffer;
        foreach(const vid_prog_pair_type& pair, buffer) {
          const lvid_type lvid = graph.local_vid(pair.first);
          //      ASSERT_FALSE(graph.l_is_master(lvid));
          vertex_programs[lvid] = pair.second;
          active_minorstep.set_bit(lvid);
        }
      }
    }
  } // end of recv vertex programs


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  sync_vertex_data(lvid_type lvid, const size_t thread_id) {
    ASSERT_TRUE(graph.l_is_master(lvid));
    const vertex_id_type vid = graph.global_vid(lvid);
    local_vertex_type vertex = graph.l_vertex(lvid);
    foreach(const procid_t& mirror, vertex.mirrors()) {
      vdata_exchange.send(mirror, std::make_pair(vid, vertex.data()));
    }
  } // end of sync_vertex_data





  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  recv_vertex_data() {
    typename vdata_exchange_type::recv_buffer_type recv_buffer;
    while(vdata_exchange.recv(recv_buffer)) {
      for (size_t i = 0;i < recv_buffer.size(); ++i) {
        typename vdata_exchange_type::buffer_type& buffer = recv_buffer[i].buffer;
        foreach(const vid_vdata_pair_type& pair, buffer) {
          const lvid_type lvid = graph.local_vid(pair.first);
          ASSERT_FALSE(graph.l_is_master(lvid));
          graph.l_vertex(lvid).data() = pair.second;
        }
      }
    }
  } // end of recv vertex data


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  sync_gather(lvid_type lvid, const gather_type& accum, const size_t thread_id) {
    if(graph.l_is_master(lvid)) {
      vlocks[lvid].lock();
      if(has_gather_accum.get(lvid)) {
        gather_accum[lvid] += accum;
      } else {
        gather_accum[lvid] = accum;
        has_gather_accum.set_bit(lvid);
      }
      vlocks[lvid].unlock();
    } else {
      const procid_t master = graph.l_master(lvid);
      const vertex_id_type vid = graph.global_vid(lvid);
      gather_exchange.send(master, std::make_pair(vid, accum));
    }
  } // end of sync_gather

  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  recv_gathers() {
    typename gather_exchange_type::recv_buffer_type recv_buffer;
    while(gather_exchange.recv(recv_buffer)) {
      for (size_t i = 0;i < recv_buffer.size(); ++i) {
        typename gather_exchange_type::buffer_type& buffer = recv_buffer[i].buffer;
        foreach(const vid_gather_pair_type& pair, buffer) {
          const lvid_type lvid = graph.local_vid(pair.first);
          const gather_type& accum = pair.second;
          ASSERT_TRUE(graph.l_is_master(lvid));
          vlocks[lvid].lock();
          if( has_gather_accum.get(lvid) ) {
            gather_accum[lvid] += accum;
          } else {
            gather_accum[lvid] = accum;
            has_gather_accum.set_bit(lvid);
          }
          vlocks[lvid].unlock();
        }
      }
    }
  } // end of recv_gather


  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  sync_message(lvid_type lvid, const size_t thread_id) {
    ASSERT_FALSE(graph.l_is_master(lvid));
    const procid_t master = graph.l_master(lvid);
    const vertex_id_type vid = graph.global_vid(lvid);
    message_exchange.send(master, std::make_pair(vid, messages[lvid]));
  } // end of send_message




  template<typename VertexProgram>
  void synchronous_engine<VertexProgram>::
  recv_messages() {
    typename message_exchange_type::recv_buffer_type recv_buffer;
    while(message_exchange.recv(recv_buffer)) {
      for (size_t i = 0;i < recv_buffer.size(); ++i) {
        typename message_exchange_type::buffer_type& buffer = recv_buffer[i].buffer;
        foreach(const vid_message_pair_type& pair, buffer) {
          const lvid_type lvid = graph.local_vid(pair.first);
          ASSERT_TRUE(graph.l_is_master(lvid));
          vlocks[lvid].lock();
          if( has_message.get(lvid) ) {
            messages[lvid] += pair.second;
          } else {
            messages[lvid] = pair.second;
            has_message.set_bit(lvid);
          }
          vlocks[lvid].unlock();
        }
      }
    }
  } // end of recv_messages











}; // namespace


#include <graphlab/macros_undef.hpp>

#endif

