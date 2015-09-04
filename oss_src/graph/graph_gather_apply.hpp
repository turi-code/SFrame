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
#ifndef GRAPHLAB_GRAPH_GATHER_APPLY_HPP
#define GRAPHLAB_GRAPH_GATHER_APPLY_HPP

#include <deque>
#include <boost/bind.hpp>
#include <graphlab/options/graphlab_options.hpp>
#include <parallel/pthread_tools.hpp>
#include <graph/vertex_set.hpp>
#include <perf/memory_info.hpp>
#include <parallel/thread_pool.hpp>

#include <rpc/dc_dist_object.hpp>
#include <rpc/buffered_exchange.hpp>

#include <graphlab/macros_def.hpp>

namespace graphlab {
  template<typename Graph, typename GatherType>
  class graph_gather_apply {
  public:
    /**
     * \brief The type of graph supported by this vertex program
     *
     * See graphlab::distributed_graph
     */
    typedef Graph graph_type;

    /**
     * \brief Graph related types
     */
    typedef typename graph_type::vertex_type          vertex_type;
    typedef typename graph_type::vertex_data_type     vertex_data_type;
    typedef typename graph_type::mirror_type          mirror_type;
    typedef typename graph_type::local_vertex_type    local_vertex_type;
    typedef typename graph_type::local_edge_type      local_edge_type;
    typedef typename graph_type::lvid_type            lvid_type;


    /**
     * \brief The result type of the gather operation.
     */
    typedef GatherType         gather_type; 

    /**
     * \brief The type of the gather function. 
     */
    typedef typename boost::function<gather_type (lvid_type, graph_type&)> gather_fun_type;

    /**
     * \brief The gather operation which will be called on each vertex (master and mirrors) and send to the master vertex.
     */
    gather_fun_type gather_fun;

    /**
     * \brief The type of the apply function. 
     */
    typedef typename boost::function<void (lvid_type, const gather_type& accum, graph_type&)> apply_fun_type;

    /**
     * \brief The apply operation which will be called on each vertex (master and mirrors) with the result of gather.
     */
    apply_fun_type apply_fun;

  private:

    /**
     * \brief The object used to communicate with remote copies of the
     * synchronous engine.
     */
    dc_dist_object< graph_gather_apply<Graph,GatherType> > rmi;

    /**
     * \brief A reference to the distributed graph on which this
     * synchronous engine is running.
     */
    graph_type& graph;

    /**
     * \brief The local worker threads used by this engine
     */
    thread_pool threads;

    /**
     * \brief A thread barrier that is used to control the threads in the
     * thread pool.
     */
    graphlab::barrier thread_barrier;

    /**
     * \brief The shared counter used coordinate operations between
     * threads.
     */
    atomic<size_t> shared_lvid_counter;

    /**
     * \brief  The vertex locks protect access to vertex specific data-structrues including
     * \ref graphlab::graph_gather_apply::gather_accum.
     */
    std::vector<simple_spinlock> vlocks;

    /**
     * \brief Bit indicating if the gather has accumulator contains any values.
     *
     * While dense bitsets are thread safe the value of this bit must change concurrently with 
     * the \ref graphlab::graph_gather_apply and therefore is set while holding the lock in 
     * \ref graphlab::graph_gather_apply::vlocks
     */
    dense_bitset has_gather_accum;
    

    /**
     * \brief Gather accumulator used for each master vertex to merge the result of all the machine
     * specific accumulators.
     *
     * The gather accumulator can be accessed by multiple threads at once and therefore must be guarded 
     * by a vertex locks in \ref graphlab::graph_gather_apply::vlocks
     */
    std::vector<gather_type> gather_accum;


    /**
     * \brief The pair type used to synchronize the results of the gather phase
     */
    typedef std::pair<vertex_id_type, gather_type> vid_gather_pair_type;

    /**
     * \brief The type of the exchange used to synchronize gather
     * accumulators
     */
    typedef buffered_exchange<vid_gather_pair_type> gather_exchange_type;

    /**
     * \brief The distributed exchange used to synchronize gather
     * accumulators.
     */
    gather_exchange_type gather_exchange;


  public:

    /**
     * \brief Construct a graph gather_apply operation with a given graph and 
     * gather apply functions.
     *
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
    graph_gather_apply(graph_type& graph,
                       gather_fun_type gather_fun,
                       apply_fun_type apply_fun,
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
    void exec(const vertex_set& vset = vertex_set(true));

  private:
    // Program Steps ==========================================================
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
     * void graph_gather_apply::member_fun(size_t threadid, const vertex_set& vset);
     * \endcode
     *
     * This function runs an rmi barrier after termination
     *
     * @tparam the type of the member function.
     * @param [in] member_fun the function to call.
     */
    template<typename MemberFunction>
    void run_synchronous(MemberFunction member_fun, const vertex_set& vset) {
      shared_lvid_counter = 0;
      if (threads.size() <= 1) {
        (this->*(member_fun))(0, vset);
      }
      else {
        // launch the initialization threads
        for(size_t i = 0; i < threads.size(); ++i) {
          boost::function<void(void)> invoke = boost::bind(member_fun, this, i, vset);
          threads.launch(invoke, i);
        }
      }
      // Wait for all threads to finish
      threads.join();
      rmi.barrier();
    } // end of run_synchronous

    /**
     * \brief Execute the \ref graphlab::graph_gather_apply::gather_fun function on all
     * vertices in the vset. The result of the gather will be send to the master proc of the vertex. 
     *
     * The accumulators are stored in \ref graphlab::graph_gather_apply::gather_accum if the 
     * the vertex is pre-allocated in the local graph of the proc. Otherwise, it wil be stored in
     * \ref graphlab::graph_gather_apply::temporary_gather_map and be merged in the graph.
     * 
     * @param thread_id the thread to run this as which determines
     * @param vset the vertex set specifying the set of vertex to run the gather operation. 
     */
    void execute_gathers(const size_t thread_id, const vertex_set& vset);


    /**
     * \brief Scatter the gather accumulator from master to the mirrors.
     *
     * @param thread_id the thread to run this as which determines
     * @param vset the vertex set specifying the set of vertex to run the gather operation. 
     */
    void execute_scatters(const size_t thread_id, const vertex_set& vset);

    /**
     * \brief Execute the \ref graphlab::graph_gahter_apply::apply_fun function on all
     * all vertices (masters and mirrors) with the synrhonized gather accumulators.
     *
     * @param thread_id the thread to run this as which determines
     * @param vset the vertex set specifying the set of vertex to run the gather operation. 
     */
    void execute_applys(const size_t thread_id, const vertex_set& vset);


    // Data Synchronization ===================================================
    /**
     * \brief Send the gather value for the vertex id to its master.
     *
     * @param [in] lvid the vertex to send the gather value to
     * @param [in] accum the locally computed gather value.
     */
    void sync_gather(lvid_type lvid, const gather_type& accum, size_t thread_id);

    /**
     * \brief Receive the gather values from the buffered exchange.
     *
     * This function returns when there is nothing left in the
     * buffered exchange and should be called after the buffered
     * exchange has been flushed
     */
    void recv_gathers(const bool try_to_recv = false);


    /**
     * \brief Send the gather values from master to mirrors.
     */
    void scatter_gather(lvid_type lvid, const gather_type& accum, size_t thread_id);
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
  template<typename Graph, typename GatherType>
  graph_gather_apply<Graph,GatherType>::graph_gather_apply(
      graph_type& graph,
      gather_fun_type gather_fun,
      apply_fun_type apply_fun,
      const graphlab_options& opts) :
    gather_fun(gather_fun), apply_fun(apply_fun), rmi(graph.dc(), this), graph(graph),
    threads(opts.get_ncpus()),
    thread_barrier(opts.get_ncpus()),
    gather_exchange(graph.dc(), opts.get_ncpus()) { } 


  template<typename Graph, typename GatherType> 
      void graph_gather_apply<Graph,GatherType>::exec(const vertex_set& vset) {
        if (vset.lazy && !vset.is_complete_set)
          return;

        gather_accum.clear();
        // Allocate vertex locks and vertex programs
        vlocks.resize(graph.num_local_vertices());
        // Allocate gather accumulators and accumulator bitset
        gather_accum.resize(graph.num_local_vertices(), gather_type());
        has_gather_accum.resize(graph.num_local_vertices());
        has_gather_accum.clear();
        rmi.barrier();

        // Execute gather operations-------------------------------------------
        // Execute the gather operation for all vertices that are active
        // in this minor-step (active-minorstep bit set).
        // if (rmi.procid() == 0) std::cerr << "Gathering..." << std::endl;
        run_synchronous(&graph_gather_apply::execute_gathers, vset);

        // Execute the gather operation for all vertices that are active
        // in this minor-step (active-minorstep bit set).
        // if (rmi.procid() == 0) std::cerr << "Gathering..." << std::endl;
        run_synchronous(&graph_gather_apply::execute_scatters, vset);


        // Execute Apply Operations -------------------------------------------
        // Run the apply function on all active vertices
        // if (rmi.procid() == 0) std::cerr << "Applying..." << std::endl;
        run_synchronous(&graph_gather_apply::execute_applys, vset);

        /**
         * Post conditions:
         *   1) any changes to the vertex data have been synchronized
         *      with all mirrors.
         *   2) all gather accumulators have been cleared
         */

        // Final barrier to ensure that all engines terminate at the same time
        rmi.full_barrier();
  } // end of start


  template<typename Graph, typename GatherType>
  void graph_gather_apply<Graph, GatherType>::
  execute_gathers(const size_t thread_id, const vertex_set& vset) {
    const bool TRY_TO_RECV = true;
    const size_t TRY_RECV_MOD = 1000;
    size_t vcount = 0;
    timer ti;

    fixed_dense_bitset<8 * sizeof(size_t)> local_bitset;
    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start =
                  shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;

      local_bitset.clear();

      if (vset.lazy)  {
        ASSERT_TRUE(vset.is_complete_set);
        local_bitset.fill();
      } else {
        // get the bit field from has_message
        size_t lvid_bit_block = vset.localvset.containing_word(lvid_block_start);
        if (lvid_bit_block == 0) continue;
        // initialize a word sized bitfield
        local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));
      }

      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;

        // std::cerr << "proc " << rmi.procid() << " gather on lvid " << lvid << std::endl;
        gather_type accum = gather_fun(lvid, graph);

        // If the accum contains a value for the local gather we put
        // that estimate in the gather exchange.
        sync_gather(lvid, accum, thread_id);

        // try to recv gathers if there are any in the buffer
        if(++vcount % TRY_RECV_MOD == 0) recv_gathers(TRY_TO_RECV);
      }
    } // end of loop over vertices to compute gather accumulators
    gather_exchange.partial_flush(thread_id);
      // Finish sending and receiving all gather operations
    thread_barrier.wait();
    if(thread_id == 0) gather_exchange.flush();
    thread_barrier.wait();
    recv_gathers();
  } // end of execute_gathers
  
  template<typename Graph, typename GatherType>
  void graph_gather_apply<Graph, GatherType>::
  execute_scatters(const size_t thread_id, const vertex_set& vset) {
    const bool TRY_TO_RECV = true;
    const size_t TRY_RECV_MOD = 1000;
    size_t vcount = 0;
    timer ti;

    fixed_dense_bitset<8 * sizeof(size_t)> local_bitset;
    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start =
                  shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;

      local_bitset.clear();

      if (vset.lazy)  {
        ASSERT_TRUE(vset.is_complete_set);
        local_bitset.fill();
      } else {
        // get the bit field from has_message
        size_t lvid_bit_block = vset.localvset.containing_word(lvid_block_start);
        if (lvid_bit_block == 0) continue;
        // initialize a word sized bitfield
        local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));
      }

      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;

        if (graph.l_is_master(lvid)) {
          const gather_type& accum = gather_accum[lvid];
          apply_fun(lvid, accum, graph);
          scatter_gather(lvid, accum, thread_id);
          // try to recv gathers if there are any in the buffer
          if(++vcount % TRY_RECV_MOD == 0) recv_gathers(TRY_TO_RECV);
        }
      }
    } // end of loop over vertices to compute gather accumulators
    gather_exchange.partial_flush(thread_id);
      // Finish sending and receiving all gather operations
    thread_barrier.wait();
    if(thread_id == 0) gather_exchange.flush();
    thread_barrier.wait();
    recv_gathers();
  } // end of execute_gathers



  template<typename Graph, typename GatherType>
  void graph_gather_apply<Graph,GatherType>::
  execute_applys(const size_t thread_id, const vertex_set& vset) {
    fixed_dense_bitset<8  * sizeof(size_t)> local_bitset;
    while (1) {
      // increment by a word at a time
      lvid_type lvid_block_start = shared_lvid_counter.inc_ret_last(8 * sizeof(size_t));
      if (lvid_block_start >= graph.num_local_vertices()) break;

      if (vset.lazy)  {
        ASSERT_TRUE(vset.is_complete_set);
        local_bitset.fill();
      } else {
        // get the bit field from has_message
        size_t lvid_bit_block = vset.localvset.containing_word(lvid_block_start);
        if (lvid_bit_block == 0) continue;
        // initialize a word sized bitfield
        local_bitset.initialize_from_mem(&lvid_bit_block, sizeof(size_t));
      }

      foreach(size_t lvid_block_offset, local_bitset) {
        lvid_type lvid = lvid_block_start + lvid_block_offset;
        if (lvid >= graph.num_local_vertices()) break;

        if (graph.l_is_master(lvid))
          continue;

        // Master and mirror vertices both should perform the apply step. 
        // vertex_type vertex(graph.l_vertex(lvid));

        // Get the local accumulator.  Note that it is possible that
        // the gather_accum was not set during the gather.
        const gather_type& accum = gather_accum[lvid];

        apply_fun(lvid, accum, graph);
     }
    } // end of loop over vertices to run apply
  } // end of execute_applys


  // Data Synchronization ===================================================
  template<typename Graph, typename GatherType>
  void graph_gather_apply<Graph,GatherType>::
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
      gather_exchange.send(master, std::make_pair(vid, accum), thread_id);
    }
  } // end of sync_gather

  template<typename Graph, typename GatherType>
  void graph_gather_apply<Graph, GatherType>::
  scatter_gather(lvid_type lvid, const gather_type& accum, const size_t thread_id) {
    ASSERT_TRUE(graph.l_is_master(lvid));
    const vertex_id_type vid = graph.global_vid(lvid);
    local_vertex_type vertex = graph.l_vertex(lvid);
    foreach(const procid_t& mirror, vertex.mirrors()) {
      gather_exchange.send(mirror, std::make_pair(vid, accum), thread_id);
    }
  } // end of sync_gather

  template<typename Graph, typename GatherType>
  void graph_gather_apply<Graph,GatherType>::
  recv_gathers(const bool try_to_recv) {
    procid_t procid(-1);
    typename gather_exchange_type::buffer_type buffer;
    while(gather_exchange.recv(procid, buffer, try_to_recv)) {
      foreach(const vid_gather_pair_type& pair, buffer) {
        ASSERT_TRUE(graph.vid2lvid.find(pair.first) != graph.vid2lvid.end());
        const lvid_type lvid = graph.local_vid(pair.first);
        const gather_type& accum = pair.second;
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
  } // end of recv_gather
}; // namespace
#include <graphlab/macros_undef.hpp>
#endif


// Remove the capability of merging flying gather accumulators.


    /**
     * \brief Temporary map storing the gather accumulators that is not preallocated in the 
     * \ref graphlab::graph_gather_apply::gather_accum. The key of the map is the global vertex id.
     *
     *  This map can be accessed by multiple threads at once and therefore must be guarded by a 
     *  lock \ref graphlab::graph_gather_apply::tmp_gather_map_lock.
     */
    // boost::unordered_map<vertex_id_type, gather_type> tmp_gather_map;


    /**
     * \brief Lock that protects access to the temporaroy gather accumulator map \ref graphlab::graph_gather_apply::tmp_gather_map. 
     */
    // mutex tmp_gather_map_lock;

    // /**
    //  * \brief Merge the gather accumulators in \ref graphlab::graph_gather_apply::tmp_gather_accum 
    //  * in to \ref graphlab::graph_gather_apply::gahter_accum. Resize the local graph and the associated 
    //  * data structures (\ref graphlab::distributed_graph::vid2lvid, 
    //  * \ref graphlab::distributed_graph::lvid2record)with the new vertex.
    //  */
    // void merge_temporary_gather_map();

  // template<typename Graph, typename GatherType>
  // void graph_gather_apply<Graph, GatherType>::
  // merge_temporary_gather_map() {
  //       // merge in the tmp_gather_map
  //       typename boost::unordered_map<vertex_id_type, gather_type>::const_iterator 
  //           it = tmp_gather_map.begin();

  //       // resize the graph
  //       size_t new_size = graph.num_local_vertices() + tmp_gather_map.size();
  //       gather_accum.resize(new_size);
  //       graph.lvid2record.resize(new_size);
  //       graph.local_graph.resize(new_size);
  //       for (; it != tmp_gather_map.end(); ++it) {
  //         lvid_type lvid = graph.vid2lvid.size();

  //         // update graph
  //         graph.vid2lvid[it->first] = lvid;
  //         graph.lvid2record[lvid].gvid = it->first;
  //         graph.lvid2record[lvid].owner = rmi.procid();

  //         // update gather accum vector
  //         gather_accum[lvid] = it->second;
  //       }
  //       tmp_gather_map.clear();
  // }


  // // Merge in the temporaroy gather map-------------------------------------------
  // // with the side effect of resizing the graph and associate vertex datastructures.
  // merge_temporary_gather_map();


  // if the vid does not exist on the proc, put in a temporary map and merge in later
  // if (graph.vid2lvid.find(pair.first) == graph.vid2lvid.end()) {
  //   tmp_gather_map_lock.lock();
  //   if (tmp_gather_map.find(pair.first) == tmp_gather_map.end()) {
  //     tmp_gather_map[pair.first] = pair.second;
  //   } else {
  //     tmp_gather_map[pair.first] += pair.second;
  //   }
  //   tmp_gather_map_lock.unlock();
  // } else {

