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


#ifndef GRAPHLAB_QUEUED_FIFO_SCHEDULER_HPP
#define GRAPHLAB_QUEUED_FIFO_SCHEDULER_HPP

#include <algorithm>
#include <queue>


#include <graph/graph_basic_types.hpp>
#include <parallel/pthread_tools.hpp>
#include <parallel/atomic.hpp>

#include <util/dense_bitset.hpp>

#include <random/random.hpp>
#include <graphlab/scheduler/ischeduler.hpp>
#include <graphlab/options/graphlab_options.hpp>


#include <graphlab/macros_def.hpp>
namespace graphlab {

  /**
   * \ingroup group_schedulers
   *
   * This class defines a multiple queue approximate fifo scheduler.
   * Each processor has its own in_queue which it puts new tasks in
   * and out_queue which it pulls tasks from.  Once a processors
   * in_queue gets too large, the entire queue is placed at the end of
   * the shared master queue.  Once a processors out queue is empty it
   * grabs the next out_queue from the master.
   */
  class queued_fifo_scheduler: public ischeduler {
  
  public:

    typedef std::deque<lvid_type> queue_type;

  private:
    size_t ncpus;
    size_t num_vertices;
    size_t multi;
    dense_bitset vertex_is_scheduled;
    std::deque<queue_type> master_queue;
    mutex master_lock;
    size_t sub_queue_size;
    std::vector<queue_type> in_queues;
    std::vector<mutex> in_queue_locks;
    std::vector<queue_type> out_queues;
    std::vector<mutex> out_queue_locks;

    void set_options(const graphlab_options& opts);
    
    void initialize_data_structures();
  public:

    queued_fifo_scheduler(size_t num_vertices,
                          const graphlab_options& opts); 

    void set_num_vertices(const lvid_type numv);

    void schedule(const lvid_type vid, double priority = 1 /* ignored */);
    
    /** Get the next element in the queue */
    sched_status::status_enum get_next(const size_t cpuid,
                                       lvid_type& ret_vid);


    bool empty();

    /**
     * Print a help string describing the options that this scheduler
     * accepts.
     */
    static void print_options_help(std::ostream& out) {
      out << "\t queuesize: [the size at which a subqueue is "
          << "placed in the master queue. default = 100]\n";
      out << "\t multi = [number of queues per thread. Default = 3].\n";
    }


  };


} // end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif

