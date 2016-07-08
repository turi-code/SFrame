/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*  
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


#include <graphlab/scheduler/queued_fifo_scheduler.hpp>
#include <graphlab/macros_def.hpp>

namespace graphlab {

void queued_fifo_scheduler::set_options(const graphlab_options& opts) {
  // read the remaining options.
  std::vector<std::string> keys = opts.get_scheduler_args().get_option_keys();
  foreach(std::string opt, keys) {
    if (opt == "queuesize") {
      opts.get_scheduler_args().get_option("queuesize", sub_queue_size);
    } else if (opt == "multi") {
      opts.get_scheduler_args().get_option("multi", multi);
    } else {
      logstream(LOG_FATAL) << "Unexpected Scheduler Option: " << opt << std::endl;
    }
  }
}

void queued_fifo_scheduler::initialize_data_structures() {
  ASSERT_GT(ncpus * multi, 1);
  in_queues.resize(ncpus * multi);
  in_queue_locks.resize(ncpus * multi);
  out_queue_locks.resize(ncpus * multi);
  out_queues.resize(ncpus);
  vertex_is_scheduled.resize(num_vertices);
}

queued_fifo_scheduler::queued_fifo_scheduler(size_t num_vertices,
                                             const graphlab_options& opts) :
    ncpus(opts.get_ncpus()),
    num_vertices(num_vertices),
    multi(3),
    sub_queue_size(100) {
      ASSERT_GE(opts.get_ncpus(), 1);
      set_options(opts);
      initialize_data_structures();
    }

void queued_fifo_scheduler::set_num_vertices(const lvid_type numv) {
  num_vertices = numv;
  vertex_is_scheduled.resize(numv);
}

void queued_fifo_scheduler::schedule(const lvid_type vid, double priority) {
  // If this is a new message, schedule it
  // the min priority will be taken care of by the get_next function
  if (vid < num_vertices && !vertex_is_scheduled.set_bit(vid)) {
    const size_t cpuid= 
        random::fast_uniform(size_t(0), 
                             in_queues.size() - 1);
    in_queue_locks[cpuid].lock();
    queue_type& queue = in_queues[cpuid];
    queue.push_back(vid);
    if(queue.size() > sub_queue_size) {
      master_lock.lock();
      queue_type emptyq;
      master_queue.push_back(emptyq);
      master_queue.back().swap(queue);
      master_lock.unlock();
    }
    in_queue_locks[cpuid].unlock();
  } 
} // end of schedule

/** Get the next element in the queue */
sched_status::status_enum queued_fifo_scheduler::get_next(const size_t cpuid,
                                                          lvid_type& ret_vid) {
  queue_type& myqueue = out_queues[cpuid];
  // if the local queue is empty try to get a queue from the master
  out_queue_locks[cpuid].lock();
  if(myqueue.empty()) {
    master_lock.lock();
    // if master queue is empty... 
    if (!master_queue.empty()) {
      myqueue.swap(master_queue.front());
      master_queue.pop_front();
      master_lock.unlock();
    }
    else {
      master_lock.unlock();
      //try to steal from the inqueues
      for (size_t i = 0; i < in_queues.size(); ++i) {
        size_t idx = (i + multi * cpuid) % in_queues.size();
        if (!in_queues[idx].empty()) {
          in_queue_locks[idx].lock();
          // double check
          if(!in_queues[idx].empty()) {
            myqueue.swap(in_queues[idx]);
          }
          in_queue_locks[idx].unlock();
          if (!myqueue.empty()) break;
        } 
      }
    }
  }
  // end of get next
  bool good = false;
  while(!myqueue.empty()) {
    // not empty, pop and verify
    ret_vid = myqueue.front();
    myqueue.pop_front();
    if (ret_vid < num_vertices) {
      good = vertex_is_scheduled.clear_bit(ret_vid);
      if (good) break;
    }
  }
  out_queue_locks[cpuid].unlock();

  if(good) {
    return sched_status::NEW_TASK;
  } else {
    return sched_status::EMPTY;
  }
} // end of get_next_task


bool queued_fifo_scheduler::empty() {
  for (size_t i = 0;i < out_queues.size(); ++i) {
    if (!out_queues[i].empty()) return false;
  }
  if (!master_queue.empty()) return false;
  for (size_t i = 0;i < in_queues.size(); ++i) {
    if (!in_queues[i].empty()) return false;
  }
  return true;
}

} // namespace graphlab
