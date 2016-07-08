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


#include <graphlab/scheduler/fifo_scheduler.hpp>

#include <graphlab/macros_def.hpp>
namespace graphlab {

void fifo_scheduler::set_options(const graphlab_options& opts) {
  ncpus = opts.get_ncpus();
  std::vector<std::string> keys = opts.get_scheduler_args().get_option_keys();
  foreach(std::string opt, keys) {
    if (opt == "multi") {
      opts.get_scheduler_args().get_option("multi", multi);
    }  else {
      logstream(LOG_FATAL) << "Unexpected Scheduler Option: " << opt << std::endl;
    }
  }
}

// Initializes the internal datastructures
void fifo_scheduler::initialize_data_structures() {
  current_queue.resize(ncpus, 0);
  size_t nqueues = std::max(multi * current_queue.size(), size_t(1));
  queues.resize(nqueues);
  locks.resize(nqueues);
  vertex_is_scheduled.resize(num_vertices);
}

fifo_scheduler::fifo_scheduler(size_t num_vertices,
                               const graphlab_options& opts):
     multi(3), num_vertices(num_vertices) { 
  ASSERT_GE(opts.get_ncpus(), 1);
  set_options(opts);
  initialize_data_structures();
}


void fifo_scheduler::set_num_vertices(const lvid_type numv) {
  num_vertices = numv;
  vertex_is_scheduled.clear();
  vertex_is_scheduled.resize(numv);
}

void fifo_scheduler::schedule(const lvid_type vid, double priority) {
  if (vid < num_vertices && !vertex_is_scheduled.set_bit(vid)) {
    /* "Randomize" the task queue task is put in. Note that we do
       not care if this counter is corrupted in race conditions
       Find first queue that is not locked and put task there (or
       after iteration limit) Choose two random queues and use the
       one which has smaller size */
    // M.D. Mitzenmacher The Power of Two Choices in Randomized
    // Load Balancing (1991)
    // http://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.
    size_t idx = 0;
    if(queues.size() > 1) {
      const uint32_t prod = 
          random::fast_uniform(uint32_t(0), 
                               uint32_t(queues.size() * queues.size() - 1));
      const uint32_t r1 = prod / queues.size();
      const uint32_t r2 = prod % queues.size();
      idx = (queues[r1].size() < queues[r2].size()) ? r1 : r2;  
    }
    locks[idx].lock(); queues[idx].push_back(vid); locks[idx].unlock();
  }
}

/** Get the next element in the queue */
sched_status::status_enum fifo_scheduler::get_next(const size_t cpuid,
                                                   lvid_type& ret_vid) {
  /* Check all of my queues for a task */
  // begin scanning from the machine's current queue
  size_t initial_idx = (current_queue[cpuid] % multi) + cpuid * multi;
  for(size_t i = 0; i < queues.size(); ++i) {
    const size_t idx = (initial_idx + i) % queues.size();
    // increment the current queue as long as I am scanning with in the 
    // queues owned by this machine
    current_queue[cpuid] += (i < multi);

    // pick up the lock
    bool good = false;
    locks[idx].lock();
    while(!queues[idx].empty()) {
      // not empty, pop and verify
      ret_vid = queues[idx].front();
      queues[idx].pop_front();
      if (ret_vid < num_vertices) {
        good = vertex_is_scheduled.clear_bit(ret_vid);
        if (good) break;
      }
    }
    locks[idx].unlock();
    // managed to retrieve a task
    if(good) {
      return sched_status::NEW_TASK;
    }
  }
  return sched_status::EMPTY;     
} // end of get_next_task


bool fifo_scheduler::empty() {
  for (size_t i = 0;i < queues.size(); ++i) {
    if (!queues[i].empty()) return false;
  }
  return true;
}

}
