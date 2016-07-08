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


#include <graphlab/scheduler/sweep_scheduler.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

void sweep_scheduler::set_options(const graphlab_options& opts) {
  std::vector<std::string> keys = opts.get_scheduler_args().get_option_keys();
  bool max_iter_set = false;
  foreach(std::string opt, keys) {
    if (opt == "order") {
      opts.get_scheduler_args().get_option("order", ordering);
      ASSERT_TRUE(ordering == "random" || ordering == "ascending");
    } else if (opt == "strict") {
      opts.get_scheduler_args().get_option("strict", strict_round_robin);
    } else if (opt == "max_iterations") {
      opts.get_scheduler_args().get_option("max_iterations", max_iterations);
      max_iter_set = true;
    } else {
      logstream(LOG_FATAL) << "Unexpected Scheduler Option: " << opt << std::endl;
    }
  }

  if (max_iter_set) {
    ASSERT_MSG(strict_round_robin, 
               "sweep_scheduler: \"strict\" must be set with \"max_iteration\"");
  }
}

sweep_scheduler::sweep_scheduler(size_t num_vertices,
                                 const graphlab_options& opts) :
    ncpus(opts.get_ncpus()),
    num_vertices(num_vertices),
    strict_round_robin(true),
    max_iterations(std::numeric_limits<size_t>::max()),
    vertex_is_scheduled(num_vertices) {
  // initialize defaults
  ASSERT_GE(opts.get_ncpus(), 1);
  ordering = "random";
  set_options(opts);

  if (ordering == "ascending") {
    randomizer = 1;
  } else if(ordering == "random") {
    randomizer = 1500450271;
  }

  if(strict_round_robin) {
    logstream(LOG_INFO)
        << "Using a strict round robin schedule." << std::endl;
    // Max iterations only applies to strict round robin
    if(max_iterations != std::numeric_limits<size_t>::max()) {
      logstream(LOG_INFO)
          << "Using maximum iterations: " << max_iterations << std::endl;
    }
    rr_index = 0;
  } else {
    // each cpu is responsible for its own subset of vertices
    // Initialize the cpu2index counters
    cpu2index.resize(ncpus);
    for(size_t i = 0; i < cpu2index.size(); ++i) cpu2index[i] = i;
  }
  vertex_is_scheduled.resize(num_vertices);
} // end of constructor


void sweep_scheduler::set_num_vertices(const lvid_type numv) {
  num_vertices = numv;
  vertex_is_scheduled.resize(numv);
}

void sweep_scheduler::schedule(const lvid_type vid, double priority) {      
  if (vid < num_vertices) vertex_is_scheduled.set_bit(vid);
} 


sched_status::status_enum sweep_scheduler::get_next(const size_t cpuid,
                                                    lvid_type& ret_vid) {         
  const size_t max_fails = (num_vertices/ncpus) + 1;
  // Check to see if max iterations have been achieved 
  if(strict_round_robin && (rr_index / num_vertices) >= max_iterations) 
    return sched_status::EMPTY;
  // Loop through all vertices that are associated with this
  // processor searching for a vertex with an active task
  for(size_t idx = get_and_inc_index(cpuid), fails = 0; 
      fails <= max_fails; // 
      idx = get_and_inc_index(cpuid), ++fails) {
    // It is possible that the get_and_inc_index could return an
    // invalid index if the number of cpus exceeds the number of
    // vertices.  In This case we alwasy return empty
    if(__builtin_expect(idx >= num_vertices, false)) return sched_status::EMPTY;
    const lvid_type vid = (idx * randomizer) % num_vertices;
    bool success = vertex_is_scheduled.clear_bit(vid);
    while(success) { // Job found now decide whether to keep it
      ret_vid = vid; 
      return sched_status::NEW_TASK;
    }
  } // end of for loop
  return sched_status::EMPTY;
} // end of get_next


}
