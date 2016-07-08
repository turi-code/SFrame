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

#ifndef GRAPHLAB_SWEEP_SCHEDULER_HPP
#define GRAPHLAB_SWEEP_SCHEDULER_HPP

#include <queue>
#include <cmath>
#include <cassert>

#include <graph/graph_basic_types.hpp>
#include <graphlab/scheduler/ischeduler.hpp>
#include <util/dense_bitset.hpp>
#include <parallel/atomic.hpp>
#include <graphlab/options/graphlab_options.hpp>

#include <graphlab/macros_def.hpp>

namespace graphlab {

   /** \ingroup group_schedulers
    */
  class sweep_scheduler: public ischeduler {
  private:

    size_t ncpus;

    size_t num_vertices;
    bool strict_round_robin;
    atomic<size_t> rr_index;
    size_t max_iterations;
    size_t randomizer;

    std::vector<lvid_type>             cpu2index;

    dense_bitset vertex_is_scheduled;
    std::string                             ordering;

    void set_options(const graphlab_options& opts);

  public:
    sweep_scheduler(size_t num_vertices,
                    const graphlab_options& opts);

    void set_num_vertices(const lvid_type numv);

    void schedule(const lvid_type vid, double priority = 1 /* ignored */) ; 


    
    sched_status::status_enum get_next(const size_t cpuid, lvid_type& ret_vid);
    
    
    static void print_options_help(std::ostream &out) {
      out << "order = [string: {random, ascending} default=random]\n"
          << "strict = [bool, use strict round robin schedule, default=true]\n"
          << "max_iterations = [integer, maximum number of iterations "
          << " (requires strict=true) \n"
          << "\t default = inf]\n";
    } // end of print_options_help


    bool empty() {
      return (vertex_is_scheduled.popcount() == 0);
    }

  private:
    inline size_t get_and_inc_index(const size_t cpuid) {
      if (strict_round_robin) { 
        return rr_index++ % num_vertices; 
      } else {
        const size_t index = cpu2index[cpuid];
        cpu2index[cpuid] += ncpus;
        // Address loop around
        if (__builtin_expect(cpu2index[cpuid] >= num_vertices, false)) 
          cpu2index[cpuid] = cpuid;
        return index;
      }
    }// end of next index

  };


} // end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif

