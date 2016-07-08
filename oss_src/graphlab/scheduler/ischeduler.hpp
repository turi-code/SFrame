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

/**
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved.
 *
 * Contributed under the iCLA for:
 *    Joseph Gonzalez (jegonzal@yahoo-inc.com)
 *
 */


#ifndef GRAPHLAB_ISCHEDULER_HPP
#define GRAPHLAB_ISCHEDULER_HPP

#include <vector>
#include <sstream>
#include <ostream>

#include <graph/graph_basic_types.hpp>

#include <graphlab/options/graphlab_options.hpp>



namespace graphlab {

  /**
   * This is an enumeration for the possible return values for
   * get_next_tasks
   */
  struct sched_status {
    /// \brief the possible scheduler status.
    enum status_enum {
      NEW_TASK,      /**< The get_next_tasks function returned a new task
                        to be executed */
      EMPTY,         /**< The schedule is empty. */
    };
  };

  /**
   * \ingroup group_schedulers
   *
   * This describes the interface/concept for a scheduler. 
   * The scheduler allows vertices to be scheduled, but deduplicates
   * repeated schedulings of the same vertex. The only guarantee is that
   * if a vertex is scheduled, the vertex will be popped at some point in
   * the future.
   * Note that all functions (with the exception of the
   * constructor and destructor and set_num_vertices()) must be thread-safe.
   */
  class ischeduler {
  public:

    /// destructor
    virtual ~ischeduler() {};

    /** Sets the number of vertices in the graph. Existing schedule
     * will not be cleared. Scheduler will not return a vertex ID
     * exceeding the number of vertices.
     */
    virtual void set_num_vertices(const lvid_type numv) = 0;

    /**
     * Adds vertex vid to the schedule. The new priority is the priority value
     */
    virtual void schedule(const lvid_type vid, double priority = 1) = 0;


    /**
     * This function is called by the engine to ask for the next
     * vertex to process.  The vertex is 
     * returned in ret_msg and ret_vid respectively.
     *
     *  \retval NEWTASK There is a new message to process
     *  \retval EMPTY There are no messages to process
     */
    virtual sched_status::status_enum
    get_next(const size_t cpuid, lvid_type& ret_vid) = 0;

    /// returns true if the scheduler is empty. Need not be consistent.
    virtual bool empty() = 0;

    /**
     * Print a help string describing the options that this scheduler
     * accepts.
     */
    static void print_options_help(std::ostream& out) { };

  };

}
#endif

