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


#ifndef GRAPHLAB_SCHEDULER_LIST_HPP
#define GRAPHLAB_SCHEDULER_LIST_HPP
#include <string>
#include <vector>
#include <iostream>
#include <boost/preprocessor.hpp>

#define __SCHEDULER_LIST__                                              \
  (("fifo", fifo_scheduler,                                             \
    "Standard FIFO task queue, poor parallelism, but task evaluation "  \
    "sequence is highly predictable. "                                  \
    "Useful for debugging and testing."))                               \
  (("sweep", sweep_scheduler,                                           \
    "very fast dynamic scheduler. Scans all vertices in sequence, "     \
    "running all update tasks on each vertex evaluated."))              \
  (("priority", priority_scheduler,                                     \
    "Standard Priority queue, poor parallelism, but task evaluation "   \
    "sequence is highly predictable. Useful for debugging"))            \
  (("queued_fifo", queued_fifo_scheduler,                               \
    "This scheduler maintains a shared FIFO queue of FIFO queues. "     \
    "Each thread maintains its own smaller in and out queues. When a "  \
    "threads out queue is too large (greater than \"queuesize\") then " \
    "the thread puts its out queue at the end of the master queue."))   

#include <graphlab/scheduler/fifo_scheduler.hpp>
#include <graphlab/scheduler/sweep_scheduler.hpp>
#include <graphlab/scheduler/priority_scheduler.hpp>
#include <graphlab/scheduler/queued_fifo_scheduler.hpp>


namespace graphlab {
  /// get all the scheduler names
  std::vector<std::string> get_scheduler_names();

  /// get all the scheduler names concated into a string
  std::string get_scheduler_names_str();

  /// Display the scheduler options for a particular scheduler
  void print_scheduler_info(std::string s, std::ostream &out);
}

#endif

