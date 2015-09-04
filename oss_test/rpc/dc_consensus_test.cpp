/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
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


#include <iostream>
#include <string>
#include <map>
#include <rpc/dc.hpp>
#include <rpc/dc_init_from_mpi.hpp>
#include <rpc/async_consensus.hpp>
#include <rpc/mpi_tools.hpp>
#include <util/blocking_queue.hpp>
using namespace graphlab;




class simple_engine_test {
 public:
  dc_dist_object<simple_engine_test> rmi;
  blocking_queue<size_t> queue;
  async_consensus cons;
  atomic<size_t> numactive;;

  simple_engine_test(distributed_control &dc):rmi(dc, this), cons(dc, 4) {
    numactive.value = 4; 
    dc.barrier();
  }

  void add_task_local(size_t i) {
    queue.enqueue(i);
    if (numactive.value < 4) cons.cancel();
  }  
  
  void task(size_t i) {
    if (i < 5) std::cout << "Task " << i << std::endl;
    if (i > 0) {
      if (rmi.numprocs() == 1) {
        add_task_local(i - 1);
      }
      else {
        rmi.RPC_CALL(remote_call, simple_engine_test::add_task_local)
            ((procid_t)((rmi.procid() + 1) % rmi.numprocs()),
             i - 1);
      }
    }
  }
  
  bool try_terminate(size_t cpuid, std::pair<size_t, bool> &job) {
    job.second = false;
    
    numactive.dec();
    cons.begin_done_critical_section(cpuid);
    job = queue.try_dequeue();
    if (job.second == false) {
      bool ret = cons.end_done_critical_section(cpuid);
      numactive.inc();
      return ret;
    }
    else {
      cons.cancel_critical_section(cpuid);
      numactive.inc();
      return false;
    }
  }
  
  void thread(size_t cpuid) {
    while(1) {
       std::pair<size_t, bool> job = queue.try_dequeue();
       if (job.second == false) {
          bool ret = try_terminate(cpuid, job);
          if (ret == true) break;
          if (ret == false && job.second == false) continue;
       }
       task(job.first);
    }
  }
  
  void start_thread() {
    thread_group thrgrp; 
    for (size_t i = 0;i < 4; ++i) {
      thrgrp.launch(boost::bind(
                            &simple_engine_test::thread,
                            this, i));
    }
    
    thrgrp.join();
    ASSERT_EQ(queue.size(), 0);
  }
};


int main(int argc, char ** argv) {
  /** Initialization */
  mpi_tools::init(argc, argv);
  global_logger().set_log_level(LOG_DEBUG);

  dc_init_param param;
  if (init_param_from_mpi(param) == false) {
    return 0;
  }
  distributed_control dc(param);
  simple_engine_test test(dc);
  test.add_task_local(1000);
  test.start_thread();
  mpi_tools::finalize();
}
