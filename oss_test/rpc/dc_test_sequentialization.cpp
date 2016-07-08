/*
* Copyright (C) 2016 Turi
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
using namespace graphlab;




class seq_test {
 public:
  dc_dist_object<seq_test> rmi;
  std::vector<size_t> ctr;
  seq_test(distributed_control &dc):rmi(dc, this), ctr(100,0) {
    rmi.barrier();
  }
  
  void recv(size_t idx, size_t val) {
    ASSERT_EQ(thread::thread_id(), idx);
    ASSERT_EQ(ctr[idx], val);
    ++ctr[idx];
  }

  void run() {
    for (size_t i = 1; i < 2; ++i) {
      rmi.dc().set_sequentialization_key(i);
      for (size_t j = 0;j < 1000000; ++j) {
        rmi.RPC_CALL(remote_call, seq_test::recv)(1, i, j);
      }
    }
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
  seq_test test(dc);
  if (dc.procid() == 0) {
    test.run();
  }
  dc.full_barrier();
  mpi_tools::finalize();
}
