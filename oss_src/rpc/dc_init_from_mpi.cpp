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


#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <rpc/dc.hpp>
#include <rpc/dc_init_from_mpi.hpp>
#include <util/stl_util.hpp>
#include <network/net_util.hpp>
#include <logger/logger.hpp>

#ifdef HAS_MPI
#include <rpc/mpi_tools.hpp>
#endif
namespace graphlab {

bool init_param_from_mpi(dc_init_param& param,dc_comm_type commtype) {
#ifdef HAS_MPI
  ASSERT_MSG(commtype == TCP_COMM, "MPI initialization only supports TCP at the moment");
  // Look for a free port to use. 
  std::pair<size_t, int> port_and_sock = get_free_tcp_port();
  size_t port = port_and_sock.first;
  int sock = port_and_sock.second;
  
  std::string ipaddr = 
      get_local_ip_as_str(mpi_tools::rank() == 0 /* print stuff only if I am master */);
  ipaddr = ipaddr + ":" + tostr(port);
  // now do an allgather
  logstream(LOG_INFO) << "Will Listen on: " << ipaddr << std::endl;
  std::vector<std::string> machines;
  mpi_tools::all_gather(ipaddr, param.machines);
  // set defaults
  param.curmachineid = (procid_t)(mpi_tools::rank());

  param.numhandlerthreads = RPC_DEFAULT_NUMHANDLERTHREADS;
  param.commtype = commtype;
  param.initstring = param.initstring + std::string(" __sockhandle__=") + tostr(sock) + " ";
  return true;
#else
  std::cerr << "MPI Support not compiled!" << std::endl;
  exit(0);
#endif
}

} // namespace graphlab


