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


#include <cstdio>
#include <cstdlib>
#include <string>
#include <rpc/dc.hpp>
#include <rpc/dc_init_from_env.hpp>
#include <util/stl_util.hpp>
#include <logger/logger.hpp>
namespace graphlab {

bool init_param_from_env(dc_init_param& param) {
  char* nodeid = getenv("SPAWNID");
  if (nodeid == NULL) {
    return false;
  }
  param.curmachineid = atoi(nodeid);

  char* nodes = getenv("SPAWNNODES");
  std::string nodesstr = nodes;
  if (nodes == NULL) {
    return false;
  }

  param.machines = strsplit(nodesstr, ",");
  for (size_t i = 0;i < param.machines.size(); ++i) {
    param.machines[i] = param.machines[i] + ":" + tostr(10000 + i);
  }
  // set defaults
  param.numhandlerthreads = RPC_DEFAULT_NUMHANDLERTHREADS;
  param.commtype = RPC_DEFAULT_COMMTYPE;
  return true;
}

} // namespace graphlab

