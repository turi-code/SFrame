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


#ifndef GRAPHLAB_DC_INIT_FROM_ZOOKEEPER_HPP
#define GRAPHLAB_DC_INIT_FROM_ZOOKEEPER_HPP
#include <rpc/dc.hpp>
namespace graphlab {
  /**
   * \ingroup rpc
   * initializes parameters from ZooKeeper. Returns true on success.
   * To initialize from Zookeeper, the following environment variables must be set
   *
   * ZK_SERVERS: A comma separated list of zookeeper servers. Port
   *             number must be included.
   * ZK_JOBNAME: The name of the job to use. This must be unique to the cluster.
   *             i.e. no other job with the same name must run at the same time
   * ZK_NUMNODES: The number of processes to wait for
   *
   */
  bool init_param_from_zookeeper(dc_init_param& param);
}

#endif // GRAPHLAB_DC_INIT_FROM_ZOOKEEPER_HPP

