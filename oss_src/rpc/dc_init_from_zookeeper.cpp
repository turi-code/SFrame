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
#include <vector>
#include <algorithm>
#include <boost/bind.hpp>
#include <rpc/dc.hpp>
#include <zookeeper_util/server_list.hpp>
#include <util/stl_util.hpp>
#include <network/net_util.hpp>
#include <parallel/pthread_tools.hpp>
#include <logger/logger.hpp>
namespace graphlab {

#ifdef HAS_ZOOKEEPER
void zk_callback(zookeeper_util::server_list* slist,
                std::string name_space,
                std::vector<std::string> servers,
                std::vector<std::string>& result,
                size_t num_to_watch_for,
                mutex& result_lock,
                conditional& result_cond) {
  if (servers.size() == num_to_watch_for) {
    result_lock.lock();
    result = servers;
    slist->stop_watching("graphlab");
    result_cond.signal();
    result_lock.unlock();
  }
}
#endif


bool init_param_from_zookeeper(dc_init_param& param) {
#ifdef HAS_ZOOKEEPER
  char* zk_hosts = getenv("ZK_SERVERS");
  char* zk_jobname = getenv("ZK_JOBNAME");
  char* zk_numnodes = getenv("ZK_NUMNODES");
  if (zk_hosts == NULL || zk_jobname == NULL || zk_numnodes == NULL) {
    return false;
  }

  std::vector<std::string> zk_hosts_list = strsplit(zk_hosts, ",");

  // number of nodes to wait for
  size_t numnodes = atoi(zk_numnodes);
  ASSERT_GE(numnodes, 1);
  logstream(LOG_EMPH) << "Using Zookeeper for Initialization. Waiting for "
                      << numnodes << " to join" << std::endl;

  // generate a unique identifier for this server

  std::pair<size_t, int> port_and_sock = get_free_tcp_port();
  size_t port = port_and_sock.first;
  int sock = port_and_sock.second;
  std::string ipaddr = get_local_ip_as_str(true);
  ipaddr = ipaddr + ":" + tostr(port);
  logstream(LOG_INFO) << "Will Listen on: " << ipaddr << std::endl;

  // get an ip address
  zookeeper_util::server_list server_list(zk_hosts_list,
                                     zk_jobname,
                                     ipaddr);

  // final server list goes here
  std::vector<std::string> received_servers;
  // locks to product the final server list
  mutex lock;
  conditional cond;

  // construct the watch to watch for changes on zookeeper
  server_list.set_callback(boost::bind(zk_callback,
                                       _1,
                                       _2,
                                       _3,
                                       boost::ref(received_servers),
                                       numnodes,
                                       boost::ref(lock),
                                       boost::ref(cond)));

  server_list.join("graphlab");

  lock.lock();
  received_servers = server_list.watch_changes("graphlab");
  // wait until I get all the servers
  // TODO: add a timeout
  while(received_servers.size() < numnodes) cond.wait(lock);
  lock.unlock();

  // done!
  // now make sure that everyone sees the server list in the same order

  ASSERT_EQ(received_servers.size(), numnodes);
  std::sort(received_servers.begin(), received_servers.end());

  // now fill the parameter list
  param.machines = received_servers;
  param.curmachineid = std::find(received_servers.begin(), received_servers.end(),
                                 ipaddr) - received_servers.begin();
  ASSERT_LT(param.curmachineid, received_servers.size());
  param.numhandlerthreads = RPC_DEFAULT_NUMHANDLERTHREADS;
  param.commtype = RPC_DEFAULT_COMMTYPE;
  param.initstring = param.initstring + std::string(" __sockhandle__=") + tostr(sock) + " ";
  // detach from the server list
  // now, this takes advantage of the Zookeeper feature that
  // every machine sees all changes in the same order.
  // i.e. At some point, everyone would have seen a complete server list.
  // Once that happens, everyone can leave.
  server_list.set_callback(NULL);
  server_list.leave("graphlab");

  return true;
#else
  std::cerr << "Zookeeper Support not compiled!" << std::endl;
  exit(0);
#endif
}

} // namespace graphlab

