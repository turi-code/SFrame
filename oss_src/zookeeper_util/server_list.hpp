/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_ZOOKEEPER_SERVER_LIST_HPP
#define GRAPHLAB_ZOOKEEPER_SERVER_LIST_HPP

#include <set>
#include <vector>
#include <string>
#include <boost/function.hpp>
#include <boost/thread/recursive_mutex.hpp>
extern "C" {
#include <zookeeper/zookeeper.h>
}

namespace graphlab {
namespace zookeeper_util {

/**
 * \ingroup fault
 *  A simple zookeeper service to maintain a list of servers.
 *  The service provides the ability to watch for new servers leaving
 *  and joining the service through the use of callbacks.
 */
class server_list {
 public:
  
  ///  Joins a zookeeper cluster. 
  ///  Zookeeper nodes will be created in the prefix "prefix".
  ///  The current machine will be identified as "serveridentifier"
  server_list(std::vector<std::string> zkhosts, 
              std::string prefix,
              std::string serveridentifier);

  /// destructor
  ~server_list();

  /// Joins a namespace
  void join(std::string name_space);

  /// Leaves a namespace. Note that if this machine crashes, or if the 
  /// server list is destroyed, it will automatically leave the namespace.
  void leave(std::string name_space);

  /// gets a list of all servers in a namespace 
  std::vector<std::string> get_all_servers(std::string name_space);

  /// Watches for changes to a namespace while returning the current contents
  /// When changes occur, the callback is called. 
  std::vector<std::string> watch_changes(std::string name_space);

  /// Removes the watch callback.
  void stop_watching(std::string name_space);


  /** Adds a callback which will be triggered when any namespace in the prefix
   * changes. The callback arguments will be the server_list object, the  
   * namespace which changed, and the new list of servers in the name space.
   * Calling this function will a NULL argument deletes
   * the callback. Note that the callback may be triggered in a different thread. 
   */
  void set_callback(boost::function<void(server_list* cur, 
                                         std::string name_space,
                                         std::vector<std::string> server)
                                         > fn);

 private:
  std::string prefix, serveridentifier;
  zhandle_t* handle;

  boost::recursive_mutex watchlock;
  std::set<std::string> watches;

  boost::function<void(server_list*, std::string, std::vector<std::string>)> callback;

  void issue_callback(std::string path);

  static void watcher(zhandle_t *zh, 
                    int type, 
                    int state, 
                    const char *path, 
                    void *watcherCtx);


};

} // namespace zookeeper 
} // namespace graphlab
#endif
