/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <zookeeper_util/zookeeper_common.hpp>
#include <zookeeper_util/server_list.hpp>
#include <iostream>
#include <algorithm>
extern "C" {
#include <zookeeper/zookeeper.h>
}

namespace graphlab {
namespace zookeeper_util {

server_list::server_list(std::vector<std::string> zkhosts, 
                         std::string _prefix,
                         std::string _serveridentifier) : 
    prefix(_prefix), serveridentifier(_serveridentifier), callback(NULL) {
  // construct hosts list
  std::string hosts = boost::algorithm::join(zkhosts, ",");
  prefix = normalize_path(prefix);
  if (prefix[0] != '/') prefix = "/" + prefix;
  handle = zookeeper_init(hosts.c_str(), watcher, 10000, NULL, (void*)this, 0);
  // create the prefix if it does not already exist
  if (prefix != "/") create_dir(handle, 
                                prefix.substr(0, prefix.length() - 1), 
                                "zk_server_list");

  assert(handle != NULL);

}

server_list::~server_list() {
  if (handle != NULL) zookeeper_close(handle);
}




std::vector<std::string> server_list::get_all_servers(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  struct String_vector children;
  children.count = 0;
  children.data = NULL;

  std::vector<std::string> ret;

  // effective path is prefix + name_space
  std::string path = prefix + name_space;

  int stat = zoo_get_children(handle, path.c_str(), 0, &children);
  // if there are no children quit
  if (stat == ZNONODE) return ret;
  ret = String_vector_to_vector(&children);
  free_String_vector(&children); 
  return ret;
}

/// Joins a namespace
void server_list::join(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  create_dir(handle, 
             prefix + name_space, 
             "zk_server_list");
  std::string path = normalize_path(prefix + name_space) + serveridentifier;
  int stat = create_ephemeral_node(handle, path, "");
  if (stat == ZNODEEXISTS) {
    std::cerr << "Server " << serveridentifier << " already exists!" << std::endl;
  }
  if (stat != ZOK) assert(false);
}

void server_list::leave(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  std::string path = normalize_path(prefix + name_space) + serveridentifier;
  delete_node(handle, path, "zk_server_list leave");
  // also try to delete its parents if they become empty
  delete_dir(handle, prefix + name_space, "zk_server_list leave cleanup");
  if (prefix != "/") delete_dir(handle, 
                                prefix.substr(0, prefix.length() - 1), 
                                "zk_server_list leave cleanup");
}


// ------------- watch implementation ---------------


std::vector<std::string> server_list::watch_changes(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  struct String_vector children;
  children.count = 0;
  children.data = NULL;
  std::vector<std::string> ret;

  std::string path = prefix + name_space;
  watchlock.lock();
  if (watches.count(path)) {
    watchlock.unlock();
    return get_all_servers(name_space);
  }
  watches.insert(path);

  int stat = zoo_get_children(handle, path.c_str(), 1, &children);
  watchlock.unlock();
  // if there are no children quit
  if (stat == ZNONODE) return ret;
  print_stat(stat, "zk_server_list watch_changes", path);
  ret = String_vector_to_vector(&children);
  free_String_vector(&children); 
  return ret;

}

void server_list::stop_watching(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  std::string path = prefix + name_space;
  watchlock.lock();
  watches.erase(path);
  watchlock.unlock();
}

void server_list::set_callback(boost::function<void(server_list*, 
                                                    std::string name_space,
                                                    std::vector<std::string> server)
                                              > fn) {
  watchlock.lock();
  callback = fn;
  watchlock.unlock();
}

void server_list::issue_callback(std::string path) {
  watchlock.lock();
  // search for the path in the watch set
  bool found = watches.count(path);
  if (found) {
    struct String_vector children;
    children.count = 0;
    children.data = NULL;
    std::vector<std::string> ret;
    // reissue the watch 
    int stat = zoo_get_children(handle, path.c_str(), 1, &children);
    print_stat(stat, "zk serverlist issue_callback", path);
    ret = String_vector_to_vector(&children);
    free_String_vector(&children); 

    // if a callback is registered
    if (callback != NULL) {
      callback(this, path, ret);
    }
  }
  watchlock.unlock();
}

void server_list::watcher(zhandle_t *zh, 
                          int type, 
                          int state, 
                          const char *path, 
                          void *watcherCtx) {
  server_list* slist = reinterpret_cast<server_list*>(watcherCtx);
  if (type == ZOO_CHILD_EVENT) {
    std::string strpath = path;
    slist->issue_callback(path);
  }
}




} // namespace zookeeper
} // namespace graphlab
