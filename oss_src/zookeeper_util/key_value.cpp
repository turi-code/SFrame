/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cstdio>
#include <cstdlib>
#include <map>
#include <iostream>
#include <algorithm>
#include <vector>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <zookeeper_util/zookeeper_common.hpp>
#include <zookeeper_util/key_value.hpp>
extern "C" {
#include <zookeeper/zookeeper.h>
}

namespace graphlab {
namespace zookeeper_util {


key_value::key_value(std::vector<std::string> zkhosts, 
                     std::string _prefix,
                     std::string serveridentifier):
    prefix(_prefix), next_callback_id(0), closing(false) {
  serveridentifier = serveridentifier;
  // construct hosts list
  std::string hosts = boost::algorithm::join(zkhosts, ",");
  prefix = normalize_path(prefix);
  if (prefix[0] != '/') prefix = "/" + prefix;
  // we need to block the watcher from running until everything is ready
  handle = zookeeper_init(hosts.c_str(), watcher, 10000, NULL, (void*)this, 0);
  assert(handle != NULL);
  // create the prefix if it does not already exist
  if (prefix != "/") create_dir(handle,
                                prefix.substr(0, prefix.length() - 1), 
                                "zk_key_value");
  if (prefix != "/") {
    masters_path = prefix + "masters/";
    values_path = prefix + "values/";
  } 
  else {
    masters_path = "/masters/";
    values_path = "/values/";
  }

  create_dir(handle, 
             masters_path.substr(0, masters_path.length() - 1),
             "zk_key_value");

  create_dir(handle, 
             values_path.substr(0, values_path.length() - 1),
             "zk_key_value");

  datalock.lock();  
  std::vector<std::string> unused1, unused2, unused3;
  get_all_keys_locked(unused1, unused2, unused3);
  datalock.unlock();  
}

key_value::~key_value() {
  if (handle == NULL) return;
  datalock.lock();
  closing = true;
  datalock.unlock();
  // cleanup
  std::set<std::string>::const_iterator iter = my_values.begin();
  while (iter != my_values.end()) {
    int version = data[*iter].remote_version;
    if (version >= 0) {
      std::string value_node = get_sequence_node_path(values_path + (*iter) + "-",
                                                     version);
      delete_node(handle, value_node, "zk_key_value cleanup");
    }
    std::string master_node = masters_path + (*iter);
    delete_node(handle, master_node, "zk_key_value cleanup");
    ++iter;
  }
  /* should not try to delete it. It will mess up any watches.
   
  delete_dir(handle, 
             masters_path.substr(0, masters_path.length() - 1),
             "zk_key_value cleanup");
  delete_dir(handle, 
             values_path.substr(0, values_path.length() - 1),
             "zk_key_value cleanup");
  if (prefix != "/") delete_dir(handle,
                                prefix.substr(0, prefix.length() - 1), 
                                "zk_key_value cleanup");
  */
  zookeeper_close(handle);
}


/** Inserts a value to the key value store. Returns true on success.
 * False on failure (indicating the key already exists)
 */
bool key_value::insert(const std::string& key, const std::string& value) {
  if (key.length() == 0) return false;
  if (my_values.count(key)) return modify(key, value);
  // ok try to create the master node
  int ret = create_ephemeral_node(handle, 
                                  masters_path + key,
                                  serveridentifier,
                                  "zk_key_value insert");
  if (ret == ZNODEEXISTS) return false;
  else {
    // ok we own this key
    my_values.insert(key);
    return modify(key, value);
  }
}


bool key_value::modify(const std::string& key, const std::string& value) {
  if (key.length() == 0) return false;
  if (my_values.count(key) == 0) return false;
  datalock.lock();
  // add a - to the end
  std::pair<int, int> ret = create_ephemeral_sequence_node(handle, 
                                                           values_path + key + "-",
                                                           value,
                                                           "zk_key_value modify");
  assert(ret.first == ZOK);
  // update the cache
  lazy_value& val = data[key];
  val.has_value = true;
  int prev_remote_version = val.remote_version;
  val.stored_version = ret.second;
  val.value = value;
  // try to delete the previous remote version node
  if (prev_remote_version >= 0) {
    std::string old_node_path = get_sequence_node_path(values_path + key + "-",
                                                       prev_remote_version);
    delete_node(handle, old_node_path, "zk_key_value modify-cleanup");
  }
  datalock.unlock();
  return true;
}


bool key_value::erase(const std::string& key) {
  if (key.length() == 0) return false;
  if (my_values.count(key) == 0) return false;
  datalock.lock();
  lazy_value& val = data[key];
  // find the current version
  int cur_remote_version = std::max(val.stored_version, val.remote_version);
  // try to delete it
  if (cur_remote_version >= 0) {
    std::string old_node_path = get_sequence_node_path(values_path + key + "-",
                                                       cur_remote_version);
    delete_node(handle, old_node_path, "zk_key_value erase-value");
  }
  std::string master_node = masters_path + key;
  delete_node(handle, master_node, "zk_key_value erase-master");
  // don't fully remove it from the data map yet
  // let the trigger take care of it
  my_values.erase(my_values.find(key));
  datalock.unlock();
  return true;
}



bool key_value::get_all_keys_locked(
    std::vector<std::string>& out_newkeys,
    std::vector<std::string>& out_deletedkeys,
    std::vector<std::string>& out_modifiedkeys) {


  struct String_vector children;
  children.count = 0;
  children.data = NULL;

  // get a list of all the keys and set the watch
  std::string values_node = values_path.substr(0, values_path.length() - 1);
  std::string master_node = masters_path.substr(0, masters_path.length() - 1);
  int stat = zoo_get_children(handle, values_node.c_str(), 1, &children);
  if (stat == ZCLOSING) return false;
  if (stat != 0) {
    print_stat(stat, "zk_key_value get_all_keys values", values_path);
    return false;
  }

  struct String_vector masters;
  stat = zoo_get_children(handle, master_node.c_str(), 1, &masters);
  if (stat == ZCLOSING) return false;
  if (stat != 0) {
    print_stat(stat, "zk_key_value get_all_keys masters", masters_path);
    free_String_vector(&children); 
    return false;
  }

  std::vector<std::string> masterkeys = String_vector_to_vector(&masters);
  std::vector<std::string> allkeys = String_vector_to_vector(&children);
  /*
  for (size_t i = 0;i < masterkeys.size(); ++i) {
    std::cout << "\t" << masterkeys[i] << "\n";
  }
  for (size_t i = 0;i < allkeys.size(); ++i) {
    std::cout << "\t" << allkeys[i] << "\n";
  }*/
  free_String_vector(&children); 
  free_String_vector(&masters); 

  fill_data_locked(allkeys, masterkeys, out_newkeys, out_deletedkeys, out_modifiedkeys);
  return true;
}

std::pair<bool, std::string> key_value::get(const std::string& key) {
  datalock.lock();
  // search for the key in the map
  std::map<std::string, lazy_value>::const_iterator iter = data.find(key);
  if (iter == data.end()) {
    datalock.unlock();
    return std::pair<bool, std::string>(false, "");
  }
  // see if we have a cached version
  if (iter->second.has_value) {
    // yup. we have a cached copy. return that
    std::pair<bool, std::string> value(true, iter->second.value);
    datalock.unlock();
    return value;
  }
  // otherwise, we need to get the data.
  // figure out the node we need to query
  int remote_version = iter->second.remote_version;
  std::string node = get_sequence_node_path(values_path + key + "-",
                                            remote_version);
  datalock.unlock();
  // ok. try to query the node
  //std::cout << "Getting value for " << node << "\n";
  std::pair<bool, std::string> value = get_node_value(handle, node, "zk_key_value get");
  // if successful, return that
  if (value.first) {
    // cache the value
    datalock.lock();
    std::map<std::string, lazy_value>::iterator iter = data.begin();
    if (iter != data.end() && iter->second.remote_version == remote_version) {
      iter->second.has_value = true;
      iter->second.stored_version = remote_version;
      iter->second.value = value.second;
    }
    datalock.unlock();
  }
  // otherwise... the node is missing. The watch should delete it eventually
  return value;
}


void key_value::fill_data_locked(const std::vector<std::string>& keys,
                                 const std::vector<std::string>& masterkeys,
                                 std::vector<std::string>& out_newkeys,
                                 std::vector<std::string>& out_deletedkeys,
                                 std::vector<std::string>& out_modifiedkeys) {
  std::set<std::string> masterkeyset;
  for (size_t i = 0;i < masterkeys.size(); ++i) {
    masterkeyset.insert(masterkeys[i]);
  }
  std::map<std::string, int> key_and_version;
  for (size_t i = 0;i < keys.size(); ++i) {
    // this must be a sequence node!
    assert(keys[i].length() > 10);
    // where the sequence number is expected to start
    size_t num_start = keys[i].length() - 10;
    size_t key_length = num_start - 1;

    // Ex: abc-1234567890
    // length = 14
    // num_start = 4
    // key_length = 3
    //
    // some sanity checks. The format must be [key]-%10d
    // check for the dash ('-')
    assert(num_start > 0);
    assert(keys[i][num_start - 1] == '-');
    int version = atoi(keys[i].c_str() + num_start);
    std::string keyname = keys[i].substr(0, key_length);
    if (masterkeyset.count(keyname)) {
      key_and_version[keyname] = std::max(version, key_and_version[keyname]);
    }
  }

  // now scan against the actual data and compute a diff
  // first search for deleted keys
  // scan the data map against the key_and_version map
  {
    std::map<std::string, lazy_value>::const_iterator iter = data.begin();
    while (iter != data.end()) {
      if (key_and_version.count(iter->first) == 0) {
        out_deletedkeys.push_back(iter->first);
      }
      ++iter;
    }
  }
  // now actually delete it from the data map
  for (size_t i = 0;i < out_deletedkeys.size(); ++i) {
    data.erase(data.find(out_deletedkeys[i]));
  }

  // ok. now loop through the key_and_version map and handle new and 
  // modified keys
  {
    std::map<std::string, int>::const_iterator iter = key_and_version.begin();
    while (iter != key_and_version.end()) {
      std::map<std::string, lazy_value>::iterator data_iter = data.find(iter->first);
      if (data_iter == data.end()) {
        // key not found. this is a new key 
        out_newkeys.push_back(iter->first);
        data[iter->first].remote_version = iter->second;
        data[iter->first].has_value = (data[iter->first].stored_version == data[iter->first].remote_version);
      } else {
        // key found. this is an existing key 
        // if the remote version changed, it was modified
        if (data_iter->second.remote_version == -1) {
          out_newkeys.push_back(iter->first);
        }
        else if (data_iter->second.remote_version < iter->second) {
          out_modifiedkeys.push_back(iter->first);
        }
        // invalidate the local value
        data_iter->second.remote_version = std::max(data_iter->second.remote_version, iter->second);
        data[iter->first].has_value = (data[iter->first].stored_version == data[iter->first].remote_version);
      }
      ++iter;
    }
  }
}

// ------------- watch implementation ---------------

int key_value::add_callback(callback_type fn) {
  datalock.lock();
  size_t cur_callback_id = next_callback_id;
  callbacks[cur_callback_id] = fn;
  ++next_callback_id;
  datalock.unlock();
  return cur_callback_id;
}

bool key_value::remove_callback(int fnid) {
  bool ret = false;
  datalock.lock();
  std::map<int, callback_type>::iterator iter = callbacks.find(fnid);
  if (iter != callbacks.end()) {
    ret = true;
    callbacks.erase(iter);
  }
  datalock.unlock();
  return ret;
}

void key_value::watcher(zhandle_t *zh, 
                        int type, 
                        int state, 
                        const char *path, 
                        void *watcherCtx) {
  key_value* slist = reinterpret_cast<key_value*>(watcherCtx);
  if (type == ZOO_CHILD_EVENT) {
    slist->datalock.lock();  
    if (!slist->closing) {
      std::vector<std::string> newkeys, deletedkeys, modifiedkeys;
      bool ret = slist->get_all_keys_locked(newkeys, deletedkeys, modifiedkeys);
      slist->datalock.unlock();  
      if (ret && !slist->callbacks.empty()) {
        std::map<int, callback_type>::iterator iter = slist->callbacks.begin();
        while (iter != slist->callbacks.end()) {
          iter->second(slist, newkeys, deletedkeys, modifiedkeys);
          ++iter;
        }
      }
    } else {
      slist->datalock.unlock();  
    }
  }
}




} // namespace zookeeper
} // namespace graphlab
