/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef ZOOKEEPER_KEY_VALUE_HPP
#define ZOOKEEPER_KEY_VALUE_HPP
#include <map>
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
 *  A simple zookeeper service to maintain a key value store
 *  The service provides the ability to watch for changes
 *  through the use of callbacks. 
 *  Keys are "owned" by their creators, and duplicate keys
 *  are not permitted. Owners can change the values of their owned keys.
 *  Keys are destroyed when their owners die.
 *
 *  The natural implementation will be to create a node for each key
 *  and have the node contain the actual value of the key. However, it is very
 *  difficult to watch for changes here because if there are a large number of 
 *  keys, we have to set a data watch on each key, and zookeeper does not like
 *  it if you make too many watches.
 *
 *  The solution: 
 *  For each key
 *   - An ephemeral masters/[key]" node is created. 
 *     This is used to identify the machine currently owning the key, and make
 *     sure that there can only be one owner for each key.
 *   - A SEQUENCE EPHEMERAL node with the name "values/[key]-%10d" is created
 *     whenever the value of the key changes. The contents of the node
 *     are the contents of the key.
 *   - Now a single watch on the entire values directory is sufficient to 
 *     identify any data changes.
 */ 
class key_value {
 public:
  
  ///  Joins a zookeeper cluster. 
  ///  Zookeeper nodes will be created in the prefix "prefix".
  key_value(std::vector<std::string> zkhosts, 
            std::string prefix,
            std::string serveridentifier);
  /// destructor
  ~key_value();

  /** Inserts a value to the key value store. Returns true on success.
   * False on failure (indicating the key already exists)
   */
  bool insert(const std::string& key, const std::string& value);

  /** Modifies the value in the key value store. Returns true on success.
   * False on failure. This instance must own the key (created the key) 
   * to modify its value.
   */
  bool modify(const std::string& key, const std::string& value);

  /** Removed a key in the key value store. Returns true on success.
   * False on failure. This instance must own the key (created the key) 
   * to delete the key.
   */
  bool erase(const std::string& key);


  /// Gets a value of a key. First element of the pair is if the key was found
  std::pair<bool, std::string> get(const std::string& key);


  typedef boost::function<void(key_value*,
                               const std::vector<std::string>& out_newkeys,
                               const std::vector<std::string>& out_deletedkeys,
                               const std::vector<std::string>& out_modifiedkeys) 
                          >  callback_type;

  /** Adds a callback which will be triggered when any key/value 
   * changes. The callback arguments will be the key_value object, 
   * and the new complete key-value mapping.
   * Calling this function will a NULL argument deletes
   * the callback. Note that the callback may be triggered in a different thread. 
   *
   * Returns the id of the callback. Calling remove_callback with the id
   * disables the callback. 
   */
  int add_callback(callback_type fn);


  /** Removes a callback identified by an ID. Returns true on success,
   * false on failure */
  bool remove_callback(int fnid);
 private:
  std::string serveridentifier;
  std::string prefix;
  std::string masters_path;
  std::string values_path;
  zhandle_t* handle;

  boost::recursive_mutex datalock;

  std::map<int, callback_type> callbacks;
  int next_callback_id;
  bool closing;

  // a list of all the values I created
  std::set<std::string> my_values;

  struct lazy_value {
    bool has_value;
    int stored_version;
    int remote_version;
    std::string value;
    lazy_value():has_value(false), stored_version(-1), remote_version(-1) {}
    lazy_value(const lazy_value& lv): 
        has_value(lv.has_value), 
        stored_version(lv.stored_version), 
        remote_version(lv.remote_version), 
        value(lv.value) {}
  };

  std::map<std::string, lazy_value> data;

  bool get_all_keys_locked(std::vector<std::string>& out_newkeys,
                           std::vector<std::string>& out_deletedkeys,
                           std::vector<std::string>& out_modifiedkeys);

  void fill_data_locked(const std::vector<std::string>& keys,
                        const std::vector<std::string>& masterkeys,
                        std::vector<std::string>& out_newkeys,
                        std::vector<std::string>& out_deletedkeys,
                        std::vector<std::string>& out_modifiedkeys);

  static void watcher(zhandle_t *zh, 
                    int type, 
                    int state, 
                    const char *path, 
                    void *watcherCtx);


};


} // namespace zookeeper
} // namespace graphlab
#endif

