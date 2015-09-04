/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*  
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


/*
  \author Yucheng Low (ylow)
  An implementation of a distributed integer -> integer map with caching
  capabilities. 

*/

#ifndef GRAPHLAB_LAZY_DHT_HPP
#define GRAPHLAB_LAZY_DHT_HPP

#include <boost/unordered_map.hpp>
#include <boost/intrusive/list.hpp>

#include <rpc/dc.hpp>
#include <parallel/pthread_tools.hpp>
#include <graphlab/util/synchronized_unordered_map.hpp>
#include <util/dense_bitset.hpp>



namespace graphlab {

  /**
     \internal
     \ingroup rpc 


     This implements a distributed key -> value map with caching
     capabilities.  It is up to the user to determine cache
     invalidation policies. User explicitly calls the invalidate()
     function to clear local cache entries.  This is an extremely lazy
     DHT in that it is up to the user to guarantee that the keys are
     unique. Any machine can call set on any key, and the result of
     the key will be stored locally. Reads on any unknown keys will be
     resolved using a broadcast operation.
  */

  template<typename KeyType, typename ValueType>
  class lazy_dht{
  public:

    typedef dc_impl::lru_list<KeyType, ValueType> lru_entry_type;
    /// datatype of the data map
    typedef boost::unordered_map<KeyType, ValueType> map_type;
    /// datatype of the local cache map
    typedef boost::unordered_map<KeyType, lru_entry_type* > cache_type;

    struct wait_struct {
      mutex mut;
      conditional cond;
      ValueType val;
      size_t numreplies;
      bool hasvalue;
    };

    typedef boost::intrusive::member_hook<lru_entry_type,
                                          typename lru_entry_type::lru_member_hook_type,
                                          &lru_entry_type::member_hook_> MemberOption;
    /// datatype of the intrusive LRU list embedded in the cache map
    typedef boost::intrusive::list<lru_entry_type, 
                                   MemberOption, 
                                   boost::intrusive::constant_time_size<false> > lru_list_type;

    /// Constructor. Creates the integer map.
    lazy_dht(distributed_control &dc, 
             size_t max_cache_size = 65536):rmi(dc, this),data(11) {
      cache.rehash(max_cache_size);
      maxcache = max_cache_size;
      logger(LOG_INFO, "%d Creating distributed_hash_table. Cache Limit = %d", 
             dc.procid(), maxcache);
      reqs = 0;
      misses = 0;
      dc.barrier();
    }


    ~lazy_dht() {
      data.clear();
      typename cache_type::iterator i = cache.begin();
      while (i != cache.end()) {
        delete i->second;
        ++i;
      }
      cache.clear();
    }
  
  
    /// Sets the key to the value
    void set(const KeyType& key, const ValueType &newval)  {
      datalock.lock();
      data[key] = newval;
      datalock.unlock();
    }
  

    std::pair<bool, ValueType> get_owned(const KeyType &key) const {
      std::pair<bool, ValueType> ret;
      datalock.lock();
      typename map_type::const_iterator iter = data.find(key);    
      if (iter == data.end()) {
        ret.first = false;
      }
      else {
        ret.first = true;
        ret.second = iter->second;
      }
      datalock.unlock();
      return ret;
    }
  
    void remote_get_owned(const KeyType &key, procid_t source, size_t ptr) const {
      std::pair<bool, ValueType> ret;
      datalock.lock();
      typename map_type::const_iterator iter = data.find(key);    
      if (iter == data.end()) {
        ret.first = false;
      }
      else {
        ret.first = true;
        ret.second = iter->second;
      }
      datalock.unlock();
      rmi.RPC_CALL(remote_call, &lazy_dht<KeyType,ValueType>::get_reply)
                   (source, ptr, ret.second, ret.first);
    }

    void get_reply(size_t ptr, ValueType& val, bool hasvalue) {
      wait_struct* w = reinterpret_cast<wait_struct*>(ptr);      
      w->mut.lock();
      if (hasvalue) {
        w->val = val;
        w->hasvalue = true;
      }
      w->numreplies--;
      if (w->numreplies == 0) w->cond.signal();
      w->mut.unlock();
    
    }

    /** Gets the value associated with the key. returns true on success.. */
    std::pair<bool, ValueType> get(const KeyType &key) const {
      std::pair<bool, ValueType> ret = get_owned(key);
      if (ret.first) return ret;
    
      wait_struct w;
      w.numreplies = rmi.numprocs() - 1;
      size_t ptr = reinterpret_cast<size_t>(&w);
      // otherwise I need to find someone with the key
      for (size_t i = 0;i < rmi.numprocs(); ++i) {
        if (i != rmi.procid()) {
          rmi.RPC_CALL(remote_call,&lazy_dht<KeyType,ValueType>::remote_get_owned)
                       (i, key, rmi.procid(), ptr);
        }
      }
      w.mut.lock();
      while (w.numreplies > 0) w.cond.wait(w.mut);
      w.mut.unlock();
      ret.first = w.hasvalue;
      ret.second = w.val;
      if (ret.first) update_cache(key, ret.second);
      return ret;
    }


    /** Gets the value associated with the key, reading from cache if available
        Note that the cache may be out of date. */
    std::pair<bool, ValueType> get_cached(const KeyType &key) const {
      std::pair<bool, ValueType> ret = get_owned(key);
      if (ret.first) return ret;
    
      reqs++;
      cachelock.lock();
      // check if it is in the cache
      typename cache_type::iterator i = cache.find(key);
      if (i == cache.end()) {
        // nope. not in cache. Call the regular get
        cachelock.unlock();
        misses++;
        return get(key);
      }
      else {
        // yup. in cache. return the value
        ret.first = true;
        ret.second = i->second->value;
        // shift the cache entry to the head of the LRU list
        lruage.erase(lru_list_type::s_iterator_to(*(i->second)));
        lruage.push_front(*(i->second));
        cachelock.unlock();
        return ret;
      }
    }

    /// Invalidates the cache entry associated with this key
    void invalidate(const KeyType &key) const{
      cachelock.lock();
      // is the key I am invalidating in the cache?
      typename cache_type::iterator i = cache.find(key);
      if (i != cache.end()) {
        // drop it from the lru list
        delete i->second;
        cache.erase(i);
      }
      cachelock.unlock();
    }


    double cache_miss_rate() {
      return double(misses) / double(reqs);
    }

    size_t num_gets() const {
      return reqs;
    }
    size_t num_misses() const {
      return misses;
    }

    size_t cache_size() const {
      return cache.size();
    }

  private:

    mutable dc_dist_object<lazy_dht<KeyType, ValueType> > rmi;
  
    mutex datalock;
    map_type data;  /// The actual table data that is distributed

  
    mutex cachelock; /// lock for the cache datastructures
    mutable cache_type cache;   /// The cache table
    mutable lru_list_type lruage; /// THe LRU linked list associated with the cache


    procid_t numprocs;   /// NUmber of processors
    size_t maxcache;     /// Maximum cache size allowed

    mutable size_t reqs;
    mutable size_t misses;
  


  

    /// Updates the cache with this new value
    void update_cache(const KeyType &key, const ValueType &val) const{
      cachelock.lock();
      typename cache_type::iterator i = cache.find(key);
      // create a new entry
      if (i == cache.end()) {
        cachelock.unlock();
        // if we are out of room, remove the lru entry
        if (cache.size() >= maxcache) remove_lru();
        cachelock.lock();
        // insert the element, remember the iterator so we can push it
        // straight to the LRU list
        std::pair<typename cache_type::iterator, bool> ret = cache.insert(std::make_pair(key, new lru_entry_type(key, val)));
        if (ret.second)  lruage.push_front(*(ret.first->second));
      }
      else {
        // modify entry in place
        i->second->value = val;
        // swap to front of list
        //boost::swap_nodes(lru_list_type::s_iterator_to(i->second), lruage.begin());
        lruage.erase(lru_list_type::s_iterator_to(*(i->second)));
        lruage.push_front(*(i->second));
      }
      cachelock.unlock();
    }

    /// Removes the least recently used element from the cache
    void remove_lru() const{
      cachelock.lock();
      KeyType keytoerase = lruage.back().key;
      // is the key I am invalidating in the cache?
      typename cache_type::iterator i = cache.find(keytoerase);
      if (i != cache.end()) {
        // drop it from the lru list
        delete i->second;
        cache.erase(i);
      }
      cachelock.unlock();
    }

  };

}
#endif

