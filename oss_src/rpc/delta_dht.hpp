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


/**
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved.  
 *
 * Contributed under the iCLA for:
 *    Joseph Gonzalez (jegonzal@yahoo-inc.com) 
 *
 */


#ifndef GRAPHLAB_DELTA_DHT_HPP
#define GRAPHLAB_DELTA_DHT_HPP


#include <boost/unordered_map.hpp>
#include <boost/functional/hash.hpp>


#include <rpc/dc.hpp>
#include <parallel/pthread_tools.hpp>
#include <graphlab/util/cache.hpp>




#include <graphlab/macros_def.hpp>
namespace graphlab {

  namespace delta_dht_impl {
    struct icache { virtual ~icache() { } };
    icache*& get_icache_ptr(const void* dht_ptr);    
  }; // end of namespace delta_dht_impl


  namespace delta_predicate {
    template<typename ValueType, typename DeltaType>
    struct uses {
      size_t max_uses;
      uses(size_t max_uses = 100) : max_uses(max_uses) { }
      //! returns true if the predicate 
      bool operator()(const ValueType& current,
                      const DeltaType& delta,
                      const size_t& uses) const {
        return uses < max_uses;
      }
    }; // end of uses

  }; // end of eviction predicates






  template<typename KeyType, typename ValueType,
           typename DeltaType = ValueType>
  class delta_dht {
  public:
    typedef KeyType   key_type;
    typedef ValueType value_type;
    typedef DeltaType delta_type;

    typedef size_t    size_type;    
    
    typedef boost::unordered_map<key_type, value_type> data_map_type;
    
    struct cache_entry {
      value_type value;
      delta_type delta;
      size_t uses;
      cache_entry(const value_type& value = value_type()) : 
        value(value), uses(0) { }
    };

    typedef cache::lru<key_type, cache_entry> cache_type;

  private:

    //! The remote procedure call manager 
    mutable dc_dist_object<delta_dht> rpc;


    //! The data stored locally on this machine
    data_map_type  data_map;

    //! The lock for the data map
    mutex data_lock;

    //! The master cache
    cache_type cache;

    //! The master cash rw lock
    mutex cache_lock;
  
    //! The maximum cache size
    size_t max_cache_size;

    size_t max_uses;

    //! the hash function
    boost::hash<key_type> hash_function;

    //! cache hits and misses
    mutable atomic<size_t> local; 
    mutable atomic<size_t> hits;
    mutable atomic<size_t> misses;
    mutable atomic<size_t> background_updates;

  public:

    delta_dht(distributed_control& dc, 
              size_t max_cache_size = 2056) : 
      rpc(dc, this), 
      max_cache_size(max_cache_size), max_uses(10) {
      rpc.barrier();
    }

    ~delta_dht() { rpc.full_barrier(); }
    
    void set_max_uses(size_t max) { max_uses = max; }

    size_t cache_local() const { return local.value; }
    size_t cache_hits() const { return hits.value; }
    size_t cache_misses() const { return misses.value; }
    size_t background_syncs() const { return background_updates.value; }

    size_t cache_size() const { 
      cache_lock.lock();
      const size_t ret_val = cache.size(); 
      cache_lock.unlock();
      return ret_val;
    }

    bool is_cached(const key_type& key) const { 
      cache_lock.lock();
      const bool ret_value = cache.contains(key); 
      cache_lock.unlock();
      return ret_value;
    }


    value_type operator[](const key_type& key) {     
      if(is_local(key)) {
        ++local;
        data_lock.lock();
        const value_type value = data_map[key];
        data_lock.unlock();
        return value;
      } else { // on a remote machine check the cache    
        // test for the key in the cache
        cache_lock.lock();
        if(cache.contains(key)) {
          ++hits;
          const value_type ret_value = cache[key].value;
          cache_lock.unlock();
          return ret_value;
        } else { // need to create a cache entry
          ++misses;
          // Free space in the cache if necessary
          while(cache.size() + 1 > max_cache_size) {
            ASSERT_GT(cache.size(), 0);
            const std::pair<key_type, cache_entry> pair = cache.evict();
            const key_type& key                         = pair.first;
            const cache_entry& entry                    = pair.second;
            send_delta(key, entry.delta);
          }          
          // get the new entry from the server
          const value_type ret_value = (cache[key].value = get_master(key));
          cache_lock.unlock();
          return ret_value;
        }
      }
    } // end of operator []
    

    void apply_delta(const key_type& key, const delta_type& delta) {
      if(is_local(key)) {
        data_lock.lock();
        data_map[key] += delta;
        data_lock.unlock();
      } else {
        // update the cache entry if availablable
        cache_lock.lock();
        if(cache.contains(key)) {
          cache_entry& entry = cache[key];
          entry.value += delta;
          entry.delta += delta;               
          if( entry.uses > max_uses ) {
            const delta_type accum_delta = entry.delta;
            entry.delta = delta_type();
            entry.uses = 0;
            cache_lock.unlock();
            send_delta(key, accum_delta);
            return;
          }
        }
        cache_lock.unlock();          
      }
    }



    //! empty the local cache
    void flush() {
      cache_lock.lock();
      while(cache.size() > 0) {
        const std::pair<key_type, cache_entry> pair = cache.evict();
        const key_type& key = pair.first;
        const cache_entry& entry = pair.second;
        send_delta(key, entry.delta);
      }
      cache_lock.unlock();
    }


    //! empty the local cache
    void barrier_flush() {
      flush();
      rpc.full_barrier();
    }
    
    
    void synchronize() {
      typedef typename cache_type::pair_type pair_type;
      cache_lock.lock();
      foreach(pair_type& pair, cache) {
        key_type& key = pair.first;
        cache_entry& entry = pair.second;
        if(entry.uses > 0) {
          const delta_type accum_delta = entry.delta;
          entry.delta = delta_type();
          entry.uses = 0;
          send_delta(key, accum_delta);
        }
      } // end of foreach
      cache_lock.unlock();
    }


    void synchronize(const key_type& key) {
      if(is_local(key)) return;
      cache_lock.lock();
      if(cache.contains(key)) {
        cache_entry& entry = cache[key];
        const delta_type accum_delta = entry.delta;
        entry.delta = delta_type();
        entry.uses = 0;
        cache_lock.unlock();
        send_delta(key, accum_delta);
      } else cache_lock.unlock();
    }


    size_t owning_cpu(const key_type& key) const {
      const size_t hash_value = hash_function(key);
      const size_t cpuid = hash_value % rpc.numprocs();
      return cpuid;
    }
      

    bool is_local(const key_type& key) const {
      return owning_cpu(key) == rpc.procid();
    } // end of is local   


    delta_type delta(const key_type& key) const {
      if(!is_local(key)) {
        cache_lock.lock();
        if(cache.contains(key)) {
          const delta_type delta = cache[key].delta;
          cache_lock.unlock();
          return delta;
        }
        cache_lock.unlock();
      }
      return delta_type();
    }


    size_t local_size() const {
      data_lock.lock();
      const size_t result = data_map.size(); 
      data_lock.unlock();
      return result;
    }


    size_t size() const {
      size_t sum = 0;
      for(size_t i = 0; i < rpc.numprocs(); ++i) {
        if(i == rpc.procid()) sum += local_size();
        else sum += rpc.remote_request(i, &delta_dht::local_size); 
      }
      return sum;
    }

    size_t numprocs() const { return rpc.num_procs(); }
    size_t procid() const { return rpc.procid(); }


    value_type get_master(const key_type& key) {
      // If the data is stored locally just read and return
      if(is_local(key)) {
        data_lock.lock();
        const value_type ret_value = data_map[key];
        data_lock.unlock();
        return ret_value;
      } else {
        return rpc.remote_request(owning_cpu(key), 
                                  &delta_dht::get_master, key);
      }
    } // end of direct get
    
  private:
    
    void send_delta(const key_type& key, const delta_type& delta)  {
      // If the data is stored locally just read and return
      ASSERT_FALSE(is_local(key));
      const size_t calling_procid = procid();
      rpc.remote_call(owning_cpu(key), 
                      &delta_dht::send_delta_rpc, 
                      calling_procid, key, delta);
      
    } // end of send_delta
    
    void send_delta_rpc(const size_t& calling_procid, 
                        const key_type& key, const delta_type& delta)  {
      // If the data is stored locally just read and return
      ASSERT_TRUE(is_local(key));
      data_lock.lock(); 
      const value_type ret_value = (data_map[key] += delta);
      data_lock.unlock();
      rpc.remote_call(calling_procid, 
                      &delta_dht::send_delta_rpc_callback, key, ret_value);      
      
    } // end of send_delta_rpc
    
    void send_delta_rpc_callback(const key_type& key, const value_type& new_value)  {
      // If the data is stored locally just read and return
      ASSERT_FALSE(is_local(key));
      cache_lock.lock();
      if(cache.contains(key)) {
        cache_entry& entry = cache[key];
        entry.value = new_value;
        entry.value += entry.delta;
      }
      ++background_updates;
      cache_lock.unlock();
    } // end of send_delta_rpc_callback  

    
    // void synchronize(const key_type& key, cache_entry& entry)  {
    //   const value_type delta = entry.current - entry.old;
    //   entry.old = synchronize_rpc(key, delta);
    //   entry.current = entry.old;
    // } // end of synchronize

    // value_type synchronize_rpc(const key_type& key, const value_type& delta) {
    //   if(is_local(key)) {
    //     data_lock.lock();
    //     typename data_map_type::iterator iter = data_map.find(key);
    //     ASSERT_TRUE(iter != data_map.end());
    //     const value_type ret_value = (iter->second += delta);
    //     data_lock.unlock();
    //     return ret_value;
    //   } else {
    //     return rpc.remote_request(owning_cpu(key), 
    //                               &delta_dht::synchronize_rpc, 
    //                               key, delta);
    //   }
    // } // end of synchronize_rpc

  }; // end of delta_dht


}; // end of namespace graphlab
#include <graphlab/macros_undef.hpp>



#endif


