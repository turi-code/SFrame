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

#ifndef GRAPHLAB_CACHE_HPP
#define GRAPHLAB_CACHE_HPP

#include <algorithm>
#include <vector>
#include <boost/functional/hash.hpp>

#include <boost/bimap.hpp>
#include <boost/bimap/list_of.hpp>
#include <boost/bimap/unordered_set_of.hpp>


#include <logger/assertions.hpp>


namespace graphlab {
  namespace cache { 

    // template<typename Cache, typename Source>
    // struct bind {
    //   typedef Cache cache_type;
    //   typedef typename cache_type::key_type key_type;
    //   typedef typename cache_type::value_type value_type;
    //   cache_type cache;
    //   Source& source;
    //   bind(Source& source, size_t capacity = 100) : 
    //     source(source), capacity(capacity) { }
    //   value_type get(const key_type& key) { return cache.get(key, source); }
    // }; // end of bind


    template<typename Key, typename Value>
    class lru {
    public:
      typedef Key key_type;
      typedef Value value_type;
      
      typedef boost::bimaps::bimap<
        boost::bimaps::unordered_set_of<key_type>, 
        boost::bimaps::list_of<value_type> > 
      cache_map_type;
      
      typedef typename cache_map_type::iterator iterator_type;
      typedef typename cache_map_type::value_type pair_type;
      
    private:
      mutable cache_map_type cache_map;
      
      
    public:

      lru(size_t cache_reserve = 1024) {
        cache_map.left.rehash(cache_reserve); 
      }

      
      size_t size() const { return cache_map.size(); }
      size_t empty() const { return size() == 0; }

      iterator_type begin() { return cache_map.begin(); }
      iterator_type end() { return cache_map.end(); }
      
      std::pair<key_type, value_type> evict() {
        ASSERT_FALSE(cache_map.empty());
        typedef typename cache_map_type::right_iterator iterator_type;
        iterator_type iter = cache_map.right.begin();
        const std::pair<key_type, value_type> 
          result(iter->get_left(), iter->get_right());
        cache_map.right.erase(iter);
        return result;
      } // end of evict

      std::pair<bool, value_type> evict(const key_type& key) {
        typedef typename cache_map_type::left_iterator iterator_type;
        iterator_type iter = cache_map.left.find(key);
        if(iter == cache_map.left.end()) 
          return std::make_pair(false, value_type());
        const value_type result = iter->get_right();
        cache_map.left.erase(iter);
        return std::make_pair(true, result);  
      } // end of evict(key)

      bool evict(const key_type& key, value_type& ret_value) {
        typedef typename cache_map_type::left_iterator iterator_type;
        iterator_type iter = cache_map.left.find(key);
        if(iter == cache_map.left.end()) return false;
        ret_value = iter->get_right();
        cache_map.left.erase(iter);
        return true;
      } // end of evict(key)
      

      bool contains(const key_type& key) const {
        typedef typename cache_map_type::left_const_iterator iterator_type;
        iterator_type iter = cache_map.left.find(key);
        return iter != cache_map.left.end();
      } // end of contains


      // value_type* find(const key_type& key) { 

      //   return cache_map.find(key); 
      // }

      value_type& operator[](const key_type& key) {
        typedef typename cache_map_type::left_iterator iterator_type;
        iterator_type iter = cache_map.left.find(key);
        if(iter != cache_map.left.end()) { // already in cache
          // move it to the end
          cache_map.right.relocate(cache_map.right.end(), 
                                   cache_map.project_right(iter));
          return iter->get_right();
        } else {
          // add it to the cache
          // Get the true entry from the source
          typedef typename cache_map_type::value_type pair_type;
          cache_map.insert(pair_type(key, value_type()));
          return cache_map.left[key];
        }
      } // end of oeprator[]

      const value_type& operator[](const key_type& key) const {
        typedef typename cache_map_type::left_const_iterator iterator_type;
        iterator_type iter = cache_map.left.find(key);
        if(iter != cache_map.left.end()) { // already in cache
          // move it to the end
          cache_map.right.relocate(cache_map.right.end(), 
                                   cache_map.project_right(iter));
          return iter->get_right();
        }
        logstream(LOG_FATAL) << "Key not found!" << std::endl;
        assert(false);
      } // end of oeprator[]

      bool get(const key_type& key, value_type& ret_value) {
        typedef typename cache_map_type::left_iterator iterator_type;
        iterator_type iter = cache_map.left.find(key);
        if(iter != cache_map.left.end()) { // already in cache
          ret_value = iter->get_right();
          // move it to the end
          cache_map.right.relocate(cache_map.right.end(), 
                                   cache_map.project_right(iter));
          return true;
         } else return false;
      } // end of get

    }; // end of class lru




    
    template<typename Key, typename Value>
    class associative {
    public:
      typedef Key key_type;
      typedef Value value_type;
       
      
    private:
      std::vector<key_type>   keys;
      std::vector<value_type> values;
      std::vector<bool>       is_set;
      size_t size_;
      boost::hash<key_type>   hash_function;
      
    public:

      associative(size_t cache_size = 1024) :
        keys(cache_size), values(cache_size), 
        is_set(cache_size), size_(0) { }
      
      size_t size() { return size_; }
      

      bool evict_slot(const key_type& key, 
                      key_type& ret_key, value_type& ret_value) {
        const size_t index = hash_function(key) % keys.size();
        if(is_set[index]) {
          ret_key = keys[index]; ret_value = values[index];
          is_set[index] = false;
          return true;
        } else return false;
      } // end of evict_slot
        
      std::pair<bool, value_type> evict(const key_type& key) {
        const size_t index = hash_function(key) % keys.size();
        if(is_set[index] && key[index] == key) {
          is_set[index] = false;
          return std::make_pair(true, values[index]);
        } else {
          return std::make_pair(false, value_type());
        }
      } // end of evict(key)

      bool evict(const key_type& key, value_type& ret_value) {
        const size_t index = hash_function(key) % keys.size();
        if(is_set[index] && key[index] == key) {
          is_set[index] = false;
          ret_value = values[index];
          return true;
        } else {
          return false;
        }
      }      

      bool contains(const key_type& key) const {
        const size_t index = hash_function(key) % keys.size();
        return is_set[index] && keys[index] == key;
      } // end of contains


      value_type& operator[](const key_type& key) {
        const size_t index = hash_function(key) % keys.size();
        if(is_set[index]) {
          ASSERT_TRUE(key == keys[index]);
          return values[index];
        } else {
          keys[index] = key;
          is_set[index] = true;
          values[index] = value_type();
          return values[index];
        }
      } // end of oeprator[]

      const value_type& operator[](const key_type& key) const {
        const size_t index = hash_function(key) % keys.size();
        if(is_set[index]) {
          ASSERT_TRUE(key == keys[index]);
          return values[index];
        } else {
          logstream(LOG_FATAL) << "Key not found!" << std::endl;
          return value_type();
        }
      } // end of oeprator[]

      bool get(const key_type& key, value_type& ret_value) {
        const size_t index = hash_function(key) % keys.size();
        if(is_set[index] && keys[index] == key) {
          ret_value = values[index];
          return true;
        } else {
          return false;
        }     
      } // end of get

    }; // end of class associative




  }; // end of cache namespace 
}; // end of graphlab namespace

#endif



