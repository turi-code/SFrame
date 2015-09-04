/**
 * Copyright (C) 2015 Dato, Inc.
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


#ifndef GRAPHLAB_MESSAGE_ARRAY_HPP
#define GRAPHLAB_MESSAGE_ARRAY_HPP


#include <vector>
#include <parallel/pthread_tools.hpp>
#include <parallel/atomic.hpp>
#include <graphlab/scheduler/get_message_priority.hpp>
namespace graphlab {

  /**
   * \TODO DOCUMENT THIS CLASS
   */ 
  
  template<typename ValueType>
  class message_array {
  public:
    typedef ValueType value_type;

  private:    
    struct message_box {
      value_type value;
      bool empty;

      message_box() : empty(true) { }
      /** returns false if element is already present */
      inline bool add(const value_type& other, double& priority) {
        if (empty) {
          value = other;
          empty = false;
          priority = scheduler_impl::get_message_priority(value);
          return true;
        } else {
          value += other;
          priority = scheduler_impl::get_message_priority(value);
          return false;
        }
      }
                
      void clear() {
        value = value_type();
        empty = true;
      }
      
    }; 

    std::vector<message_box> message_vector;
    // lock array
    simple_spinlock lock_array[65536];
    size_t joincounter[65536];
    size_t addcounter[65536];

    /** Not assignable */
    void operator=(const message_array& other) { }

    static size_t get_lock_idx(size_t i) {
      return i % 65536;
    }
  public:
    /** Initialize the per vertex task set */
    message_array(size_t num_vertices = 0) :
              message_vector(num_vertices) { 
      for (size_t i = 0; i < 65536; ++i) {
        joincounter[i] = 0; 
        addcounter[i] = 0;
      }
    }

    /**
     * Resizes the number of elements this message vector can hold
     */
    void resize(size_t num_vertices) {
      message_vector.resize(num_vertices);
    }

    /** Add a message to the set returning false if a message is already
        present. */
    bool add(const size_t idx, 
             const value_type& val,
             double* message_priority = NULL) {
      double priority;
      size_t lockidx = get_lock_idx(idx);
      lock_array[lockidx].lock();
      bool ret = message_vector[idx].add(val, priority);
      joincounter[lockidx] += !ret;
      addcounter[lockidx]++;
      lock_array[lockidx].unlock();
      if (message_priority) (*message_priority) = priority;
      return ret;
    } 

    /** Returns the current message stored at idx and 
     * clears the message.
     * Returns true on success and false if there is no message
     * stored at the index.
     */
    bool get(const size_t idx,
             value_type& ret_val) {
      bool has_val = false;
      size_t lockidx = get_lock_idx(idx);
      lock_array[lockidx].lock();
      if (!message_vector[idx].empty) {
        ret_val = message_vector[idx].value;
        message_vector[idx].clear();
        has_val = true;
      }
      lock_array[lockidx].unlock();
      return has_val;
    }

    /** Returns the current message stored at idx. 
     * Returns true on success and false if there is no message
     * stored at the index.
     * Does not change the contents of the message
     */
    bool peek(const size_t idx,
              value_type& ret_val) {
      bool has_val = false;
      size_t lockidx = get_lock_idx(idx);
      lock_array[lockidx].lock();
      if (!message_vector[idx].empty) {
        ret_val = message_vector[idx].value;
        has_val = true;
      }
      lock_array[lockidx].unlock();
      return has_val;
    }
    

    /// clears the message at a particular idx
    void clear(const size_t idx) { 
      size_t lockidx = get_lock_idx(idx);
      lock_array[lockidx].lock();
      message_vector[idx].clear(); 
      lock_array[lockidx].unlock();
    }

    /// Returns true if the message at position idx is empty
    bool empty(const size_t idx) const {
      return message_vector[idx].empty;
    }

    bool empty() const {
      for (size_t i = 0;i < message_vector.size(); ++i) {
        if (!message_vector[i].empty) return false;
      }
      return true;
    }

    /// Returns the length of the message vector
    size_t size() const { 
      return message_vector.size(); 
    }
    
    size_t num_joins() const { 
      size_t total_joins = 0;
      for (size_t i = 0; i < 65536; ++i) {
        total_joins += joincounter[i];
      }
      return total_joins;
    }


    size_t num_adds() const { 
      size_t total_adds = 0;
      for (size_t i = 0; i < 65536; ++i) {
        total_adds += addcounter[i];
      }
      return total_adds;
    }

    /// not thread safe. Clears all contents
    void clear() {
      for (size_t i = 0; i < message_vector.size(); ++i) clear(i);
    }

    
  }; // end of vertex map

}; // end of namespace graphlab

#undef VALUE_PENDING

#endif

