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



#ifndef GRAPHLAB_ATOMIC_ADD_VECTOR2_EMPTY_SPECIALIZATION_HPP
#define GRAPHLAB_ATOMIC_ADD_VECTOR2_EMPTY_SPECIALIZATION_HPP


#include <vector>


#include <parallel/pthread_tools.hpp>
#include <graphlab/util/lock_free_pool.hpp>
#include <graphlab/util/empty.hpp>
#include <util/dense_bitset.hpp>
#include <parallel/atomic_add_vector2.hpp>


namespace graphlab {

  /**
   * \TODO DOCUMENT THIS CLASS
   */

  template<>
  class atomic_add_vector2<graphlab::empty> {
  public:
    typedef graphlab::empty value_type;


  private:

    dense_bitset atomic_box_vec;


    /** Not assignable */
    void operator=(const atomic_add_vector2& other) { }


  public:
    /** Initialize the per vertex task set */
    atomic_add_vector2(size_t num_vertices = 0) {
      resize(num_vertices);
      atomic_box_vec.clear();
    }

    /**
     * Resize the internal locks for a different graph
     */
    void resize(size_t num_vertices) {
      atomic_box_vec.resize(num_vertices);
      atomic_box_vec.clear();
    }

    /** Add a task to the set returning false if the task was already
        present. */
    bool add(const size_t& idx,
             const value_type& val) {
      return !atomic_box_vec.set_bit(idx);
    } // end of add task to set


    // /** Add a task to the set returning false if the task was already
    //     present. */
    // bool add_unsafe(const size_t& idx,
    //                 const value_type& val) {
    //   ASSERT_LT(idx, atomic_box_vec.size());
    //   return atomic_box_vec[idx].set_unsafe(pool, val, joincounter);
    // } // end of add task to set


    bool add(const size_t& idx,
             const value_type& val,
             value_type& new_value) {
      return !atomic_box_vec.set_bit(idx);
    } // end of add task to set


    bool test_and_get(const size_t& idx,
                      value_type& ret_val) {
      return atomic_box_vec.clear_bit(idx);
    }

    bool peek(const size_t& idx,
                   value_type& ret_val) {
      return atomic_box_vec.get(idx);
    }

    bool empty(const size_t& idx) const {
      return !atomic_box_vec.get(idx);
    }

    size_t size() const {
      return atomic_box_vec.size();
    }

    size_t num_joins() const {
      return 0;
    }


    void clear() {
      atomic_box_vec.clear();
    }

    void clear(size_t i) { atomic_box_vec.clear_bit(i);}

  }; // end of vertex map

}; // end of namespace graphlab

#undef VALUE_PENDING

#endif

