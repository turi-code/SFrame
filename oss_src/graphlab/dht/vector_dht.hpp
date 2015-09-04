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
 *
 */


#ifndef GRAPHLAB_VECTOR_DHT_HPP
#define GRAPHLAB_VECTOR_DHT_HPP

#include <unordered_map>
#include <vector>
#include <algorithm>
#include <parallel/pthread_tools.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <parallel/atomic.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_remote_request.hpp>
#include <graphlab/util/token.hpp>
#include <graphlab/dht/dht_base_old.hpp>

#include <iostream>

namespace graphlab {

/**
 * \ingroup rpc
 *
 * This defines a distributed hash table of vectors for machine
 * learning.  Sizing of vectors is done automatically, but a default
 * value must be included.  Currently, only get and set operations
 * are supported.  This is in heavy development.
 *
 * The keys are given as type Token.  Many optimizations are possible.
 *
 * \tparam T The data type
 */
template <typename T>
class vector_dht : public _DHT::DHT_BaseOld<vector_dht<T>,
                                         _DHT::InternalHashMapContainer<std::vector<T> > > {
 public:

  /// The numeric value type given in the
  typedef T NumericValueType;

  /// The type of the vector holding the data.
  typedef std::vector<NumericValueType> VecValueType;

 private:
  typedef typename _DHT::DHT_BaseOld<vector_dht<T>, _DHT::InternalHashMapContainer<VecValueType> > Base;

 public:
  /// The key type.
  typedef typename Base::KeyType KeyType;

 private:
  friend Base;

 public:
  typedef typename Base::InternalContainer InternalContainer;

 private:

  ////////////////////////////////////////////////////////////
  // Details of the different tables.

  static constexpr size_t internal_hash_offset = 64;
  static constexpr size_t machine_hash_offset = 96;

 private:

  // Controlling the table sizing policies
  static constexpr size_t table_resize_policy(size_t req_idx) { return (5 * (req_idx + 1)) / 4; }
  static constexpr size_t new_table_size_policy(size_t n_tables, size_t total_sizes, size_t req_idx) {
    // On a new table reserve 2/3rds of the existing average.  Can be tweaked.
    return std::max(std::max(size_t(8),
                             (total_sizes / (3 * (1 + n_tables) / 2))),
                    table_resize_policy(req_idx));
  }

 public:

  /**
   * Constructs a distributed hash table.  Options may be passed in
   * with the opt_map parameter.
   *
   * \li \b default_value (default: 0)  The default numeric value all entries start out with.
   *
   * \li \b vector_size (default: 0) If not zero, forces a size policy
   * on the vectors -- all new vectors will have this as the forced
   * size.  Attempting to access other elements will throw an error.
   *
   * \param dc                      The distributed control object.
   */
  vector_dht(distributed_control &dc,
             const options_map& opt_map = options_map(_DHT::_dht_default_options_old))
      : Base(dc, opt_map)
      , _default_element_value(_DHT::get_option_old<NumericValueType>(opt_map, "default_value", 0))
      , _default_vector_size(_DHT::get_option_old<size_t>(opt_map, "vector_size", 0))
      , _total_table_sizes(0)
  {
  }

  ////////////////////////////////////////////////////////////
  // The methods defining an interface with a more fine grained
  // control.

 public:
  /**
   * Retrieves an element in the distributed hash table of vectors.
   * If the value is not present, then the default element value set
   * in the constructor is returned.
   *
   * \param t   The token used for the key.
   * \param idx The index in the vector of the requested element.
   *
   * \return    The value token t and index idx.
   */
  NumericValueType get(const KeyType& t, size_t idx) {

    procid_t owningmachine = this->_owning_machine(t);

    // Lock the appropriate table if it's on our machine.
    if (owningmachine == this->rmi.dc().procid()) {
      return _get_local(t, idx);
    } else {

      // otherwise I need to go to another machine
      return object_fiber_remote_request(this->rmi, owningmachine,
                                         &vector_dht::_get_local,
                                         t, idx)();
    }
  }


  /**
   * Retrieves a full vector in the distributed hash table.  If the
   * key is not present, then an empty vector is returned if dynamic
   * sizing is used, and one of the proper size otherwise.
   *
   * \param t   The token used for the key.
   *
   * \return    The vector at token t.
   */
  VecValueType get_vector(const KeyType& t) {

    procid_t owningmachine = this->_owning_machine(t);

    // Lock the appropriate table if it's on our machine.
    if (owningmachine == this->rmi.dc().procid()) {
      return _get_vector_local(t);
    } else {

      // otherwise I need to go to another machine
      return object_fiber_remote_request(this->rmi, owningmachine,
                                         &vector_dht::_get_vector_local,
                                         t)();
    }
  }
    
  /**
   * Sets an element in the distributed hash table of vectors.  If
   * the value is not present, then a vector is created filled with
   * the default values up to the idx unless a fixed vector size is
   * specified, in which case a full length vector is created.
   *
   * \param t      The token used for the key.
   * \param idx    The index in the vector of the requested element.
   * \param value  The value to set in the array.
   */
  void set(const KeyType& t, size_t idx, NumericValueType value) {

    procid_t owningmachine = this->_owning_machine(t);

    // Lock the appropriate table if it's on our machine.
    if (owningmachine == this->rmi.dc().procid()) {
      _set_local(t, idx, value);

    } else {

      // otherwise I need to go to another machine
      this->rmi.remote_call(owningmachine, &vector_dht::_set_local, t, idx, value);
    }
  }

  /**
   * Sets a full vector in the distributed hash table of vectors.
   * Any existing vectors are overwritten.  An error is raised if
   * the dht requires a fixed size but the given vector is not this
   * size.
   *
   * \param t      The token used for the key.
   * \param vec    The vector of values to set in the array.
   */
  void set_vector(const KeyType& t, const VecValueType& vec) {

    if(_using_fixed_vector_size()) {
      ASSERT_EQ(vec.size(), _default_vector_size);
    }
      
    procid_t owningmachine = this->_owning_machine(t);

    // Lock the appropriate table if it's on our machine.
    if (owningmachine == this->rmi.dc().procid()) {
      _set_vector_local(t, vec);
    } else {

      // otherwise I need to go to another machine
      this->rmi.remote_call(owningmachine, &vector_dht::_set_vector_local, t, vec);
    }
  }

  /**
   * Atomically applies a delta (+= operation) to an element in the
   * distributed hash table of vectors.  If the value is not
   * present, then a vector is created filled with the default
   * values up to the idx unless a fixed vector size is specified,
   * in which case a full length vector is created.  The delta is
   * then applied to this vector.  The new value is returned.
   *
   * \param t      The token used for the key.
   * \param idx    The index in the vector of the requested element.
   * \param value  The value to set in the array.
   *
   * \return       The new value after the value is applied.
   */
  NumericValueType apply_delta(const KeyType& t, size_t idx, NumericValueType delta) {

    procid_t owningmachine = this->_owning_machine(t);

    // Lock the appropriate table if it's on our machine.
    if (owningmachine == this->rmi.dc().procid()) {
      return _apply_delta_local(t, idx, delta);
    } else {

      // otherwise I need to go to another machine
      return object_fiber_remote_request(this->rmi, owningmachine,
                                         &vector_dht::_apply_delta_local,
                                         t, idx, delta)();
    }
  }

  /**
   * Returns True if a key is stored locally -- and thus can be quickly accessed -- and false otherwise.
   *
   * \param t      The token used for the key.
   */
  bool is_local(const KeyType& t) {
    return _is_local(t);
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Internal interface methods

 private:

  ////////////////////////////////////////////////////////////////////////////////
  // Local access functions

  NumericValueType _get_local(const KeyType& t, size_t idx) {

    InternalContainer& ic = this->_get_internal_container(t);
    _DHT::ScopedLock locker(ic.lock);

    // cout << rmi.dc().procid() << "Getting value " << t.str() << "," << idx << endl;

    // Get the vector
    auto it = ic.table.find(t);

    if(it == ic.table.end())
      return _default_element_value;

    VecValueType& vec = it->second;

    if(_using_fixed_vector_size()) {

      DASSERT_EQ(vec.size(), _default_vector_size);
      DASSERT_LT(idx, vec.size());

      return vec[idx];
    } else {
      return (idx < vec.size()) ? vec[idx] : _default_element_value;
    }
  }

  VecValueType _get_vector_local(const KeyType& t) {

    InternalContainer& ic = this->_get_internal_container(t);
    _DHT::ScopedLock locker(ic.lock);

    // cout << rmi.dc().procid() << "Getting value " << t.str() << "," << idx << endl;

    // Get the vector
    auto it = ic.table.find(t);

    if(it == ic.table.end()) {
      return (_using_fixed_vector_size()
              ? VecValueType(_default_vector_size, _default_element_value)
              : VecValueType() );
    } else {
      return it->second;
    }
  }

  void _set_local(const KeyType& t, size_t idx, NumericValueType value) {

    InternalContainer& ic = this->_get_internal_container(t);
    _DHT::ScopedLock locker(ic.lock);

    // cout << rmi.dc().procid() << "Setting value " << t.str() << "," << idx << endl;

    // Get the vector
    VecValueType& vec = _get_sized_vector_in_locked_table(ic, t, idx);

    DASSERT_LT(idx, vec.size());
    vec[idx] = value;
  }

  void _set_vector_local(const KeyType& t, const VecValueType& vec) {

    InternalContainer& ic = this->_get_internal_container(t);
    _DHT::ScopedLock locker(ic.lock);

    ic.table[t] = vec;
  }

  NumericValueType _apply_delta_local(const KeyType& t, size_t idx, NumericValueType delta) {

    InternalContainer& ic = this->_get_internal_container(t);
    _DHT::ScopedLock locker(ic.lock);

    VecValueType& vec = _get_sized_vector_in_locked_table(ic, t, idx);
    vec[idx] += delta;

    return vec[idx];
  }

  ////////////////////////////////////////////////////////////
  // Common control structures

  NumericValueType _default_element_value;
  size_t _default_vector_size;
  size_t _total_table_sizes;
  size_t _n_tables;

  bool _using_fixed_vector_size() const {
    return _default_vector_size != 0;
  }

  ////////////////////////////////////////////////////////////
  // Handling the vector stuff internal in the containers

  VecValueType& _get_sized_vector_in_locked_table(InternalContainer& ic, const KeyType& t, size_t req_idx) {
    auto it = ic.table.find(t);

    if(it == ic.table.end()) {
      return _init_vector_in_locked_table(ic, t, req_idx);
    } else {
      VecValueType& vec = it->second;
      if(_using_fixed_vector_size()) {
        return vec;
      } else {
        if(req_idx >= vec.size())
          _resize_vector_in_locked_table(vec, req_idx);
        DASSERT_LT(req_idx, vec.size());
        return vec;
      }
    }
  }

  VecValueType& _init_vector_in_locked_table(InternalContainer& ic, const KeyType& t, size_t req_idx) {

    // Set the new vector size to be large enough to contain the
    // requesting index plus a bit and at least as large as the
    // average of everything else.

    if(_using_fixed_vector_size()) {
      VecValueType& vec = ic.table.emplace(t, VecValueType()).first->second;
      vec.insert(vec.end(), _default_vector_size, _default_element_value);

      return vec;

    } else {
      // Build the vector
      VecValueType& vec = ic.table.emplace(t, VecValueType()).first->second;

      size_t new_vec_size = new_table_size_policy(_n_tables, _total_table_sizes, req_idx);
      vec.reserve(new_vec_size);

      // Resize it down to the value of the required index so we can
      // accurately keep track of the average size of the vectors.
      // The extra capacity is still there.
      vec.insert(vec.end(), req_idx + 1, _default_element_value);

      _total_table_sizes += (req_idx + 1);
      ++_n_tables;

      DASSERT_EQ(req_idx + 1, vec.size());

      return vec;
    }
  }

  void _resize_vector_in_locked_table(VecValueType& vec, size_t req_idx) {
    DASSERT_FALSE(_using_fixed_vector_size());

    // static mutex local_lock;
    // local_lock.lock();

    size_t v_size = vec.size();

    if(req_idx < vec.capacity())
      vec.reserve(table_resize_policy(req_idx));

    size_t n_added_elements = ((req_idx + 1) - vec.size());

    // Needs to be atomic, maybe?
    _total_table_sizes += n_added_elements;

    vec.insert(vec.end(), n_added_elements, _default_element_value);

    DASSERT_EQ(vec.size(), n_added_elements + v_size);
    DASSERT_EQ(vec.size(), req_idx + 1);

    // local_lock.unlock();
  }

};
}

#endif
