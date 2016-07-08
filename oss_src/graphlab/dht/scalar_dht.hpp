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
 *
 */


#ifndef GRAPHLAB_SCALAR_DHT_HPP
#define GRAPHLAB_SCALAR_DHT_HPP

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
#include <graphlab/dht/dht_base.hpp>
#include <graphlab/dht/dht_index_resolution.hpp>
#include <graphlab/util/bitops.hpp>

#include <iostream>

namespace graphlab {
namespace _DHT {

template <typename K, typename V> using simple_map = std::map<K, V>;

template <typename _ValueType>
struct ScalarDHTPolicy {

  typedef WeakToken  KeyType;
  typedef _ValueType ValueType;

  // Just using the base container with a simple map
  template <typename DHTClass, typename ValueType>
  using InternalContainer = InternalContainerBase<DHTClass, simple_map, ValueType>;

  typedef StandardHashResolver Resolver;
};

}

/**
 * \ingroup rpc
 *
 * This defines a distributed hash table of scalar values for machine
 * learning.  The data is stored on a machine determined by part of
 * the hash value.  The method is intended to be used with fibers.
 *
 * The table is designed for machine learning purposes by providing a
 * number of methods specialized to ML type operations.  Furthermore,
 * any new values are implicitly initilized to zero; calling get() on
 * a value not previously set returns zero.
 *
 * The keys are given as type WeakToken, which essentially just stores
 * a 128 bit hash value.  Conversion from integer and other data types
 * is done implicitly.  Many optimizations are possible.
 *
 * \tparam T The data type.
 */
template <typename T, template <typename _VT> class PolicyTemplate = _DHT::ScalarDHTPolicy>
class scalar_dht
    : public _DHT::DHT_Base<scalar_dht<T, PolicyTemplate>, PolicyTemplate<T> > {
 public:

  typedef PolicyTemplate<T> Policy;

  typedef _DHT::DHT_Base<scalar_dht, Policy> Base;

  /// The key type.
  typedef typename Base::KeyType KeyType;

  /// The numerical value type
  typedef typename Base::ValueType ValueType;

  typedef _DHT::GetVisitor<scalar_dht, ValueType> GetVisitor;
  typedef _DHT::SetVisitor<scalar_dht, ValueType> SetVisitor;
  typedef _DHT::ApplyDeltaVisitor<scalar_dht, ValueType> ApplyDeltaVisitor;
  typedef _DHT::ApplyDeltaVisitorWithReturn<scalar_dht, ValueType> ApplyDeltaVisitorWithReturn;

 private:
  friend Base;

 public:

  /**
   * Constructs a distributed hash table.  Options may be passed in
   * with the opt_map parameter.  Currently there are no options.
   *
   * \param dc                      The distributed control object.
   */
  scalar_dht(distributed_control &dc,
             const options_map& opt_map = options_map(_DHT::_dht_default_options))
      : Base(dc, opt_map)
  {
  }

  ////////////////////////////////////////////////////////////
  // The methods defining an interface with a more fine grained
  // control.

 public:
  /**
   * Retrieves an element in the distributed hash table of scalars.
   * If the value is not present, then the default element value set
   * in the constructor is returned.
   *
   * \param t   The token used for the key.
   *
   * \return    The value token t and index idx.
   */
  ValueType get(const KeyType& t) {
    return this->_apply(t, GetVisitor());
  }

  /**
   * Retrieves multiple elements in the distributed hash table of
   * scalars.  If values are not present, then the default element
   * value set in the constructor is returned.
   *
   * \tparam _KeyType    A type convertable into the actual key type.
   * \param key_vector   A vector used for key storage.
   *
   * \return A vector of values corresponding to elements in the hash
   * corresponding to the retrieved values.
   */
  template <typename _KeyType>
  std::vector<ValueType> batch_get(const std::vector<_KeyType>& key_vector) {
    return this->_batch_apply(key_vector, GetVisitor());
  }

  /**
   * Sets an element.
   *
   * \param key    The key to be set.
   * \param value  The value to set in the array.
   */
  void set(const KeyType& key, ValueType value) {
    this->_apply(key, SetVisitor(value));
  }

  /**
   * Sets a collection of elements.
   *
   * \tparam _KeyType    A type convertable into the actual key type.
   * \param key_vector   A vector of keys to set.
   * \param value A single value that all elements in key_vector are
   * set to.
   *
   */
  template <typename _KeyType, typename _ValueType>
  void batch_set(const std::vector<_KeyType>& keys,
                 _ValueType& value) {
    this->_batch_apply(keys, SetVisitor(value));
  }

  /**
   * Sets a collection of elements in the distributed hash table of scalars.
   *
   * \tparam _KeyType    A type convertable into the actual key type.
   * \param keys         A vector of keys to set.
   *
   * \param value A single value that all elements in key_vector are
   * set to.
   *
   */
  template <typename _KeyType>
  void batch_set(const std::vector<_KeyType>& keys,
                 ValueType value) {
    this->_batch_apply(keys, SetVisitor(value));
  }

  /**
   * Sets a collection of elements in the distributed hash table of scalars.
   *
   * \tparam _KeyType    A type convertable into the actual key type.
   * \param keys          A vector of keys to set.
   *
   * \tparam _ValueType  A type convertable into the numerical type ValueType.
   *
   * \param values A vector of values corresponding to the keys in
   * keys.  This array must be the same length as keys.
   *
   */
  template <typename _KeyType, typename _ValueType>
  void batch_set(const std::vector<_KeyType>& keys,
                 const std::vector<_ValueType>& values,
                 typename std::enable_if<std::is_convertible<_ValueType, ValueType>::value>::type* = 0) {

    ASSERT_EQ(keys.size(), values.size());

    std::vector<SetVisitor> set_visitors;
    set_visitors.reserve(values.size());

    for(const auto& v : values)
      set_visitors.push_back(SetVisitor(v));

    this->_batch_apply(keys, set_visitors);
  }


  /**
   * Atomically applies a delta (+= operation) to an element in the
   * distributed hash table of scalars.  If the value is not present,
   * then an element with the default value 0 is created.  The delta
   * is then applied to this element.  The new value is returned (if
   * the new value is not needed, use the simple method apply_delta).
   *
   * \param key    The token used for the key.
   * \param delta  The value to add to the value currently set in the array.
   *
   * \return       The new value after the delta is applied.
   */
  ValueType apply_delta_return_new(const KeyType& key, ValueType delta) {
    return this->_apply(key, ApplyDeltaVisitorWithReturn(delta));
  }

  /**
   * Atomically applies a set of deltas (+= operation) to elements in
   * the distributed hash table of scalars.  In this version, the
   * delta value is common between all operations. If a value is not
   * present, then an element with the default value is created.  The
   * delta is then applied to this element.  A vector of the new
   * values is returned.
   *
   * \param keys A vector of key values to apply the delta value on.
   * \param delta A scalar delta value to apply to all the entries
   * corresponding to the keys in keys.
   *
   * \return A vectors of values corresponding to the values in the
   * tree given by the existing values.
   */
  template <typename _KeyType>
  std::vector<ValueType> batch_apply_delta_return_new(const std::vector<_KeyType>& keys, ValueType delta) {
    return this->_batch_apply(keys, ApplyDeltaVisitorWithReturn(delta));
  }

  /**
   * Atomically applies a set of deltas (+= operation) to an element
   * in the distributed hash table of scalars.  The values are given
   * by a vector of delta values. If the value is not present, then an
   * element with the default value is created.  The delta is then
   * applied to this element.  The new value is returned.
   *
   * \tparam _KeyType    A type convertable into the actual key type.
   * \param keys          A vector of keys to set.
   *
   * \tparam _ValueType  A type convertable into the numerical type ValueType.
   *
   * \param deltas A vector of delta values corresponding to the keys in
   * keys.  This array must be the same length as keys.
   *
   * \return A vectors of values corresponding to the new values after
   * application of the deltas.
   */
  template <typename _KeyType, typename _ValueType>
  std::vector<ValueType> batch_apply_delta_return_new(
      const std::vector<_KeyType>& keys,
      const std::vector<_ValueType>& deltas,
      typename std::enable_if<std::is_convertible<_ValueType, ValueType>::value>::type* = 0) {

    ASSERT_EQ(keys.size(), deltas.size());

    std::vector<ApplyDeltaVisitorWithReturn> apply_deltas;
    apply_deltas.reserve(keys.size());

    for(const auto& delta : deltas)
      apply_deltas.emplace_back(delta);

    return this->_batch_apply(keys, apply_deltas);
  }

  /**
   * Atomically applies a delta (+= operation) to an element in the
   * distributed hash table of scalars.  If the value is not present,
   * then an element with the default value 0 is created.  The delta
   * is then applied to this element.
   *
   * \param key    The token used for the key.
   * \param delta  The value to add to the value currently set in the array.
   *
   */
  void apply_delta(const KeyType& t, ValueType delta) {
     this->_apply(t, ApplyDeltaVisitor(delta));
  }

  /**
   * Atomically applies a set of deltas (+= operation) to elements in
   * the distributed hash table of scalars.  In this version, the
   * delta value is common between all operations. If a value is not
   * present, then an element with the default value is created.  The
   * delta is then applied to this element.
   *
   * \param keys A vector of key values to apply the delta value on.
   * \param delta A scalar delta value to apply to all the entries
   * corresponding to the keys in keys.
   *
   */
  template <typename _KeyType>
  void batch_apply_delta(const std::vector<_KeyType>& keys, ValueType delta) {
    this->_batch_apply(keys, ApplyDeltaVisitor(delta));
  }


  /**
   * Atomically applies a set of deltas (+= operation) to an element
   * in the distributed hash table of scalars.  The values are given
   * by a vector of delta values. If the value is not present, then an
   * element with the default value is created.  The delta is then
   * applied to this element.  The new value is returned.
   *
   * \tparam _KeyType    A type convertable into the actual key type.
   * \param keys          A vector of keys to set.
   *
   * \tparam _ValueType  A type convertable into the numerical type ValueType.
   *
   * \param deltas A vector of delta values corresponding to the keys in
   * keys.  This array must be the same length as keys.
   *
   */
  template <typename _KeyType, typename _ValueType>
  void batch_apply_delta(
      const std::vector<_KeyType>& keys,
      const std::vector<_ValueType>& deltas,
      typename std::enable_if<std::is_convertible<_ValueType, ValueType>::value>::type* = 0) {

    ASSERT_EQ(keys.size(), deltas.size());

    std::vector<ApplyDeltaVisitor> apply_deltas;
    apply_deltas.reserve(keys.size());

    for(ValueType v : deltas)
      apply_deltas.emplace_back(v);

    this->_batch_apply(keys, apply_deltas);
  }

  /**
   * Returns True if a key is stored locally -- and thus can be quickly accessed -- and false otherwise.
   *
   * \param key      The token used for the key.
   */
  bool is_local(const KeyType& key) {
    return this->_is_local(key);
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Internal interface methods
};
}

#endif
