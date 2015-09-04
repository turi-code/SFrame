/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_DHT_INTERNAL_CONTAINER_H_
#define GRAPHLAB_DHT_INTERNAL_CONTAINER_H_

#include <unordered_map>
#include <vector>
#include <algorithm>
#include <type_traits>
#include <parallel/pthread_tools.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <parallel/atomic.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_remote_request.hpp>
#include <graphlab/util/token.hpp>
#include <graphlab/util/bitops.hpp>
#include <graphlab/options/options_map.hpp>
#include <graphlab/dht/visitors.hpp>

#include <iostream>

namespace graphlab {
namespace _DHT {

////////////////////////////////////////////////////////////////////////////////
// Several types of internal containers for special use

template <typename _DHT,
          template <typename K, typename V> class _StorageContainer,
          typename _ValueType>
class InternalContainerBase {
 public:
  InternalContainerBase() {}
  InternalContainerBase(const InternalContainerBase&) = delete;
  InternalContainerBase operator=(const InternalContainerBase&) = delete;

  typedef WeakToken KeyType;
  typedef uint64_t LocalKeyType;
  typedef _ValueType ValueType;
  typedef _StorageContainer<LocalKeyType, ValueType> StorageType;
  typedef _DHT DHT;

  static constexpr LocalKeyType getLocalKey(const KeyType& k) {
    return LocalKeyType(k.hash());
  }

  template <typename Visitor>
  typename Visitor::ReturnType
  apply(DHT* local_dht_instance, LocalKeyType key, Visitor getter,
        _ENABLE_IF_RETURNS_VOID(Visitor))
  {
    _lock.lock();
    getter(local_dht_instance, table, key);
    _lock.unlock();
  }

  template <typename Visitor>
  typename Visitor::ReturnType
  apply(DHT* local_dht_instance, LocalKeyType key, Visitor getter,
        _ENABLE_IF_RETURNS_VALUE(Visitor)) {

    _lock.lock();
    typename Visitor::ReturnType r = getter(local_dht_instance, table, key);
    _lock.unlock();

    return r;
  }

private:

  mutable mutex _lock;
  StorageType table;
};

}}

#endif /* _DHT_INTERNAL_H_ */
