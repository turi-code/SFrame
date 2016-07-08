/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_DHT_INTERNAL_OLD_H_
#define GRAPHLAB_DHT_INTERNAL_OLD_H_

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
#include <graphlab/util/bitops.hpp>
#include <graphlab/options/options_map.hpp>

#include <iostream>

namespace graphlab {
namespace _DHT
{

static const std::map<std::string, std::string> _dht_default_options_old =
{ {"default_element_value", "0"},
  {"vector_size",   "0"}};

////////////////////////////////////////////////////////////////////////////////
// Several types of internal containers for special use

template <typename _ValueType>
struct InternalHashMapContainer {
  InternalHashMapContainer() {}
  InternalHashMapContainer(const InternalHashMapContainer&) = delete;
  InternalHashMapContainer& operator=(const InternalHashMapContainer&) = delete;

  typedef WeakToken KeyType;
  typedef _ValueType ValueType;
  typedef std::unordered_map<KeyType, ValueType> StorageType;

  // Need to define lock or table
  mutex lock;
  StorageType table;
};

////////////////////////////////////////////////////////////////////////////////
// Some convenience structures and routines

template <typename T>
static inline T get_option_old(const options_map& opt, const std::string& s, const T& def_val) {
  T v = def_val;
  opt.get_option(s, v);
  return v;
}

struct ScopedLock {
  ScopedLock(mutex& _lock) : lock(_lock) { lock.lock();   }
  ~ScopedLock()                          { lock.unlock(); }
 private:
  mutex& lock;
};

template <typename ContainerObject, typename _InternalContainer>
class DHT_BaseOld {
 public:

  typedef WeakToken KeyType;
  typedef ContainerObject DerivedType;
  typedef _InternalContainer InternalContainer;
  typedef typename InternalContainer::ValueType ValueType;

 protected:
  mutable dc_dist_object<DerivedType> rmi;

  static constexpr size_t internal_hash_offset = 64;
  static constexpr size_t machine_hash_offset  = 96;

 private:
  const unsigned int _n_bits_container_lookup;
  size_t _container_lookup_size;
  InternalContainer* _container_lookup;

 protected:
  DHT_BaseOld(distributed_control& dc, const options_map& opt)
      : rmi(dc, static_cast<DerivedType*>(this) )
      , _n_bits_container_lookup(
          bitwise_log2_ceil(std::max(get_option_old<size_t>(opt, "min_dht_internal_size", 64),
                                     size_t(dc.numprocs() * dc.numprocs()))))
      , _container_lookup_size(1 << _n_bits_container_lookup)
      , _container_lookup(new InternalContainer[_container_lookup_size])
  {
  }

 public:
  DHT_BaseOld(const DHT_BaseOld&) = delete;
  DHT_BaseOld& operator=(const DHT_BaseOld&) = delete;

 protected:
  virtual ~DHT_BaseOld() {
    delete[] _container_lookup;
    _container_lookup = nullptr;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // All the internal functions used by the child class

  void finalize_setup() {
    rmi.barrier();
  }

  InternalContainer& _get_internal_container(const KeyType& key) {

    size_t index = bitwise_pow2_mod(key.hash() >> internal_hash_offset, _n_bits_container_lookup);

    DASSERT_LT(index, _container_lookup_size);

    return _container_lookup[index];
  }

  procid_t _owning_machine(const KeyType& t) const {
    procid_t r = (t.hash() >> 64) % rmi.dc().numprocs();
    return r;
  }

  bool _is_local(const KeyType& t) const {
    return _owning_machine(t) == rmi.dc().procid();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Direct interface methods

};
}}

#endif /* _DHT_INTERNAL_H_ */
