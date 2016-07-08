/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_DHT_INTERNAL_H_
#define GRAPHLAB_DHT_INTERNAL_H_

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

#include <graphlab/dht/visitors.hpp>
#include <graphlab/dht/dht_internal_container.hpp>
#include <graphlab/dht/batch_visitors.hpp>

#include <iostream>

namespace graphlab {
namespace _DHT {

static const std::map<std::string, std::string> _dht_default_options =
{ {"vector_size",   "0"} };

////////////////////////////////////////////////////////////////////////////////
// Several types of internal containers for special use

////////////////////////////////////////////////////////////////////////////////
// Some convenience structures and routines

template <typename T>
static inline T get_option(const options_map& opt, const std::string& s, const T& def_val) {
  T v = def_val;
  opt.get_option(s, v);
  return v;
}

////////////////////////////////////////////////////////////////////////////////
// The base

template <typename DHTClass, typename Policy>
class DHT_Base {
 public:

  typedef DHTClass DerivedType;

  typedef typename Policy::KeyType KeyType;
  typedef typename Policy::ValueType ValueType;
  typedef typename Policy::template InternalContainer<DHTClass, ValueType> InternalContainer;
  typedef typename Policy::Resolver::InternalTableIndexType InternalTableIndexType;
  typedef typename InternalContainer::LocalKeyType InternalContainerKeyType;

  template <typename VisitorContainer>
  using BatchVisitor = _DHT::BatchVisitor<DHTClass, VisitorContainer>;

 protected:
  mutable dc_dist_object<DerivedType> rmi;

  uint n_machines() const {return rmi.dc().numprocs(); }
  uint this_machine() const {return rmi.dc().procid(); }

 private:
  static constexpr uint n_bits_container_lookup =
      Policy::Resolver::n_bits_container_lookup;

  static constexpr uint internal_container_lookup_size =
      (uint(1) << n_bits_container_lookup);

  static constexpr uint internal_container_lookup_hash_offset =
                  Policy::Resolver::internal_container_lookup_hash_offset;

  static constexpr uint machine_lookup_hash_offset =
                  Policy::Resolver::machine_lookup_hash_offset;

  InternalContainer* _container_lookup;

  DerivedType* this_ptr() {
    return static_cast<DerivedType*>(this);
  }

  InternalContainer& getTableContainer(InternalTableIndexType table_idx) const {
    DASSERT_LT(uint(table_idx), uint(internal_container_lookup_size));
    return _container_lookup[uint(table_idx)];
  }

 protected:

  DHT_Base(distributed_control& dc, const options_map& opt)
      : rmi(dc, this_ptr())
      , _container_lookup(new InternalContainer[internal_container_lookup_size])
      , _num_batch_container_structures(fiber_control::get_instance().num_workers() + 1)
      , _batch_container_structures(new _BatchContainerStructures[_num_batch_container_structures])
  {
    for(size_t i = 0; i < _num_batch_container_structures; ++i)
      _batch_container_structures[i].setup(this);
  }

 public:
  DHT_Base(const DHT_Base&) = delete;
  DHT_Base& operator=(const DHT_Base&) = delete;

 protected:
  virtual ~DHT_Base() {
    delete[] _container_lookup;
    _container_lookup = nullptr;
    delete[] _batch_container_structures;
    _batch_container_structures = nullptr;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // All ready to go!

  template <typename Visitor>
  typename Visitor::ReturnType _apply(KeyType key, Visitor a) {

    InternalTableIndexType table_idx = Policy::Resolver::getInternalTableIndex(key);
    uint machine_idx = Policy::Resolver::getMachineIndex(rmi.dc(), key);
    InternalContainerKeyType ic_key = InternalContainer::getLocalKey(key);

    if(machine_idx == this_machine()) {
      return _apply_local(table_idx, ic_key, a);
    } else {
      return _apply_remote(machine_idx, table_idx, ic_key, a);
    }
  }

  ////////////////////////////////////////
  // Remote calling routines; need to split these up by return types

  template <typename Visitor>
  typename Visitor::ReturnType _apply_local(
      InternalTableIndexType table_idx, InternalContainerKeyType ic_key, Visitor a) {

    return _container_lookup[table_idx].apply(this_ptr(), ic_key, a);
  }

  ////////////////////////////////////////
  // Remote calling routines; need to split these up by return types

  template <typename Visitor>
  typename Visitor::ReturnType _apply_remote(
      uint machine_idx,
      InternalTableIndexType table_idx,
      InternalContainerKeyType ic_key,
      Visitor a,
      _ENABLE_IF_RETURNS_VALUE(Visitor)) {

    return object_fiber_remote_request(rmi, machine_idx,
                                       &DHT_Base::template _apply_local<Visitor>,
                                       table_idx, ic_key, a)();
  }

  // Void type accessor handling
  template <typename Visitor>
  void _apply_remote(
      uint machine_idx,
      InternalTableIndexType table_idx,
      InternalContainerKeyType ic_key,
      Visitor a,
      _ENABLE_IF_RETURNS_VOID(Visitor)) {

    return rmi.remote_call(machine_idx,
                           &DHT_Base::template _apply_local<Visitor>,
                           table_idx, ic_key, a);
  }


  ////////////////////////////////////////////////////////////////////////////////
  // Batch versions of the above

  template <typename Visitor, typename VisitorContainer>
  Visitor __get_visitor(VisitorContainer v, size_t) {
    return v;
  }

  template <typename Visitor>
  Visitor __get_visitor(const std::vector<Visitor>& v, size_t) {

  }

  struct _BatchContainerStructures {
    _BatchContainerStructures() {}

    void setup(DHT_Base* _dht_ptr) {
      dht_ptr = _dht_ptr;
      table_idx_vect.resize(dht_ptr->n_machines());
      table_keys_vect.resize(dht_ptr->n_machines());
    }

    DHT_Base* dht_ptr;

    std::vector<std::vector<InternalTableIndexType> > table_idx_vect;
    std::vector<std::vector<InternalContainerKeyType> > table_keys_vect;

    void clear() {

      for(auto& tiv : table_idx_vect)
        tiv.clear();

      for(auto& tkv : table_keys_vect)
        tkv.clear();
    }

    uint add(KeyType key) {
      uint machine_idx = Policy::Resolver::getMachineIndex(dht_ptr->rmi.dc(), key);

      table_idx_vect[machine_idx].push_back(Policy::Resolver::getInternalTableIndex(key));
      table_keys_vect[machine_idx].push_back(InternalContainer::getLocalKey(key));

      return machine_idx;
    }
  };

  uint _num_batch_container_structures;
  _BatchContainerStructures *_batch_container_structures;

  _BatchContainerStructures _get_bcs() {
    uint thread_id = (fiber_control::get_worker_id() + 1);

    DASSERT_LT(thread_id, fiber_control::get_instance().num_workers() + 1);
    _BatchContainerStructures& bcs = _batch_container_structures[thread_id];
    bcs.clear();

    return bcs;
  }

  // Step 1: Build the direction tables.  Template switched on vector of visitors or single visitor.

  template <typename _KeyType, typename VisitorContainer>
  typename BatchVisitor<VisitorContainer>::BatchReturnType
  _batch_apply(const std::vector<_KeyType>& keys,
               const VisitorContainer& visitor,
               typename std::enable_if<BatchVisitor<VisitorContainer>::single_visitor>::type* = 0) {

    _BatchContainerStructures bcs = _get_bcs();

    for(const auto& key : keys)
      bcs.add(key);

    return _batch_apply_step_2(bcs, keys, std::vector<VisitorContainer>(n_machines(), visitor));
  }

  template <typename _KeyType, typename VisitorContainer>
  typename BatchVisitor<VisitorContainer>::BatchReturnType
  _batch_apply(const std::vector<_KeyType>& keys,
               const VisitorContainer& visitors,
               typename std::enable_if<BatchVisitor<VisitorContainer>::vector_of_visitors>::type* = 0) {

    _BatchContainerStructures bcs = _get_bcs();

    DASSERT_EQ(keys.size(), visitors.size());

    std::vector<std::vector<typename BatchVisitor<VisitorContainer>::VisitorType> > visitor_map(n_machines());

    for(size_t i = 0; i < keys.size(); ++i) {
      uint machine_idx = bcs.add(keys[i]);
      visitor_map[machine_idx].push_back(visitors[i]);
    }

    return _batch_apply_step_2(bcs, keys, visitor_map);
  }

  // Step 2: Build the batched visitors for each of the machines

  template <typename _KeyType, typename VisitorContainer>
  typename BatchVisitor<VisitorContainer>::BatchReturnType
  _batch_apply_step_2(_BatchContainerStructures& bcs,
                      const std::vector<_KeyType>& keys,
                      const std::vector<VisitorContainer>& vv) {

    // Split everything
    std::vector<BatchVisitor<VisitorContainer> > batch_visitors;

    batch_visitors.reserve(n_machines());
    for(uint machine_idx = 0; machine_idx < n_machines(); ++machine_idx) {
      batch_visitors.emplace_back(bcs.table_idx_vect[machine_idx],
                                  bcs.table_keys_vect[machine_idx],
                                  vv[machine_idx]);
    }

    return _batch_apply_step_3(batch_visitors, keys);
  }

  // Step 3: Remote call the batched functions.  Template switched on void/non-void return type.

  template <typename _KeyType, typename Visitor>
  typename BatchVisitor<Visitor>::BatchReturnType
  _batch_apply_step_3(const std::vector<BatchVisitor<Visitor> >& batch_visitors,
                      const std::vector<_KeyType>& keys,
                      typename std::enable_if<BatchVisitor<Visitor>::vector_return_value>::type* = 0) {

    typedef typename BatchVisitor<Visitor>::BatchReturnType VecReturnType;

    static_assert(std::is_same<VecReturnType, std::vector<typename VecReturnType::value_type> >::value,
                  "Bad value for batch return type.");

    std::vector<request_future<VecReturnType> > remotes;
    remotes.resize(n_machines());

    for(uint machine_idx = 0; machine_idx < n_machines(); ++machine_idx) {
      if(machine_idx != this_machine()) {
        remotes[machine_idx] = object_fiber_remote_request
            (rmi, machine_idx,
             &DHT_Base::template _batch_apply_local<BatchVisitor<Visitor> >,
             batch_visitors[machine_idx]);
      }
    }

    std::vector<VecReturnType> ret_vec_values(n_machines());

    // Apply the local one first, while waiting for the others.
    // Dunno, seems like a good idea.
    ret_vec_values[this_machine()] = _batch_apply_local(batch_visitors[this_machine()]);

    for(uint machine_idx = 0; machine_idx < n_machines(); ++machine_idx) {
      if(machine_idx != this_machine()) {
        ret_vec_values[this_machine()] = remotes[machine_idx]();
      }
    }

    // Now just need to go through and update all the values
    VecReturnType ret_v;
    ret_v.resize(keys.size());

    std::vector<size_t> indices(n_machines(), 0);

    for(size_t i = 0; i < keys.size(); ++i) {
      uint machine_idx = Policy::Resolver::getMachineIndex(rmi.dc(), keys[i]);
      ret_v[i] = ret_vec_values[machine_idx][indices[machine_idx]++];
    }

    return ret_v;
  }

  template <typename _KeyType, typename Visitor>
  void
  _batch_apply_step_3(const std::vector<BatchVisitor<Visitor> >& batch_visitors,
                      const std::vector<_KeyType>& keys,
                      typename std::enable_if<BatchVisitor<Visitor>::void_return_value>::type* = 0) {

    for(uint machine_idx = 0; machine_idx < n_machines(); ++machine_idx) {
      if(machine_idx != this_machine()) {
        rmi.remote_call(machine_idx,
                        &DHT_Base::template _batch_apply_local<BatchVisitor<Visitor> >,
                        batch_visitors[machine_idx]);
      }
    }

    _batch_apply_local(batch_visitors[this_machine()]);
  }

  ////////////////////////////////////////
  // Step 4:  Apply local, again switch on return type.  This is controlled here.

  template <typename BatchVisitor>
  typename BatchVisitor::BatchReturnType _batch_apply_local(BatchVisitor& visitor) {
    return _batch_apply_local_switch(visitor);
  }

  ////////////////////////////////////////
  // Step 5:  Actually Apply it.

  template <typename BatchVisitor>
  typename BatchVisitor::BatchReturnType _batch_apply_local_switch(
      const BatchVisitor& bv,
      typename std::enable_if<BatchVisitor::void_return_value>::type* = 0) {

    for(size_t i = 0; i < bv.table_indices.size(); ++i) {
      InternalTableIndexType idx = bv.table_indices[i];
      InternalContainerKeyType key = bv.table_keys[i];
      getTableContainer(idx).apply(this_ptr(), key, bv.get_visitor(i));
    }
  }

  template <typename BatchVisitor>
  typename BatchVisitor::BatchReturnType _batch_apply_local_switch(
      const BatchVisitor& bv,
      typename std::enable_if<BatchVisitor::vector_return_value>::type* = 0) {

    static_assert(!std::is_same<typename BatchVisitor::VisitorType::ReturnType, void>::value,
                  "ReturnType error.");

        static_assert(std::is_same<typename BatchVisitor::BatchReturnType,
                                   std::vector<typename BatchVisitor::VisitorReturnType> >::value,
                  "Bad value for batch return type.");

    std::vector<typename BatchVisitor::VisitorReturnType> ret_v;

    ret_v.reserve(bv.table_indices.size());

    for(size_t i = 0; i < bv.table_indices.size(); ++i) {
      InternalTableIndexType idx = bv.table_indices[i];
      InternalContainerKeyType key = bv.table_keys[i];
      InternalContainer& table = getTableContainer(idx);

      const typename BatchVisitor::VisitorType& visitor = bv.get_visitor(i);
      typename BatchVisitor::VisitorType::ReturnType r = table.apply(this_ptr(), key, visitor);

      ret_v.push_back(r);
    }

    return ret_v;
  }


  ////////////////////////////////////////////////////////////////////////////////
  // All the internal functions used by the child class

  void finalize_setup() {
    rmi.barrier();
  }

  bool _is_local(const KeyType& key) const {
    uint machine_idx = Policy::getMachineIndex(rmi.dc(), key);
    return machine_idx == this_machine();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Direct interface methods

};
}}

#endif /* _DHT_INTERNAL_H_ */
