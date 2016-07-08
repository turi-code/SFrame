/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_DHT_ACCESSOR_PATTERNS_H_
#define GRAPHLAB_DHT_ACCESSOR_PATTERNS_H_

#include <serialization/oarchive.hpp>
#include <serialization/iarchive.hpp>

#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <parallel/atomic.hpp>

namespace graphlab {
namespace _DHT {

#define _ENABLE_IF_RETURNS_VOID(Visitor)                                       \
typename std::enable_if<std::is_same<typename Visitor::ReturnType, void>::value>::type* = 0

#define _ENABLE_IF_RETURNS_VALUE(Visitor)                                       \
typename std::enable_if<!std::is_same<typename Visitor::ReturnType, void>::value>::type* = 0

////////////////////////////////////////////////////////////////////////////////
// Get accessor with things

template <typename DHTClass, typename _ValueType>
class GetVisitor {
 public:
  typedef _ValueType Value;
  typedef _ValueType ReturnType;
  typedef typename DHTClass::InternalContainerKeyType LocalKeyType;

  template <typename Table>
  ReturnType operator()(DHTClass* dht, Table& table, LocalKeyType key) const {
    auto it = table.find(key);

    if(it == table.end()) {
      return Value();
    } else {
      return it->second;
    }
  }

  void save(oarchive &oarc) const {}
  void load(iarchive &iarc) {}
};

////////////////////////////////////////////////////////////////////////////////
// Set accessor with things

template <typename DHTClass, typename _Value>
class SetVisitor {
 public:
  typedef void ReturnType;
  typedef _Value Value;
  typedef typename DHTClass::InternalContainerKeyType LocalKeyType;

  SetVisitor(_Value value = 0) {
    _value = value;
  }

  template <typename Table>
  void operator()(DHTClass*, Table& table, LocalKeyType key) {
    table[key] = _value;
  }

  void save(oarchive &oarc) const {
    oarc << _value;
  }

  void load(iarchive &iarc) {
    iarc >> _value;
  }

 private:
  Value _value;
};

////////////////////////////////////////////////////////////////////////////////
// Set accessor with things

template <typename DHTClass, typename _Value>
class ApplyDeltaVisitorWithReturn {
 public:
  typedef _Value ReturnType;
  typedef _Value Value;
  typedef typename DHTClass::InternalContainerKeyType LocalKeyType;

  ApplyDeltaVisitorWithReturn(Value delta=0) {
    _delta = delta;
  }

  template <typename Table>
  ReturnType operator()(DHTClass*, Table& table, LocalKeyType key) {
    return (table[key] += _delta);
  }

  void save(oarchive &oarc) const {
    oarc << _delta;
  }

  void load(iarchive &iarc) {
    iarc >> _delta;
  }

 private:
  Value _delta;
};

////////////////////////////////////////////////////////////////////////////////
// Set accessor with things

template <typename DHTClass, typename _Value>
class ApplyDeltaVisitor {
 public:
  typedef void ReturnType;
  typedef _Value Value;
  typedef typename DHTClass::InternalContainerKeyType LocalKeyType;

  ApplyDeltaVisitor(Value delta=0) {
    _delta = delta;
  }

  template <typename Table>
  void operator()(DHTClass*, Table& table, LocalKeyType key) {
    (table[key] += _delta);
  }

  void save(oarchive &oarc) const {
    oarc << _delta;
  }

  void load(iarchive &iarc) {
    iarc >> _delta;
  }

 private:
  Value _delta;
};




}
}

#endif /* _ACCESSOR_PATTERNS_H_ */
