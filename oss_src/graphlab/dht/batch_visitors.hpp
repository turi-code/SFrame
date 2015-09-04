/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_DHT_BATCH_VISITORS_H_
#define GRAPHLAB_DHT_BATCH_VISITORS_H_

#include <serialization/oarchive.hpp>
#include <serialization/iarchive.hpp>
#include <vector>

namespace graphlab {
namespace _DHT {

////////////////////////////////////////////////////////////////////////////////
// Name user

template <typename T>
struct _BatchVisitorReturnTypeSwitch {
  typedef std::vector<T> ReturnType;
};

template <>
struct _BatchVisitorReturnTypeSwitch<void> {
  typedef void ReturnType;
};

////////////////////////////////////////////////////////////////////////////////
// Visitor container type switches.  This way, we can push through
// either a vector of visitors or a single visitor as the

template <typename T>
struct _VisitorContainerInformation {
  typedef typename T::ReturnType VisitorReturnType;
  typedef T VisitorType;
  static constexpr bool is_vector = false;
};

template <typename V>
struct _VisitorContainerInformation<std::vector<V> >{
  typedef typename V::ReturnType VisitorReturnType;
  typedef V VisitorType;
  static constexpr bool is_vector = true;
};

////////////////////////////////////////////////////////////////////////////////
// A batch container for the visitors.  It can be either a single
// visitor, or a vector of visitors.
template <typename DHTClass, typename _VisitorContainer>
class BatchVisitor {
 public:

  typedef _VisitorContainer VisitorContainer;

  static constexpr bool vector_of_visitors = _VisitorContainerInformation<VisitorContainer>::is_vector;
  static constexpr bool single_visitor     = !vector_of_visitors;

  typedef typename DHTClass::InternalTableIndexType                                  InternalTableIndexType;
  typedef typename DHTClass::InternalContainerKeyType                                InternalContainerKeyType;
  typedef typename DHTClass::InternalContainer                                       InternalContainer;

  typedef typename _VisitorContainerInformation<VisitorContainer>::VisitorType       VisitorType;
  typedef typename _VisitorContainerInformation<VisitorContainer>::VisitorReturnType VisitorReturnType;

  // The return type of the visitor is switched on the
  static constexpr bool void_return_value   =  std::is_same<VisitorReturnType, void>::value;
  static constexpr bool vector_return_value = !std::is_same<VisitorReturnType, void>::value;

  typedef typename _BatchVisitorReturnTypeSwitch<VisitorReturnType>::ReturnType      BatchReturnType;

 public:

  /////////////////////////////////////////////////////////////// /////////////////
  // Switched on whether it is instanciated locally or through the
  // serialization mechanism.
  BatchVisitor()
      : table_indices(_table_indices_internal)
      , table_keys(_table_keys_internal)
      , visitors(_visitors_internal)
  {}

  BatchVisitor(const std::vector<InternalTableIndexType>& _table_indices,
               const std::vector<InternalContainerKeyType>& _table_keys,
               const VisitorContainer& _visitors)
      : _table_indices_internal(_table_indices)
      , _table_keys_internal(_table_keys)
      , _visitors_internal(_visitors)
      , table_indices(_table_indices_internal)
      , table_keys(_table_keys_internal)
      , visitors(_visitors_internal)
      // : table_indices(_table_indices)
      // , table_keys(_table_keys)
      // , visitors(_visitors)
  {
    DASSERT_EQ(table_indices.size(), table_keys.size());
  }

 private:

  // These are used for the local initialization part
  std::vector<InternalTableIndexType> _table_indices_internal;
  std::vector<InternalContainerKeyType> _table_keys_internal;
  VisitorContainer _visitors_internal;

 public:

  // How we can access the world
  const std::vector<InternalTableIndexType>& table_indices;
  const std::vector<InternalContainerKeyType>& table_keys;

  const VisitorType& get_visitor(size_t idx) const {
    return _get_visitor<BatchVisitor>(idx);
  }

 private:

  const VisitorContainer& visitors;

  template <typename DummyThis>
  const VisitorType& _get_visitor(
      size_t idx, typename std::enable_if<DummyThis::vector_of_visitors>::type* = 0) const {
    DASSERT_LT(idx, visitors.size());
    return visitors[idx];
  }

  template <typename DummyThis>
  const VisitorType& _get_visitor(
      size_t, typename std::enable_if<!DummyThis::vector_of_visitors>::type* = 0) const {

    return visitors;
  }

  /////////////////////////////////////////////////////////////////////////////////
  // Serialization routines.

 public:
  void save(oarchive &oarc) const {
    oarc << table_indices << table_keys << visitors;
  }

  void load(iarchive &iarc) {
    iarc >> _table_indices_internal >> _table_keys_internal >> _visitors_internal;
  }




};

// A utility class for some of the above bookkeeping





}}







#endif /* _BATCH_VISITORS_H_ */
