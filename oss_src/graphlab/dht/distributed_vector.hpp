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


#ifndef GRAPHLAB_DISTRIBUTED_VECTOR_H_
#define GRAPHLAB_DISTRIBUTED_VECTOR_H_

#include <unordered_map>
#include <vector>
#include <algorithm>
#include <parallel/pthread_tools.hpp>
#include <rpc/dc_dist_object.hpp>
#include <graphlab/dht/scalar_dht.hpp>
#include <graphlab/util/token.hpp>

#include <iostream>

namespace graphlab {

namespace _DHT {

// TODO: optimize -- Much room for improvement here.
template <typename _ValueType>
struct DistributedVectorPolicy {

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
 * This defines a distributed vector of scalar values for machine
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
 * Currently, this version simply wraps the scalar_dht; however, this
 * will diverge in the future as many optimizations are possible to
 * take advantage of the ordering of the keys.  The methods available,
 * however, will remain the same.
 *
 * \tparam T The data type.
 */
template <typename ValueType>
using distributed_vector = scalar_dht<ValueType, _DHT::DistributedVectorPolicy>;
}

#endif
