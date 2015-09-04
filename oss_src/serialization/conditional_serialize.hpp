/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

/*  
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


#ifndef GRAPHLAB_SERIALIZATION_CONDITIONAL_SERIALIZE_HPP
#define GRAPHLAB_SERIALIZATION_CONDITIONAL_SERIALIZE_HPP
#include <serialization/oarchive.hpp>
#include <serialization/iarchive.hpp>
namespace graphlab {

template <typename T>
struct conditional_serialize {
  bool hasval;
  T val;

  conditional_serialize(): hasval(false) { }
  conditional_serialize(T& val): hasval(true), val(val) { }

  conditional_serialize(const conditional_serialize& cs): hasval(cs.hasval), val(cs.val) { }
  conditional_serialize& operator=(const conditional_serialize& cs) {
    hasval = cs.hasval;
    val = cs.val;
    return (*this);
  }
  void save(oarchive& oarc) const {
    oarc << hasval;
    if (hasval) oarc << val;
  }

  void load(iarchive& iarc) {
    iarc >> hasval;
    if (hasval) iarc >> val;
  }
};

};

#endif
