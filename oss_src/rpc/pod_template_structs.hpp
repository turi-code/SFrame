/**
 * Copyright (C) 2016 Turi
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


#ifndef POD_TEMPLATE_STRUCTS_HPP
#define POD_TEMPLATE_STRUCTS_HPP
#include <serialization/serialization_includes.hpp>
#include <serialization/is_pod.hpp>

namespace graphlab {
namespace dc_impl {
namespace pod_template_detail {

template <typename F>
struct pod_call_struct0 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
};


template <typename F, typename T0>
struct pod_call_struct1 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0;
};


template <typename F, typename T0, typename T1>
struct pod_call_struct2 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0; T1 t1;
};

template <typename F, typename T0, typename T1, typename T2>
struct pod_call_struct3 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0; T1 t1; T2 t2;
};


template <typename F, typename T0, typename T1, typename T2,
          typename T3>
struct pod_call_struct4 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0; T1 t1; T2 t2; T3 t3;
};


template <typename F, typename T0, typename T1, typename T2,
          typename T3, typename T4>
struct pod_call_struct5 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0; T1 t1; T2 t2; T3 t3; T4 t4;
};


template <typename F, typename T0, typename T1, typename T2,
          typename T3, typename T4, typename T5>
struct pod_call_struct6 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0; T1 t1; T2 t2; T3 t3; T4 t4; T5 t5;
};


template <typename F, typename T0, typename T1, typename T2,
          typename T3, typename T4, typename T5,
          typename T6>
struct pod_call_struct7 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0; T1 t1; T2 t2; T3 t3; T4 t4; T5 t5; T6 t6;
};


template <typename F, typename T0, typename T1, typename T2,
          typename T3, typename T4, typename T5,
          typename T6, typename T7>
struct pod_call_struct8 : public IS_POD_TYPE{
  size_t dispatch_function;
  size_t objid;
  F remote_function;
  T0 t0; T1 t1; T2 t2; T3 t3; T4 t4; T5 t5; T6 t6; T7 t7;
};

}
}
}

#endif
