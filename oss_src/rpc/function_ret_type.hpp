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


#ifndef FUNCTION_RETURN_TYPE_HPP
#define FUNCTION_RETURN_TYPE_HPP
#include <boost/preprocessor.hpp>
#include <rpc/function_arg_types_def.hpp>
namespace graphlab {
namespace dc_impl {

  
/**
\ingroup rpc
\internal

This struct performs two duties.
Firstly, it provides a consistent interface through a function called ::fcallN<F>
to complete a function call with a variable number of arguments.
Next, it provides the type of the return value of the function in ::type.
If the return type is void, it is promoted to an int. This makes the output
type of the function call be always serializable, simplifying the implementation
of "requests".
*/
template <typename RetType>
struct function_ret_type {
  typedef RetType type;
  
  #define GENARGS(Z,N,_)  BOOST_PP_CAT(__GLRPC_R, N)  BOOST_PP_CAT(i, N)
 
  #define FCALL(Z, N, _) \
  template <typename F> \
  static RetType BOOST_PP_CAT(fcall, N)(F f BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N, GENARGS, _)){ \
    return f(BOOST_PP_ENUM_PARAMS(N, i)); \
  } 
    
  BOOST_PP_REPEAT(8, FCALL ,  _ )

  #undef FCALL
  #undef GENARGS

};

template <>
struct function_ret_type<void> {
  typedef size_t type;
  
  #define GENARGS(Z,N,_)  BOOST_PP_CAT(__GLRPC_R, N) BOOST_PP_CAT(i, N)
 
  #define FCALL(Z, N, _) \
  template <typename F> \
  static size_t BOOST_PP_CAT(fcall, N)(F f BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N, GENARGS, _)){ \
    f(BOOST_PP_ENUM_PARAMS(N, i)); \
    return 0; \
  } 
  
  BOOST_PP_REPEAT(8, FCALL ,  _ )

  #undef FCALL
  #undef GENARGS

};
#include <rpc/function_arg_types_undef.hpp>


} // namespace dc_impl
} // namespace graphlab

#include <rpc/mem_function_arg_types_def.hpp>

namespace graphlab {
namespace dc_impl {

/**
This struct performs two duties.
Firstly, it provides a consistent interface through a function called ::fcallN<F>
to complete a \b member function call with a variable number of arguments.
Next, it provides the type of the return value of the function in ::type.
If the return type is void, it is promoted to an int. This makes the output
type of the function call be always serializable, simplifying the implementation
of "requests".
*/
template <typename RetType>
struct mem_function_ret_type {
  typedef RetType type;
  
  #define GENARGS(Z,N,_)  BOOST_PP_CAT(__GLRPC_R, N)  BOOST_PP_CAT(i, N)
 
  #define FCALL(Z, N, _) \
  template <typename F, typename T> \
  static RetType BOOST_PP_CAT(fcall, N)(F f , T t BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N, GENARGS, _)){ \
    return (t->*f)(BOOST_PP_ENUM_PARAMS(N, i)); \
  }

  BOOST_PP_REPEAT(8, FCALL ,  _ )

  #undef FCALL
  #undef GENARGS

};

template <>
struct mem_function_ret_type<void> {
  typedef size_t type;
  
  #define GENARGS(Z,N,_)  BOOST_PP_CAT(__GLRPC_R, N) BOOST_PP_CAT(i, N)
 
  #define FCALL(Z, N, _) \
  template <typename F, typename T> \
  static size_t BOOST_PP_CAT(fcall, N)(F f , T t BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N, GENARGS, _)){ \
     (t->*f)(BOOST_PP_ENUM_PARAMS(N, i)); \
     return 0; \
  }

  BOOST_PP_REPEAT(8, FCALL ,  _ )

  #undef FCALL
  #undef GENARGS

};




} // namespace dc_impl
} // namespace graphlab

#include <rpc/mem_function_arg_types_undef.hpp>

#endif

