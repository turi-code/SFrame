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


#ifndef REPACK_DISPATCH_HPP
#define REPACK_DISPATCH_HPP
#include <iostream>
#include <serialization/serialization_includes.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/function_arg_types_def.hpp>
#include <boost/preprocessor.hpp>
namespace graphlab {
namespace dc_impl {

/**
\ingroup rpc
\internal
\file function_call_dispatch.hpp

This is an internal function and should not be used directly

A "call" is an RPC which is performed asynchronously.
There are 2 types of calls. A "basic" call calls a standard C/C++ function
and does not require the function to be modified.
while the "regular" call requires the first 2 arguments of the function 
to be "distributed_control &dc, procid_t source".

A "dispatch" is a wrapper function on the receiving side of an RPC
which decodes the packet and performs the function call.

This scary looking piece of code is actually quite straightforward.
Given function of type F, and pointer remote_function, as well as input types T1 ... Tn
it will construct an input archive and deserialize the types T1.... Tn,
and call the function remote_function with it. This code dispatches to the "intrusive" 
form of a function call (that is the function call must take a distributed_control
and a "procid_t source" as its first 2 arguments.

For instance, the 1 argument version of this is DISPATCH1:

\code
template<typename DcType, typename F, F remote_function, typename T0> 
void DISPATCH1 (DcType& dc, procid_t source, unsigned char packet_type_mask, 
                const char* buf, size_t len) { 
  iarchive iarc(buf, len);
  size_t s;
  iarc >> s;
  T0 (f0) ;
  iarc >> (f0) ;
  remote_function(dc, source , (f0) );
  charstring_free(f0);
} 
\endcode

charstring_free is a special template function which calls free(f1)
only if f1 is a character array (char*)

And similarly, the non-intrusive dispatch a little below

Note that the template around DcType is *deliberate*. This prevents this
function from instantiating the distributed_control until as late as possible, 
avoiding problems with circular references.
*/

#define GENFN(N) BOOST_PP_CAT(__GLRPC_F, N)
#define GENFN2(N) BOOST_PP_CAT(f, N)
#define GENARGS(Z,N,_)  (BOOST_PP_CAT(f, N))
#define GENPARAMS(Z,N,_)  BOOST_PP_CAT(T, N) (BOOST_PP_CAT(f, N)) ; iarc >> (BOOST_PP_CAT(f, N)) ;
#define CHARSTRINGFREE(Z,N,_)  charstring_free(BOOST_PP_CAT(f, N));

#define DISPATCH_GENERATOR(Z,N,_) \
template<typename DcType, typename F, F exec_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
void BOOST_PP_CAT(DISPATCH,N) (DcType& dc, procid_t source, unsigned char packet_type_mask, \
               const char* buf, size_t len) { \
  iarchive iarc(buf, len); \
  BOOST_PP_REPEAT(N, GENPARAMS, _)                \
  exec_function(dc, source BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_)  ); \
  BOOST_PP_REPEAT(N, CHARSTRINGFREE, _)                \
} 

BOOST_PP_REPEAT(6, DISPATCH_GENERATOR, _)

#undef GENFN
#undef GENFN2
#undef GENARGS
#undef GENPARAMS
#undef DISPATCH_GENERATOR



/**
This is similar, but generates the non-intrusive version of a 
dispatcher. That is, the target function does not need to take
"distributed_control &dc, procid_t source" as its first 2 arguments.

template<typename DcType, typename F , typename T0> 
void NONINTRUSIVE_DISPATCH1(DcType& dc, procid_t source, 
                            unsigned char packet_type_mask, 
                            const char* buf, size_t len) { 
  iarchive iarc(buf, len);
  size_t s;
  iarc >> s;
  F f = reinterpret_cast<F>(s);
  T0 (f0) ;
  iarc >> (f0) ;
  f( (f0) );
  charstring_free(f0);
}
*/



#define GENFN(N) BOOST_PP_CAT(__GLRPC_NIF, N)
#define GENFN2(N) BOOST_PP_CAT(f, N)
#define GENARGS(Z,N,_) (BOOST_PP_CAT(f, N))
#define GENPARAMS(Z,N,_)  BOOST_PP_CAT(T, N) (BOOST_PP_CAT(f, N)) ; iarc >> (BOOST_PP_CAT(f, N)) ;
#define CHARSTRINGFREE(Z,N,_)  charstring_free(BOOST_PP_CAT(f, N));


#define NONINTRUSIVE_DISPATCH_GENERATOR(Z,N,_) \
template<typename DcType, typename F , F exec_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
void BOOST_PP_CAT(NONINTRUSIVE_DISPATCH,N) (DcType& dc, procid_t source, unsigned char packet_type_mask,  \
               const char* buf, size_t len) { \
  iarchive iarc(buf, len); \
  BOOST_PP_REPEAT(N, GENPARAMS, _)                \
  exec_function(BOOST_PP_ENUM(N,GENARGS ,_)  ); \
  BOOST_PP_REPEAT(N, CHARSTRINGFREE, _)                \
} 

BOOST_PP_REPEAT(6, NONINTRUSIVE_DISPATCH_GENERATOR, _)

#undef GENFN
#undef GENFN2
#undef GENARGS
#undef GENPARAMS
#undef NONINTRUSIVE_DISPATCH_GENERATOR

} // namespace dc_impl
} // namespace graphlab


#include <rpc/function_arg_types_undef.hpp>
#endif

