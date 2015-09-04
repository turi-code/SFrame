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


#ifndef FUNCTION_CALL_ISSUE_HPP
#define FUNCTION_CALL_ISSUE_HPP
#include <iostream>
#include <serialization/serialization_includes.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/dc_send.hpp>
#include <rpc/function_call_dispatch.hpp>
#include <rpc/is_rpc_call.hpp>
#include <rpc/request_reply_handler.hpp>
#include <boost/preprocessor.hpp>
#include <rpc/dc_thread_get_send_buffer.hpp>
#include <rpc/dc_compile_parameters.hpp>
#include <rpc/function_arg_types_def.hpp>
#include <rpc/dc_registry.hpp>

namespace graphlab{
namespace dc_impl {

/**
\ingroup rpc
\internal
 * \file function_call_issue.hpp

This is an internal function and should not be used directly

A "call" is an RPC which is performed asynchronously.
There are 2 types of calls. A "basic" call calls a standard C/C++ function
and does not require the function to be modified.
while the "regular" call requires the first 2 arguments of the function
to be "distributed_control &dc, procid_t source".

An "issue" is a wrapper function on the sending side of an RPC
which encodes the packet and transmits it to the other side.
(I realized later this is called a "Marshaller")

Native Call Formats \n
=================== \n
The format of a "call" packet is in the form of an archive and is as follows

\li dispatch_id      -- index of target machine's dispatcher function
\li fn::arg1_type    -- target function's 1st argument
\li fn::arg2_type    -- target function's 2nd argument
\li  ...
\li fn::argN_type    -- target function's Nth argument

Argument casting is deferred to as late as possible. So the data type of
arguments are the data types that the caller use to call the function.
A dispatcher function will be instantiated with the input types, which will
then perform the type cast.

The code below generates the following for different number of arguments. Here, we
demonstrate the 1 argument version.
\code
namespace function_call_issue_detail { 
  template <typename BoolType, typename F, F remote_function , typename T0> 
  struct dispatch_selector1 { 
   static auto constexpr dispatchfn = dc_impl::NONINTRUSIVE_DISPATCH1<distributed_control,F, remote_function , T0 >;
  };

  template <typename F , F remote_function , typename T0> 
  struct dispatch_selector1<boost::mpl::bool_<true>, F, remote_function , T0>{ 
    static auto constexpr dispatchfn = dc_impl::DISPATCH1<distributed_control,F, remote_function , T0 >;
  };
} 
  template<typename F, F remote_function , typename T0> 
  class remote_call_issue1 { 
   public: 
    static void exec(dc_send* sender, unsigned char flags, procid_t target , const T0 &i0 ) { 
      oarchive* ptr = get_thread_local_buffer(target);
      oarchive& arc = *ptr;
      size_t len = dc_send::write_packet_header(arc, _get_procid(), flags, _get_sequentialization_key());
      uint32_t beginoff = arc.off;
      arc << dispatch_info.dispatch_id;
      arc << i0;
      *(reinterpret_cast<uint32_t*>(arc.buf + len)) = arc.off - beginoff;
      release_thread_local_buffer(target, flags & CONTROL_PACKET);
      if (flags & FLUSH_PACKET) pull_flush_soon_thread_local_buffer(target);
     } 
     struct _internal_struct_{ 
       dispatch_type dispatch_selector;
       function_dispatch_id_type dispatch_id;
       _internal_struct_() { 
         dispatch_selector = 
           function_call_issue_detail::dispatch_selector1<typename is_rpc_call<F>::type, F, remote_function , T0 >::dispatchfn;
         dispatch_id = 
           dc_impl::add_to_function_registry(reinterpret_cast<const void*>(&dispatch_selector), sizeof(dispatch_type));
       }
     };
   const static _internal_struct_ dispatch_info;
 };

 template<typename F, F remote_function , typename T0> 
 const typename remote_call_issue1<F, remote_function , T0>::_internal_struct_ 
 remote_call_issue1<F, remote_function , T0>::dispatch_info;
\endcode

The basic idea of the code is straightforward.  The receiving end cannot call
the target function (remote_function) directly, since it has no means of
understanding how to deserialize or to construct the stack for the
remote_function.  So instead, we generate a "dispatch" function on the
receiving side. The dispatch function is templated according to the type
information of the remote_function as well as the remote_function pointer, and
therefore knows how to deserialize the data, and issue the function call. That
is the "dispatch_type".

However, since we defined two families of receiving functions: a non-intrusive
version which does not take (dc, procid) as an argument and an intrusive
version which does, the dispatch function must therefore be slightly different
for each of them. That is what the dispatch_selector class performs.  The first
template argument of the dispatch_selector family of classes is a boolean flag
which denotes whether the function is a non-intrusive call or not. This boolean
flag itself is determined using the is_rpc_call<F>::type template.
*/



#define GENARGS(Z,N,_)  BOOST_PP_CAT(const T, N) BOOST_PP_CAT(&i, N)
#define GENI(Z,N,_) BOOST_PP_CAT(i, N)
#define GENT(Z,N,_) BOOST_PP_CAT(T, N)
#define GENARC(Z,N,_) arc << BOOST_PP_CAT(i, N);


/**
The dispatch_selectorN structs are used to pick between the standard dispatcher and the nonintrusive dispatch
by checking if the function is a RPC style call or not.
*/
#define REMOTE_CALL_ISSUE_GENERATOR(Z,N,FNAME_AND_CALL) \
namespace function_call_issue_detail {      \
template <typename BoolType, typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
struct BOOST_PP_CAT(dispatch_selector, N){  \
  static auto constexpr dispatchfn = BOOST_PP_CAT(dc_impl::NONINTRUSIVE_DISPATCH,N)<distributed_control,F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N, GENT ,_) >;  \
};\
template <typename F , F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
struct BOOST_PP_CAT(dispatch_selector, N)<boost::mpl::bool_<true>, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)>{  \
  static auto constexpr dispatchfn = BOOST_PP_CAT(dc_impl::DISPATCH,N)<distributed_control,F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N, GENT ,_) >; \
}; \
} \
template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
class  BOOST_PP_CAT(FNAME_AND_CALL, N) { \
  public: \
  static void exec(dc_send* sender, unsigned char flags, procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    oarchive* ptr = get_thread_local_buffer(target);  \
    oarchive& arc = *ptr;                         \
    size_t len = dc_send::write_packet_header(arc, _get_procid(), flags, _get_sequentialization_key()); \
    uint32_t beginoff = arc.off; \
    arc << dispatch_info.dispatch_id; \
    BOOST_PP_REPEAT(N, GENARC, _)                \
    *(reinterpret_cast<uint32_t*>(arc.buf + len)) = arc.off - beginoff; \
    release_thread_local_buffer(target, flags & CONTROL_PACKET); \
    if (flags & FLUSH_PACKET) pull_flush_soon_thread_local_buffer(target); \
  }\
  \
  struct _internal_struct_{ \
    typedef BOOST_PP_CAT(function_call_issue_detail::dispatch_selector,N)<typename is_rpc_call<F>::type, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T) > dispatch_selector_type; \
    dispatch_type dispatch_selector;   \
    function_dispatch_id_type dispatch_id; \
    _internal_struct_() { \
      dispatch_selector = dispatch_selector_type::dispatchfn;   \
      dispatch_id = dc_impl::add_to_function_registry(reinterpret_cast<const void*>(&dispatch_selector), sizeof(dispatch_type)); \
    } \
  }; \
  static const _internal_struct_ dispatch_info; \
}; \
\
template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
const typename BOOST_PP_CAT(FNAME_AND_CALL, N)<F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)>::_internal_struct_ BOOST_PP_CAT(FNAME_AND_CALL, N)<F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)>::dispatch_info; \



/**
Generates a function call issue. 3rd argument is the issue name
*/
BOOST_PP_REPEAT(6, REMOTE_CALL_ISSUE_GENERATOR,  remote_call_issue )



#undef GENARC
#undef GENT
#undef GENI
#undef GENARGS
#undef REMOTE_CALL_ISSUE_GENERATOR

} // namespace dc_impl
} // namespace graphlab

#include <rpc/function_arg_types_undef.hpp>

#endif

