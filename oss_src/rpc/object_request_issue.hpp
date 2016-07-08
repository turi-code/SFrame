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


#ifndef OBJECT_REQUEST_ISSUE_HPP
#define OBJECT_REQUEST_ISSUE_HPP
#include <sstream>
#include <iostream>
#include <string>
#include <serialization/serialization_includes.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/request_reply_handler.hpp>
#include <rpc/object_request_dispatch.hpp>
#include <rpc/function_ret_type.hpp>
#include <rpc/mem_function_arg_types_def.hpp>
#include <rpc/request_future.hpp>
#include <rpc/dc_thread_get_send_buffer.hpp>
#include <rpc/dc_compile_parameters.hpp>
#include <rpc/dc_registry.hpp>
#include <boost/preprocessor.hpp>

namespace graphlab {
namespace dc_impl {


#define GENARGS(Z,N,_)  BOOST_PP_CAT(const T, N) BOOST_PP_CAT(&i, N)
#define GENT(Z,N,_) BOOST_PP_CAT(T, N)
#define GENARC(Z,N,_) arc << BOOST_PP_CAT(i, N);


/**
\internal
\ingroup rpc
\file object_request_issue.hpp


This is an internal function and should not be used directly

This is the marshall function for the an object member function call.
This is very similar to the standard function request issue in request_issue.hpp
, with the only difference that an object id has to be transmitted

\code
template < typename T, typename F, typename T0 > 
class object_request_issue1 {
 public:
  static void exec (dc_dist_object_base * rmi, dc_send * sender,
                    size_t request_handle, unsigned char flags,
                    procid_t target, size_t objid, F remote_function,
                    const T0 & i0) {
    oarchive *ptr = get_thread_local_buffer (target);
    oarchive & arc = *ptr;
    size_t len =
      dc_send::write_packet_header (arc, _get_procid (), flags,
				    _get_sequentialization_key ());
    uint32_t beginoff = arc.off;
    arc << dispatch_info.dispatch_id; 
    arc << objid;
    arc << request_handle;
    arc << i0;
    uint32_t curlen = arc.off - beginoff;
    *(reinterpret_cast < uint32_t * >(arc.buf + len)) = curlen;
    release_thread_local_buffer (target, flags & CONTROL_PACKET);
    if ((flags & CONTROL_PACKET) == 0)
      rmi->inc_bytes_sent (target, curlen);
    pull_flush_thread_local_buffer (target);
  }

  struct _internal_struct_{ 
    dispatch_type dispatch_selector;
    function_dispatch_id_type dispatch_id;
    _internal_struct_() { 
      dispatch_selector = 
      dc_impl::OBJECT_NONINTRUSIVE_REQUESTDISPATCH1 < distributed_control, T, F, remote_function, T0 >::dispatchfn;
      dispatch_id = 
      dc_impl::add_to_function_registry(reinterpret_cast<const void*>(&dispatch_selector), sizeof(dispatch_type));
    }
  };
 const static _internal_struct_ dispatch_info;
};

template<typename F, F remote_function , typename T0> 
const typename object_request_issue1<F, remote_function , T0>::_internal_struct_ 
object_request_issue1<F, remote_function , T0>::dispatch_info;
\endcode


*/
#define REMOTE_REQUEST_ISSUE_GENERATOR(Z,N,FNAME_AND_CALL) \
template<typename T,typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
class  BOOST_PP_CAT(FNAME_AND_CALL, N) { \
  public: \
  static void exec(dc_dist_object_base* rmi, dc_send* sender, size_t request_handle, unsigned char flags, procid_t target,size_t objid BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    oarchive* ptr = get_thread_local_buffer(target);  \
    oarchive& arc = *ptr;                         \
    size_t len = dc_send::write_packet_header(arc, _get_procid(), flags, _get_sequentialization_key()); \
    uint32_t beginoff = arc.off; \
    arc << dispatch_info.dispatch_id; \
    arc << objid;       \
    arc << request_handle; \
    BOOST_PP_REPEAT(N, GENARC, _)                \
    uint32_t curlen = arc.off - beginoff;   \
    *(reinterpret_cast<uint32_t*>(arc.buf + len)) = curlen; \
    release_thread_local_buffer(target, flags & CONTROL_PACKET); \
    if ((flags & CONTROL_PACKET) == 0)                       \
      rmi->inc_bytes_sent(target, curlen);           \
    if (flags & FLUSH_PACKET) pull_flush_soon_thread_local_buffer(target); \
  }\
  struct _internal_struct_{ \
    dispatch_type dispatch_selector;  \
    function_dispatch_id_type dispatch_id; \
    _internal_struct_() { \
      dispatch_selector = BOOST_PP_CAT(dc_impl::OBJECT_NONINTRUSIVE_REQUESTDISPATCH,N)<distributed_control,T,F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N, GENT ,_) >;  \
      dispatch_id = dc_impl::add_to_function_registry(reinterpret_cast<const void*>(&dispatch_selector), sizeof(dispatch_type)); \
    } \
  }; \
  static const _internal_struct_ dispatch_info; \
}; \
template<typename T, typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
const typename BOOST_PP_CAT(FNAME_AND_CALL, N)<T, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)>::_internal_struct_ BOOST_PP_CAT(FNAME_AND_CALL, N)<T, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)>::dispatch_info; 

BOOST_PP_REPEAT(6, REMOTE_REQUEST_ISSUE_GENERATOR,  object_request_issue )



#undef GENARC
#undef GENT
#undef GENARGS
#undef REMOTE_REQUEST_ISSUE_GENERATOR


} // namespace dc_impl
} // namespace graphlab
#include <rpc/mem_function_arg_types_undef.hpp>

#endif

