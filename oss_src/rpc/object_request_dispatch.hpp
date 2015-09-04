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


#ifndef OBJECT_REQUEST_DISPATCH_HPP
#define OBJECT_REQUEST_DISPATCH_HPP
#include <sstream>
#include <iostream>
#include <string>
#include <functional>
#include <algorithm>
#include <serialization/serialization_includes.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/request_reply_handler.hpp>
#include <rpc/function_ret_type.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <rpc/mem_function_arg_types_def.hpp>
#include <rpc/dc_macros.hpp>
#include <boost/preprocessor.hpp>

namespace graphlab {
namespace dc_impl{


/**
\ingroup rpc
\internal
\file object_request_dispatch.hpp

This is an internal function and should not be used directly

This is the dispatch function for the an object request.
This is similar to the standard request dispatcher in request_dispatch.hpp
except that the object needs to be located using the object id.
After the function call, it also needs to increment the call count for
the object context.

\code
template < typename DcType, typename T, typename F, typename T0 > 
void OBJECT_NONINTRUSIVE_REQUESTDISPATCH1 (DcType & dc, procid_t source,
                                           unsigned char packet_type_mask,
                                           const char *buf, size_t len) {
  iarchive iarc (buf, len);
  F f;
  deserialize (iarc, (char *) (&f), sizeof (F));
  size_t objid;
  iarc >> objid;
  T *obj = reinterpret_cast < T * >(dc.get_registered_object (objid));
  size_t id;
  iarc >> id;
  T0 (f0);
  iarc >> (f0);
  typename function_ret_type < 
    typename boost::remove_const < 
    typename boost::remove_reference < 
    typename boost::function < 
    typename boost::remove_member_pointer < F >::type >::result_type >::type >::type >::type 
        ret = mem_function_ret_type < 
                typename boost::remove_const <
                typename boost::remove_reference < 
                typename boost::function < 
                typename boost::remove_member_pointer < F >::type >::result_type >::type >::type >::fcall1 (f, obj, (f0));

  charstring_free (f0);
  boost::iostreams::stream < resizing_array_sink > retstrm (128);
  oarchive oarc (retstrm);
  oarc << ret;
  retstrm.flush ();
  if ((packet_type_mask & CONTROL_PACKET) == 0) {
    dc.get_rmi_instance (objid)->inc_calls_received (source);
    dc.get_rmi_instance (objid)->inc_bytes_sent (source, retstrm->len);
  }
  if (packet_type_mask & CONTROL_PACKET) {
    dc.control_call (source, request_reply_handler, id,
		     blob (retstrm->str, retstrm->len));
  }
  else {
    dc.reply_remote_call (source, request_reply_handler, id,
			  blob (retstrm->str, retstrm->len));
  }
  free (retstrm->str);
}
\endcode


*/
#define GENFN(N) BOOST_PP_CAT(__GLRPC_NIF, N)
#define GENFN2(N) BOOST_PP_CAT(f, N)
#define GENNIARGS(Z,N,_) (BOOST_PP_CAT(f, N))

#define GENPARAMS(Z,N,_)                                                \
  BOOST_PP_CAT(T, N) (BOOST_PP_CAT(f, N)) ;                             \
  iarc >> (BOOST_PP_CAT(f, N)) ;

#define CHARSTRINGFREE(Z,N,_)  charstring_free(BOOST_PP_CAT(f, N));

#define NONINTRUSIVE_DISPATCH_GENERATOR(Z,N,_)                          \
  template<typename DcType, typename T,                                 \
           typename F, F remote_function BOOST_PP_COMMA_IF(N)                             \
           BOOST_PP_ENUM_PARAMS(N, typename T) >                        \
  void BOOST_PP_CAT(OBJECT_NONINTRUSIVE_REQUESTDISPATCH,N) (DcType& dc, \
                                                            procid_t source, \
                                                            unsigned char packet_type_mask, \
                                                            const char* buf, size_t len) { \
    iarchive iarc(buf, len);                                                \
    size_t objid;                                                       \
    iarc >> objid;                                                      \
    T* obj = reinterpret_cast<T*>(dc.get_registered_object(objid));     \
    size_t id; iarc >> id;                                              \
    BOOST_PP_REPEAT(N, GENPARAMS, _);                                   \
    typename function_ret_type<__GLRPC_FRESULT>::type ret =                     \
      mem_function_ret_type<__GLRPC_FRESULT>::BOOST_PP_CAT(fcall, N)            \
      (remote_function, obj BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENNIARGS ,_));      \
    BOOST_PP_REPEAT(N, CHARSTRINGFREE, _);                              \
    boost::iostreams::stream<resizing_array_sink> retstrm(128);         \
    oarchive oarc(retstrm);                                             \
    oarc << ret;                                                        \
    retstrm.flush();                                                    \
    if ((packet_type_mask & CONTROL_PACKET) == 0) {                     \
      dc.get_rmi_instance(objid)->inc_calls_received(source);           \
      dc.get_rmi_instance(objid)->inc_bytes_sent(source, retstrm->len); \
    }                                                                   \
    /*std::cerr << "Request wait on " << id << std::endl ; */           \
    if (packet_type_mask & CONTROL_PACKET) {                            \
      dc.RPC_CALL(control_call, request_reply_handler) (source,         \
                      id,                                               \
                      blob(retstrm->str, retstrm->len));                \
    } else if(packet_type_mask & FLUSH_PACKET) {                        \
      dc.RPC_CALL(reply_remote_call, request_reply_handler)(source,     \
                      id,                                               \
                      blob(retstrm->str, retstrm->len));                \
    }  else {                                                            \
      dc.RPC_CALL(remote_call,request_reply_handler) (source,       \
                     id,                                           \
                     blob(retstrm->str, retstrm->len));            \
    }                                                                   \
    free(retstrm->str);                                                 \
    /* std::cerr << "Request received on " << id << std::endl ; */      \
  } 



BOOST_PP_REPEAT(6, NONINTRUSIVE_DISPATCH_GENERATOR, _)


#undef GENFN
#undef GENFN2
#undef GENNIARGS
#undef GENPARAMS
#undef NONINTRUSIVE_DISPATCH_GENERATOR

} // namespace dc_impl
} // namespace graphlab
#include <rpc/mem_function_arg_types_undef.hpp>
#endif

