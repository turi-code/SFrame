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


#ifndef REQUEST_DISPATCH_HPP
#define REQUEST_DISPATCH_HPP
#include <sstream>
#include <iostream>
#include <string>
#include <serialization/serialization_includes.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/request_reply_handler.hpp>
#include <rpc/function_ret_type.hpp>
#include <rpc/function_arg_types_def.hpp>
#include <rpc/dc_macros.hpp>
#include <boost/preprocessor.hpp>

namespace graphlab {
namespace dc_impl{

/**
\internal
\ingroup rpc
\file request_dispatch.hpp

This is an internal function and should not be used directly.

Given  function F, as well as input types T1 ... Tn
it will construct an input archive and deserialize the types T1.... Tn,
and call the function f with it. The return value of the function
is then returned to the caller through the reply call to the 
source's request_reply_handler. This code dispatches to the "intrusive" 
form of a function call (that is the function call must take a distributed_control
and a "procid_t source" as its first 2 arguments.

For instance, the 1 argument of this will be DISPATCH1:
\code
template < typename DcType, typename F, F remote_function, typename T0 > 
void REQUESTDISPATCH1 (DcType & dc, procid_t source,
                       unsigned char packet_type_mask, 
                       const char *buf, size_t len) {
  iarchive iarc (buf, len);
  size_t s;
  iarc >> s;
  size_t id;
  iarc >> id;
  T0 (f0);
  iarc >> (f0);
  typename function_ret_type < 
    typename boost::remove_const <
    typename boost::remove_reference < 
    typename boost::function <
    typename boost::remove_pointer <
    F >::type >::result_type >::type >::type >::type ret =
          function_ret_type < 
            typename boost::remove_const <
            typename boost::remove_reference < 
            typename boost::function <
            typename boost::remove_pointer <F>::type >::result_type >::type >::type >::fcall3 (remote_function, dc, source, (f0));
  charstring_free (f0);
  boost::iostreams::stream < resizing_array_sink > retstrm (128);
  oarchive oarc (retstrm);
  oarc << ret;
  retstrm.flush ();
  if (packet_type_mask & CONTROL_PACKET) {
    dc.template control_call<decltype(request_reply_handler), request_reply_handler>
      (source, id, blob (retstrm->str, retstrm->len));
  }
  else {
    dc.template reply_remote_call<decltype(request_reply_handler), request_reply_handler>
      (source, id, blob (retstrm->str, retstrm->len));
  }
  free (retstrm->str);
}
\endcode

charstring_free is a special template function which calls free(f1)
only if f1 is a character array (char*)

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
template<typename DcType, typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
void BOOST_PP_CAT(REQUESTDISPATCH,N) (DcType& dc, procid_t source, unsigned char packet_type_mask, \
               const char* buf, size_t len) { \
  iarchive iarc(buf, len); \
  size_t id; iarc >> id;    \
  BOOST_PP_REPEAT(N, GENPARAMS, _)                \
  typename function_ret_type<__GLRPC_FRESULT>::type ret = function_ret_type<__GLRPC_FRESULT>::BOOST_PP_CAT(fcall, BOOST_PP_ADD(N, 2))   \
                                                  (remote_function, dc, source BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_)); \
  BOOST_PP_REPEAT(N, CHARSTRINGFREE, _)                \
  boost::iostreams::stream<resizing_array_sink> retstrm(128);    \
  oarchive oarc(retstrm); \
  oarc << ret; \
  retstrm.flush(); \
  if (packet_type_mask & CONTROL_PACKET) { \
    dc.RPC_CALL(control_call, request_reply_handler)(source, id, blob(retstrm->str, retstrm->len));\
  } \
  else {  \
    dc.RPC_CALL(reply_remote_call, request_reply_handler)(source, id, blob(retstrm->str, retstrm->len));\
  } \
  free(retstrm->str);                                                 \
} 

BOOST_PP_REPEAT(7, DISPATCH_GENERATOR, _)

#undef GENFN
#undef GENFN2
#undef GENARGS
#undef GENPARAMS
#undef DISPATCH_GENERATOR

/**
Same as above, but is the non-intrusive version.
*/
#define GENFN(N) BOOST_PP_CAT(NIF, N)
#define GENFN2(N) BOOST_PP_CAT(f, N)
#define GENNIARGS(Z,N,_)  (BOOST_PP_CAT(f, N))
#define GENPARAMS(Z,N,_)  BOOST_PP_CAT(T, N) (BOOST_PP_CAT(f, N)) ; iarc >> (BOOST_PP_CAT(f, N)) ;
#define CHARSTRINGFREE(Z,N,_)  charstring_free(BOOST_PP_CAT(f, N));

#define NONINTRUSIVE_DISPATCH_GENERATOR(Z,N,_) \
template<typename DcType, typename F, F remote_function  BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
void BOOST_PP_CAT(NONINTRUSIVE_REQUESTDISPATCH,N) (DcType& dc, procid_t source, unsigned char packet_type_mask, \
               const char* buf, size_t len) { \
  iarchive iarc(buf, len); \
  size_t id; iarc >> id;    \
  BOOST_PP_REPEAT(N, GENPARAMS, _)                \
  typename function_ret_type<__GLRPC_FRESULT>::type ret = function_ret_type<__GLRPC_FRESULT>::BOOST_PP_CAT(fcall, N) \
                                          (remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENNIARGS ,_)); \
  BOOST_PP_REPEAT(N, CHARSTRINGFREE, _)                \
  boost::iostreams::stream<resizing_array_sink> retstrm(128);    \
  oarchive oarc(retstrm); \
  oarc << ret; \
  retstrm.flush(); \
  if (packet_type_mask & CONTROL_PACKET) { \
    dc.RPC_CALL(control_call, request_reply_handler)(source, id, blob(retstrm->str, retstrm->len));\
  } \
  else if(packet_type_mask & FLUSH_PACKET) {  \
    dc.RPC_CALL(reply_remote_call, request_reply_handler)(source, id, blob(retstrm->str, retstrm->len));\
  } \
  else {  \
    dc.RPC_CALL(remote_call, request_reply_handler)(source, id, blob(retstrm->str, retstrm->len));\
  } \
  free(retstrm->str);                                                 \
} 

BOOST_PP_REPEAT(7, NONINTRUSIVE_DISPATCH_GENERATOR, _)


#undef GENFN
#undef GENFN2
#undef GENNIARGS
#undef GENPARAMS
#undef NONINTRUSIVE_DISPATCH_GENERATOR

} // namespace dc_impl
} // namespace graphlab
#include <rpc/function_arg_types_undef.hpp>
#endif

