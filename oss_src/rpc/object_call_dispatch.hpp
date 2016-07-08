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


#ifndef GRAPHLAB_OBJECT_CALL_DISPATCH_HPP
#define GRAPHLAB_OBJECT_CALL_DISPATCH_HPP
#include <iostream>
#include <serialization/serialization_includes.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/mem_function_arg_types_def.hpp>
#include <boost/preprocessor.hpp>
namespace graphlab {
namespace dc_impl {



/**
\ingroup rpc
\internal
\file object_call_dispatch.hpp
This is an internal function and should not be used directly

This is similar to a regular function call in function_call_dispatch.hpp
with the only difference
that it needs to locate the object using dc.get_registered_object(...)
After the function call, it also needs to increment the call count for
the object context.
\code
template<typename DcType, typename T, typename F, F remote_function, typename T0 > 
void OBJECT_NONINTRUSIVE_DISPATCH1(DcType& dc, procid_t source, 
                                   unsigned char packet_type_mask, 
                                   const char* buf, size_t len){ 
  iarchive iarc(buf, len);
  size_t objid;
  iarc >> objid;
  T* obj = reinterpret_cast<T*>(dc.get_registered_object(objid));
  T0 (f0) ;
  iarc >> (f0) ;
  (obj->*remote_function)( (f-1) );
  charstring_free(f0);
  if ((packet_type_mask & CONTROL_PACKET) == 0) dc.get_rmi_instance(objid)->inc_calls_received(source);
}
\endcode
*/



#define GENFN(N) BOOST_PP_CAT(__GLRPC_NIF, N)
#define GENFN2(N) BOOST_PP_CAT(f, N)
#define GENARGS(Z,N,_) (BOOST_PP_CAT(f, N))

/**
 * This macro defines and deserializes each of the parameters to the
 * function.
 */
#define GENPARAMS(Z,N,_)  \
  BOOST_PP_CAT(T, N) (BOOST_PP_CAT(f, N)) ; \
  iarc >> (BOOST_PP_CAT(f, N)) ;

#define CHARSTRINGFREE(Z,N,_)  charstring_free(BOOST_PP_CAT(f, N));


#define OBJECT_NONINTRUSIVE_DISPATCH_GENERATOR(Z,N,_)                   \
  template<typename DcType, typename T,                                 \
           typename F, F remote_function BOOST_PP_COMMA_IF(N)                              \
           BOOST_PP_ENUM_PARAMS(N, typename T) >                        \
  void BOOST_PP_CAT(OBJECT_NONINTRUSIVE_DISPATCH,N)(DcType& dc,         \
                                                    procid_t source,    \
                                                    unsigned char packet_type_mask, \
                                                    const char* buf, size_t len){ \
    iarchive iarc(buf, len);                                                \
    size_t objid;                                                       \
    iarc >> objid;                                                      \
    T* obj = reinterpret_cast<T*>(dc.get_registered_object(objid));     \
    /* Deserialize the arguments to f */                                \
    BOOST_PP_REPEAT(N, GENPARAMS, _);                                   \
    /* Invoke f */                                                      \
    (obj->*remote_function)(BOOST_PP_ENUM(N,GENARGS ,_)  );                           \
    /* Free the buffers for the args */                                 \
    BOOST_PP_REPEAT(N, CHARSTRINGFREE, _) ;                             \
    /* Count the call if not a control call */                          \
    if ((packet_type_mask & CONTROL_PACKET) == 0)                       \
      dc.get_rmi_instance(objid)->inc_calls_received(source);           \
  } 



/**
 * This macro generates dispatch functions for functions for rpc calls
 * with up to 6 arguments.
 *
 * Remarks: If the compiler generates the following error "Too
 * few/many arguments to function" at this point is is due to the
 * caller not providing the correct number fo arguments in the RPC
 * call.  Note that default arguments are NOT supported in rpc calls
 * and so all arguments must be provided.
 *
 */
BOOST_PP_REPEAT(7, OBJECT_NONINTRUSIVE_DISPATCH_GENERATOR, _)



#undef GENFN
#undef GENFN2
#undef GENARGS
#undef GENPARAMS
#undef NONINTRUSIVE_DISPATCH_GENERATOR

} // namespace dc_impl
} // namespace graphlab


#include <rpc/mem_function_arg_types_undef.hpp>
#endif

