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


#ifndef DC_INTERNAL_TYPES_HPP
#define DC_INTERNAL_TYPES_HPP
#include <boost/function.hpp>
#include <boost/unordered_map.hpp>
#include <rpc/dc_types.hpp>
#include <graphlab/util/resizing_array_sink.hpp>
#include <serialization/serialization_includes.hpp>
#include <parallel/pthread_tools.hpp>
namespace graphlab {
class distributed_control;

namespace dc_impl {  

/** 
 * \internal
 * \ingroup rpc
 * The type of the callback function used by the communications
classes when data is received*/
typedef void (*comm_recv_callback_type)(void* tag, procid_t src, 
                                        const char* buf, size_t len);

/**
 * \internal
 * \ingroup rpc
 * The type of the local function call dispatcher.
 * \see dispatch_type2
 */
typedef void (*dispatch_type)(distributed_control& dc, procid_t, unsigned char, const char* data, size_t len);

/**
 *\internal
 * \ingroup rpc
 * A second type of the local function call dispatcher.
 * Currently only used by POD calls. TODO: to move all other call
 * systems to use dispatch2.
 * \see dispatch_type
 */
typedef void (*dispatch_type2)(distributed_control& dc, procid_t, unsigned char, const char* data, size_t len);


typedef boost::unordered_map<std::string, dispatch_type> dispatch_map_type;

// commm capabilities
const size_t COMM_STREAM = 1;
const size_t COMM_DATAGRAM = 0;

/**
 * \internal
 * \ingroup rpc
 * The header form of each packet */
struct packet_hdr {
  uint32_t len; /// length of the packet
  procid_t src; /// source machine
  unsigned char packet_type_mask; /// the types are in dc_packet_mask.hpp
  unsigned char sequentialization_key;
};

typedef uint32_t block_header_type;

/**
 * \internal
 * \ingroup rpc
 * special handling for the only pointer datatype 
we natively support serialization for. Basically,
we must delete it. if charstring_free is called on a
char*, it will be deleted. Otherwise it will not do anything*/
template <typename T> 
inline void charstring_free(T& t) { }

/**
 * \internal
 * \ingroup rpc
 */
template <>
inline void charstring_free<char*>(char* &c){
    delete [] c;
};



/**
 * \internal
 * \ingroup rpc
 * 
 * The data needed to receive the matched send / recvs */
struct recv_from_struct {
  inline recv_from_struct():tag(0), hasdata(false) { }
  
  std::string data;
  size_t tag;
  mutex lock;
  conditional cond;
  bool hasdata;
  
};

/**
 * \internal
 * \ingroup rpc
 * Used for termination detection
 */
struct terminator_token {
  terminator_token():calls_sent(0),calls_recv(0),terminate(false) { }
  terminator_token(size_t sent, size_t recv):calls_sent(sent),
                          calls_recv(recv),terminate(false) { }
  size_t calls_sent;
  size_t calls_recv;
  bool terminate;
};



/**
 * Used to maintain a linked list of buffers.
 */
struct buffer_elem {
  char* buf;
  size_t len;
  buffer_elem* next;
};

}
}

SERIALIZABLE_POD(graphlab::dc_impl::terminator_token);
#endif

