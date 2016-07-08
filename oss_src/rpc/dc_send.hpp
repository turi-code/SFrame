/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**  
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


#ifndef DC_SEND_HPP
#define DC_SEND_HPP
#include <sys/types.h>
#include <sys/socket.h>

#include <iostream>
#include <rpc/circular_iovec_buffer.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/thread_local_send_buffer.hpp>
#include <rpc/dc_types.hpp>
namespace graphlab {
namespace dc_impl {

/**
\ingroup rpc
\internal
Base class of the data sending class.
This class forms the sending side of a "multiplexer"
send_data() will be called with a packet mask as well as a
character stream containing the contents of the packet.
The class should accumulate the data in an iovec structure
and relinquish it on get_outgoing_data()
*/
class dc_send{
 public:
  dc_send() { }
  
  virtual ~dc_send() { }

  virtual void register_send_buffer(thread_local_buffer* buffer) = 0;
  virtual void unregister_send_buffer(thread_local_buffer* buffer) = 0;

  /**
    Bytes sent must be incremented BEFORE the data is transmitted.
    Packets marked CONTROL_PACKET should not be counted
  */
  virtual size_t bytes_sent() = 0;
 
  /**
   * flushes immediately
   */ 
  virtual void flush() = 0;

  /**
   * Requests a flush as soon as possible
   */
  virtual void flush_soon() = 0;


  /**
   * Writes a string to an internal buffer to be flushed later.
   * This is a "slow path" to be used only when the thread local buffer
   * is not available.
   */
  virtual void write_to_buffer(char* c, size_t len) = 0;

  virtual size_t set_option(std::string opt, size_t val) {
    return 0;
  }

  /**
   * Returns length if there is data, 0 otherwise. This function
   * must be reentrant, but it is guaranteed that only one thread will
   * call this function at anytime.
   */
  virtual size_t get_outgoing_data(circular_iovec_buffer& outdata) = 0;


  /**
   * Utility function: writes a packet header into an archive.
   * but returns an offset to the location of the length entry allowing it to
   * be filled in later.
   */
  inline static size_t write_packet_header(oarchive& oarc, 
                                           procid_t src, 
                                           unsigned char packet_type_mask, 
                                           unsigned char sequentialization_key) {
    size_t base = oarc.off;
    oarc.advance(sizeof(packet_hdr));
    packet_hdr* hdr = reinterpret_cast<packet_hdr*>(oarc.buf + (oarc.off - sizeof(packet_hdr)));
    hdr->len = 0;
    hdr->src = src;
    hdr->packet_type_mask = packet_type_mask;
    hdr->sequentialization_key = sequentialization_key;
    return base;
  }
};
  

} // namespace dc_impl
} // namespace graphlab
#endif

