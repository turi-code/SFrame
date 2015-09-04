/**
 * Copyright (C) 2015 Dato, Inc.
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



#include <iostream>
#include <algorithm>
#include <rpc/dc.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/dc_stream_receive.hpp>

//#define DC_RECEIVE_DEBUG
namespace graphlab {
namespace dc_impl {



char* dc_stream_receive::get_buffer(size_t& retbuflength) {
  retbuflength = write_buffer_len - write_buffer_written;
  return writebuffer + write_buffer_written;
}


char* dc_stream_receive::advance_buffer(char* c, size_t wrotelength, 
                            size_t& retbuflength) {
  // find the last complete message we have read
  write_buffer_written += wrotelength;
  if (write_buffer_written >= sizeof(packet_hdr)) {
    size_t offset = 0;
    packet_hdr* hdr = reinterpret_cast<packet_hdr*>(writebuffer);
    // keep pushing the header until I reach a point where there is insufficient
    // room to read a header, or the message is not large enough
    while(offset + sizeof(packet_hdr) <= write_buffer_written &&
          offset + hdr->len + sizeof(packet_hdr) <= write_buffer_written) {
      offset += hdr->len + sizeof(packet_hdr);
      hdr = reinterpret_cast<packet_hdr*>(writebuffer + offset);
    }

    if (offset > 0) {
      // ok. everything before the offset is good
      // since we are going to give this buffer away, we need to prepare a new buffer

      // allocate whatever it is going to take to hold next message
      // have we read the incomplete message's header?
      size_t incomplete_message_len = 0;
      if (offset + sizeof(packet_hdr) <= write_buffer_written) incomplete_message_len = hdr->len;

      size_t new_buflen = std::max<size_t>(sizeof(packet_hdr) + incomplete_message_len, RECEIVE_BUFFER_SIZE);
      char* new_writebuffer = (char*)malloc(new_buflen);

      if (write_buffer_len - offset > 0) {
        // copy over to the new buffer everything we will not use
        memcpy(new_writebuffer, writebuffer + offset, write_buffer_written - offset);
      }
      // if we reach here, we have an available block
      // give away the buffer to dc
      dc->deferred_function_call_chunk(writebuffer, offset, associated_proc);
      writebuffer = new_writebuffer;
      write_buffer_written -= offset;
      write_buffer_len = new_buflen;
    } else {
      // nothing ready yet
      // do we have enough room though?
      if (hdr->len + sizeof(packet_hdr) > write_buffer_len) {
        size_t newlen = hdr->len + sizeof(packet_hdr);
        writebuffer = (char*)realloc(writebuffer, newlen);
        write_buffer_len = newlen;
      }
    }
  }
  return get_buffer(retbuflength);
}


  
void dc_stream_receive::shutdown() { }

} // namespace dc_impl
} // namespace graphlab

