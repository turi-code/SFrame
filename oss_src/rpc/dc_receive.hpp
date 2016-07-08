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


#ifndef DC_RECEIVE_HPP
#define DC_RECEIVE_HPP
#include <rpc/dc_internal_types.hpp>
#include <rpc/dc_types.hpp>
#include <parallel/atomic.hpp>
namespace graphlab {
namespace dc_impl {
  
/**
\ingroup rpc
\internal
Base class of the data receiving class.
This class forms the receiving side of a "multiplexer"
Data entering from a single socket will be passed to this
function through the incoming_data function call.

This class must understand the packet header and issue the right
calls in the owning dc. 
*/
class dc_receive {
 public:
  dc_receive() { };
  virtual ~dc_receive() { };

  /**
    gets a buffer. The buffer length is returned in retbuflength
    This will be used for receiving data.
    If get_buffer() or advance_buffer() is called,
    incoming_data will never be called.
  */
  virtual char* get_buffer(size_t& retbuflength) = 0;
  
  /**
    Commits a buffer obtained using get_buffer.
    c will be the result of a previous call to get_buffer() or advance_buffer()
    This function should commit a range of bytes starting of c,
    up to 'wrotelength' bytes. A new empty buffer should be returned
    and the size is returned in retbuflength
  */
  virtual char* advance_buffer(char* c, size_t wrotelength, 
                              size_t& retbuflength) = 0;
  

  /**
   * Last call sent to any instance of dc_receive.
   * If the sender multithreads, the sending thread must shut down.
   */
  virtual void shutdown() = 0;
};


} // namespace dc_impl
} // namespace graphlab
#endif

