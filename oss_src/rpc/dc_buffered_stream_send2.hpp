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


#ifndef DC_BUFFERED_STREAM_SEND2_HPP
#define DC_BUFFERED_STREAM_SEND2_HPP
#include <iostream>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/type_traits/is_base_of.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/thread_local_send_buffer.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_comm_base.hpp>
#include <rpc/dc_send.hpp>
#include <parallel/pthread_tools.hpp>
#include <graphlab/util/inplace_lf_queue.hpp>
#include <logger/logger.hpp>
namespace graphlab {
class distributed_control;

namespace dc_impl {


/**
 * \internal
   \ingroup rpc
Sender for the dc class.
  The job of the sender is to take as input data blocks of
  pieces which should be sent to a single destination socket.
  This can be thought of as a sending end of a multiplexor.
  This class performs buffered transmissions using an blocking
  queue with one call per queue entry.
  A seperate thread is used to transmit queue entries. Rudimentary
  write combining is used to decrease transmission overhead.
  This is typically the best performing sender.

  This can be enabled by passing "buffered_queued_send=yes"
  in the distributed control initstring.

  dc_buffered_stream_send22 is similar, but does not perform write combining.

*/

class dc_buffered_stream_send2: public dc_send{
 public:
  dc_buffered_stream_send2(distributed_control* dc,
                                   dc_comm_base *comm,
                                   procid_t target) :
                  dc(dc),  comm(comm), target(target) { }

  ~dc_buffered_stream_send2();

  void register_send_buffer(thread_local_buffer* buffer);

  void unregister_send_buffer(thread_local_buffer* buffer);

  size_t get_outgoing_data(circular_iovec_buffer& outdata);

  inline size_t bytes_sent();

  void write_to_buffer(char* c, size_t len);

  void flush();

  void flush_soon();

 private:
  /// pointer to the owner
  distributed_control* dc;
  dc_comm_base *comm;
  procid_t target;
  atomic<size_t> total_bytes_sent;



  std::vector<thread_local_buffer*> send_buffers;
  // temporary array matched to the same length as send_buffers
  // to avoid repeated reallocation of this array when 
  // get_outgoing_data is called
  std::vector<std::vector<std::pair<char*, size_t> > > to_send;

  std::vector<std::pair<char*, size_t> > additional_flush_buffers;
  mutex lock;
};



} // namespace dc_impl
} // namespace graphlab
#endif // DC_BUFFERED_STREAM_SEND_EXPQUEUE_HPP

