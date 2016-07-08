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


#ifndef GRAPHLAB_FIBER_BUFFERED_EXCHANGE_HPP
#define GRAPHLAB_FIBER_BUFFERED_EXCHANGE_HPP

#include <parallel/pthread_tools.hpp>
#include <fiber/fiber_control.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <rpc/mpi_tools.hpp>


#include <graphlab/macros_def.hpp>
namespace graphlab {

  /**
   * \ingroup rpc
   *
   * The buffered exchange provides high performance exchange of bulk data 
   * between machines. This is like the \ref graphlab::buffered_exchange, but is
   * much stricter. All send calls must occur within fibers, and all recv calls
   * must occur within fibers. Specifically, a collection of "fiber-worker-local"
   * send buffer and receive buffers are created. Sends by fibers write into the
   * buffer owned by the current worker handling the fiber. Receives similarly
   * read from the buffer owned by the current worker handling the fiber.
   * As such a bit more subtlety is needed to use this class correctly.
   *
   * For instance, if we are doing bulk exchanges of integers:
   * \code
   * buffered_exchange<int> exchange(dc);
   *  .. In parallel in fibers .. {
   *    exchange.send([target machine], [value to send to target])
   *    exchange.partial_flush();
   *  }
   *
   *  .. now in 1 thread ..
   *  exchange.flush()
   *
   *  .. In parallel in fibers .. {
   *    procid_t proc;
   *    std::vector<buffered_exchange<int>::buffer_record> buffer; // (an array of buffers)
   *    while(exchange.recv(buffer)) {
   *      for each buffer_record in buffer:
   *        buffer_record.proc is the machine which sent the contents of this record
   *        buffer_record.buffer is an array containing values sent by the machine buffer_record.proc
   *    }
   *  }
   *  
   *  .. now in 1 thread ..
   *  exchange.recv(buffer, false); // get from all receive buffers
   *  while(exchange.recv(buffer)) {
   *    for each buffer_record in buffer:
   *      buffer_record.proc is the machine which sent the contents of this record
   *      buffer_record.buffer is an array containing values sent by the machine buffer_record.proc
   *  }
   * \endcode
   *
   * \note The buffered exchange sends data in the background, so recv can be
   * called even before the flush calls.
   * \note The last single threaded receive is not necessary if worker-affinity
   * is set correctly so that every worker is active in the parallel receiving
   * block.
   *
   * \see graphlab::buffered_exchange
   */
  template<typename T>
  class fiber_buffered_exchange {
  public:
    typedef std::vector<T> buffer_type;

    struct buffer_record {
      procid_t proc;
      buffer_type buffer;
      buffer_record() : proc(-1)  { }
    }; // end of buffer record
    typedef std::vector<buffer_record> recv_buffer_type;
  private:



    /** The rpc interface for this class */
    mutable dc_dist_object<fiber_buffered_exchange> rpc;

    std::vector<std::vector< buffer_record> > recv_buffers;


    struct send_record {
      oarchive* oarc;
      size_t numinserts;
    };

    std::vector<std::vector<send_record> > send_buffers;
    const size_t max_buffer_size;


    /**
     * Flushes the send buffer local to worker id "wid" and going to process proc
     */
    void flush_buffer(size_t wid, procid_t proc) {
      if(send_buffers[wid][proc].oarc) {
        // write the length at the end of the buffere are returning
        send_buffers[wid][proc].oarc->write(reinterpret_cast<char*>(&send_buffers[wid][proc].numinserts), sizeof(size_t));
        rpc.split_call_end(proc, send_buffers[wid][proc].oarc);
//         logstream(LOG_DEBUG) << rpc.procid() << ": Sending exchange of length " 
//                              << send_buffers[wid][proc].oarc->off << " to " 
//                              << proc << std::endl;
        send_buffers[wid][proc].oarc = NULL;
        send_buffers[wid][proc].numinserts = 0;
      }
    }

  public:
    /**
     * Constructs a buffered exchange object.
     *
     * \ref dc The master distributed_control object
     * \ref max_buffer_size The size of the per thread and per target send buffer.
     */
    fiber_buffered_exchange(distributed_control& dc,
                      const size_t max_buffer_size = DEFAULT_BUFFERED_EXCHANGE_SIZE) :
      rpc(dc, this),
      max_buffer_size(max_buffer_size) {
       send_buffers.resize(fiber_control::get_instance().num_workers());
       recv_buffers.resize(fiber_control::get_instance().num_workers());
       for (size_t i = 0;i < send_buffers.size(); ++i) {
         send_buffers[i].resize(dc.numprocs());
         for (size_t j = 0;j < send_buffers[i].size(); ++j) {
           send_buffers[i][j].oarc = NULL;
           send_buffers[i][j].numinserts = 0;
         }
       }
       rpc.barrier();
      }


    ~fiber_buffered_exchange() {
      // clear the send buffers
      for (size_t i = 0;i < send_buffers.size(); ++i) {
        for (size_t j = 0;j < send_buffers[i].size(); ++j) {
          if (send_buffers[i][j].oarc) rpc.split_call_cancel(send_buffers[i][j].oarc);
        }
      }
    }
    // fiber_buffered_exchange(distributed_control& dc, handler_type recv_handler,
    //                   size_t buffer_size = 1000) :
    // rpc(dc, this), send_buffers(dc.numprocs()), send_locks(dc.numprocs()),
    // max_buffer_size(buffer_size), recv_handler(recv_handler) { rpc.barrier(); }


    /**
     * Sends a value to a target machine.
     * Must be called from within a fiber
     */
    void send(const procid_t proc, const T& value) {
      size_t wid = fiber_control::get_worker_id();
      if (send_buffers[wid][proc].oarc == NULL) {
        send_buffers[wid][proc].oarc = rpc.split_call_begin(&fiber_buffered_exchange::rpc_recv);
        // write a header
        (*send_buffers[wid][proc].oarc) << rpc.procid();
        send_buffers[wid][proc].numinserts = 0;
      }

      (*(send_buffers[wid][proc].oarc)) << value;
      ++send_buffers[wid][proc].numinserts;


      if(send_buffers[wid][proc].oarc->off >= max_buffer_size) {
        flush_buffer(wid, proc);
      }
    } // end of send

    /**
     * Flushes the send buffers owned by the worker currently running the 
     * current fiber.
     */
    void partial_flush() {
      for(procid_t proc = 0; proc < rpc.numprocs(); ++proc) {
        flush_buffer(fiber_control::get_worker_id(), proc);
      }
    }

    /**
     * Flushes all send buffers. Must be called only on one thread.
     * Will not return until all machines call flush.
     */
    void flush() {
      for(size_t i = 0; i < send_buffers.size(); ++i) {
        for (size_t j = 0;j < send_buffers[i].size(); ++j) {
          flush_buffer(i,j);
        }
      }
      rpc.dc().flush();
      rpc.full_barrier();
    } // end of flush

    /**
     * Receives a collection of buffers.
     * Must be called from within a fiber.
     * \param ret_buffer If return value is true, this contains a collection of 
     *                   buffers sent to this machine.
     * \param self_buffer If true, only receives the worker local buffers.
     * \returns true If ret_buffer contains values.
     */
    bool recv(std::vector<buffer_record>& ret_buffer,
              const bool self_buffer = true) {
      fiber_control::fast_yield();
      ret_buffer.clear();
      bool success = false;
      if (self_buffer) {
        // get from my own buffer
        size_t wid = fiber_control::get_worker_id();
        if(!recv_buffers[wid].empty()) {
          success = true;
          std::swap(ret_buffer, recv_buffers[wid]);
        }
      } else {
        for (size_t i = 0;i < recv_buffers.size(); ++i) {
          if(!recv_buffers[i].empty()) {
            success = true;
            std::swap(ret_buffer, recv_buffers[i]);
            break;
          }
        }
      }
      return success;
    } // end of recv



    /**
     * Returns the number of elements avalable for receiving. 
     */
    size_t size() const {
      size_t count = 0;
      for (size_t i = 0;i < recv_buffers.size(); ++i) {
        count += recv_buffers[i].size();
      }
      return count;
    } // end of size

    /**
     * Returns true if there are no elements to receive.
     */
    bool empty() const { 
      for (size_t i = 0;i < recv_buffers.size(); ++i) {
        if (recv_buffers[i].size() > 0) return false;
      }
      return true;
    }

    void clear() { }

    void barrier() { rpc.barrier(); }
  private:
    void rpc_recv(size_t len, wild_pointer w) {
      buffer_type tmp;
      iarchive iarc(reinterpret_cast<const char*>(w.ptr), len);
      // first desrialize the source process
      procid_t src_proc; iarc >> src_proc;
//       logstream(LOG_DEBUG) << rpc.procid() << ": Receiving exchange of length "
//                            << len << " from " << src_proc << std::endl;
      // create an iarchive which just points to the last size_t bytes
      // to get the number of elements
      iarchive numel_iarc(reinterpret_cast<const char*>(w.ptr) + len - sizeof(size_t),
                          sizeof(size_t));
      size_t numel = 0; 
      numel_iarc.read(reinterpret_cast<char*>(&numel), sizeof(size_t));
      //std::cout << "Receiving: " << numel << "\n";
      tmp.resize(numel);
      for (size_t i = 0;i < numel; ++i) {
        iarc >> tmp[i];
      }

      size_t wid = fiber_control::get_worker_id();
      recv_buffers[wid].push_back(buffer_record());
      buffer_record& rec = recv_buffers[wid].back();
      rec.proc = src_proc;
      rec.buffer.swap(tmp);
    } // end of rpc rcv



  }; // end of buffered exchange


}; // end of graphlab namespace
#include <graphlab/macros_undef.hpp>

#endif


