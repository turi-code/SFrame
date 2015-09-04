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


#ifndef GRAPHLAB_BUFFERED_EXCHANGE_HPP
#define GRAPHLAB_BUFFERED_EXCHANGE_HPP

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
   * between machines. The basic usage is simple:
   *
   * For instance, if we are doing bulk exchanges of integers:
   * \code
   * buffered_exchange<int> exchange(dc, numthreads);
   *  .. In parallel .. {
   *    exchange.send([target machine], [value to send to target], [thread id])
   *    exchange.partial_flush([thread id]);
   *  }
   *
   *  .. now in 1 thread ..
   *  exchange.flush()
   *
   *  .. In parallel .. {
   *    procid_t proc;
   *    buffered_exchange<int>::buffer_type buffer; // (an array of integers)
   *    while(exchange.recv(proc, buffer)) {
   *      process array of integers (buffeer) which were sent by proc
   *    }
   *  }
   * \endcode
   *
   * \note The buffered exchange sends data in the background, so recv can be
   * called even before the flush calls.
   *
   * \see graphlab::fiber_buffered_exchange
   */
  template<typename T>
  class buffered_exchange {
  public:
    typedef std::vector<T> buffer_type;

  private:
    struct buffer_record {
      procid_t proc;
      buffer_type buffer;
      buffer_record() : proc(-1)  { }
    }; // end of buffer record



    /** The rpc interface for this class */
    mutable dc_dist_object<buffered_exchange> rpc;

    std::deque< buffer_record > recv_buffers;
    mutex recv_lock;


    struct send_record {
      oarchive* oarc;
      size_t numinserts;
    };

    std::vector<send_record> send_buffers;
    std::vector< mutex >  send_locks;
    // where local to local writes 
    std::vector<buffer_type> local_to_local_buffers;
    std::vector<mutex> local_to_local_locks;
    // where local to local writes go after a flush
    std::deque<buffer_type> flushed_local_to_local_buffers;
    mutex flushed_local_to_local_buffers_lock;
    const size_t num_threads;
    const size_t max_buffer_size;


    // typedef boost::function<void (const T& tref)> handler_type;
    // handler_type recv_handler;

  public:
    /**
     * Constructs a buffered exchange object.
     *
     * \ref dc The master distributed_control object
     * \ref num_threads The number of threads to support. This is essentially
     *                  the number of fine-grained locks to use. This does not
     *                  need to match the total number of threads used during 
     *                  the exchange process, but there are performance / contention
     *                  advantages if this matches.
     * \ref max_buffer_size The size of the per thread and per target send buffer.
     */
    buffered_exchange(distributed_control& dc,
                      const size_t num_threads = 1,
                      const size_t max_buffer_size = DEFAULT_BUFFERED_EXCHANGE_SIZE) :
      rpc(dc, this),
      send_buffers(num_threads *  dc.numprocs()),
      send_locks(num_threads *  dc.numprocs()),
      local_to_local_buffers(num_threads),
      local_to_local_locks(num_threads),
      num_threads(num_threads),
      max_buffer_size(max_buffer_size) {
       //
       for (size_t i = 0;i < send_buffers.size(); ++i) {
         // initialize the split call
         send_buffers[i].oarc = rpc.RPC_CALL(split_call_begin, buffered_exchange::rpc_recv)();
         send_buffers[i].numinserts = 0;
         // begin by writing the src proc.
         (*(send_buffers[i].oarc)) << rpc.procid();
       }
       rpc.barrier();
      }


    ~buffered_exchange() {
      // clear the send buffers
      for (size_t i = 0;i < send_buffers.size(); ++i) {
        rpc.RPC_CALL(split_call_cancel, buffered_exchange::rpc_recv)(send_buffers[i].oarc);
      }
    }
    // buffered_exchange(distributed_control& dc, handler_type recv_handler,
    //                   size_t buffer_size = 1000) :
    // rpc(dc, this), send_buffers(dc.numprocs()), send_locks(dc.numprocs()),
    // max_buffer_size(buffer_size), recv_handler(recv_handler) { rpc.barrier(); }


    /**
     * Sends a value to a target machine.
     * Use the send buffer owned by thread_id.
     */
    void send(const procid_t proc, const T& value, const size_t thread_id = 0) {
      if (proc == rpc.procid()) {
        local_to_local_locks[thread_id].lock();
        local_to_local_buffers[thread_id].push_back(value);
        local_to_local_locks[thread_id].unlock();
      } else {
        //ASSERT_LT(proc, rpc.numprocs());
        //ASSERT_LT(thread_id, num_threads);
        const size_t index = thread_id * rpc.numprocs() + proc;
        //ASSERT_LT(index, send_locks.size());
        send_locks[index].lock();

        (*(send_buffers[index].oarc)) << value;
        ++send_buffers[index].numinserts;

        if(send_buffers[index].oarc->off >= max_buffer_size) {
          oarchive* prevarc = swap_buffer(index);
          send_locks[index].unlock();
          // complete the send
          rpc.RPC_CALL(split_call_end, buffered_exchange::rpc_recv)(proc, prevarc);
        } else {
          send_locks[index].unlock();
        }
      }
    } // end of send

#if defined(__cplusplus) && __cplusplus >= 201103L
    /**
     * Sends a value to a target machine.
     * Use the send buffer owned by thread_id.
     */
    void send(const procid_t proc, T&& value, const size_t thread_id = 0) {
      if (proc == rpc.procid()) {
        local_to_local_locks[thread_id].lock();
        local_to_local_buffers[thread_id].push_back(std::forward<T>(value));
        local_to_local_locks[thread_id].unlock();
      } else {
        //ASSERT_LT(proc, rpc.numprocs());
        //ASSERT_LT(thread_id, num_threads);
        const size_t index = thread_id * rpc.numprocs() + proc;
        //ASSERT_LT(index, send_locks.size());
        send_locks[index].lock();

        (*(send_buffers[index].oarc)) << value;
        ++send_buffers[index].numinserts;

        if(send_buffers[index].oarc->off >= max_buffer_size) {
          oarchive* prevarc = swap_buffer(index);
          send_locks[index].unlock();
          // complete the send
          rpc.RPC_CALL(split_call_end, buffered_exchange::rpc_recv)(proc, prevarc);
        } else {
          send_locks[index].unlock();
        }
      }
    } // end of send
#endif

    /**
     * Flushes the send buffer owned owned by thread_id.
     */
    void partial_flush(size_t thread_id) {
      for(procid_t proc = 0; proc < rpc.numprocs(); ++proc) {
        if (proc != rpc.procid()) {
          const size_t index = thread_id * rpc.numprocs() + proc;
          ASSERT_LT(proc, rpc.numprocs());
          if (send_buffers[index].numinserts > 0) {
            send_locks[index].lock();
            oarchive* prevarc = swap_buffer(index);
            send_locks[index].unlock();
            // complete the send
            rpc.RPC_CALL(split_call_end, buffered_exchange::rpc_recv)(proc, prevarc);
            rpc.dc().flush_soon(proc);
          }
        } 
      }
    }

    /**
     * Flushes all send buffers. Must be called only on one thread.
     * Will not return until all machines call flush.
     */
    void flush() {
      for(size_t i = 0; i < send_buffers.size(); ++i) {
        const procid_t proc = i % rpc.numprocs();
        if (proc != rpc.procid()) { 
          ASSERT_LT(proc, rpc.numprocs());
          send_locks[i].lock();
          if (send_buffers[i].numinserts > 0) {
            oarchive* prevarc = swap_buffer(i);
            // complete the send
            rpc.RPC_CALL(split_call_end, buffered_exchange::rpc_recv)(proc, prevarc);
          }
          send_locks[i].unlock();
        }
      }
      // flush local to local writes
      
#if defined(__cplusplus) && __cplusplus >= 201103L
      for(buffer_type& rec: local_to_local_buffers) {
        flushed_local_to_local_buffers.push_back(std::move(rec));
        rec.clear();
      }
#else 
      foreach(buffer_type& rec, local_to_local_buffers) {
        flushed_local_to_local_buffers.push_back();
        std::swap(flushed_local_to_local_buffers.back(), ret);
        rec.clear();
      }
#endif
      rpc.dc().flush_soon();
      rpc.full_barrier();
    } // end of flush



    /**
     * Returns a collection of T sent by ret_proc.
     *
     * \param ret_proc On successful return, will contain a valid procid indicating
     *               that the values in the buffer were sent by that process.
     * \param ret_buffer A sequence of values sent by ret_proc
     * \param try_lock If true, will not acquire the lock if the lock is 
     *                 contended. Useful for polling the receive buffer
     *                 while sending is occuring.
     * \return True on success and there are values in the buffer. 
     *         False if the receive buffer is empty. Or if try_lock is set,
     *         False may also indicate the buffer lock is being contended.
     */
    bool recv(procid_t& ret_proc, buffer_type& ret_buffer,
              const bool try_lock = false) {
      if (!flushed_local_to_local_buffers.empty()) {
        flushed_local_to_local_buffers_lock.lock();
        // recheck the flushed buffer
        if (!flushed_local_to_local_buffers.empty()) {
          std::swap(ret_buffer, flushed_local_to_local_buffers.front());
          flushed_local_to_local_buffers.pop_front();
          ret_proc = rpc.procid();
          flushed_local_to_local_buffers_lock.unlock();
          return true;
        }
        flushed_local_to_local_buffers_lock.unlock();
      }
      fiber_control::fast_yield();
      dc_impl::blob read_buffer;
      bool has_lock = false;
      if(try_lock) {
        if (recv_buffers.empty()) return false;
        has_lock = recv_lock.try_lock();
      } else {
        recv_lock.lock();
        has_lock = true;
      }
      bool success = false;
      if(has_lock) {
        if(!recv_buffers.empty()) {
          success = true;
          buffer_record& rec =  recv_buffers.front();
          // read the record
          ret_proc = rec.proc;
          ret_buffer.swap(rec.buffer);
          ASSERT_LT(ret_proc, rpc.numprocs());
          recv_buffers.pop_front();
        }
        recv_lock.unlock();
      }

      return success;
    } // end of recv



    /**
     * Returns the number of elements available for receiving.
     */
    size_t size() const {
      typedef typename std::deque< buffer_record >::const_iterator iterator;
      recv_lock.lock();
      size_t count = 0;
      foreach(const buffer_record& rec, recv_buffers) {
        count += rec.buffer.size();
      }

      foreach(const buffer_type& rec, flushed_local_to_local_buffers) {
        count += rec.size();
      }
      recv_lock.unlock();
      return count;
    } // end of size

    /**
     * Returns true if there are no elements available for receiving.
     */
    bool empty() const { 
      return recv_buffers.empty() && flushed_local_to_local_buffers.empty(); 
    }

    void clear() { }

    void barrier() { rpc.barrier(); }
  private:
    void rpc_recv(size_t len, wild_pointer w) {
      buffer_type tmp;
      iarchive iarc(reinterpret_cast<const char*>(w.ptr), len);
      // first desrialize the source process
      procid_t src_proc; iarc >> src_proc;
      ASSERT_LT(src_proc, rpc.numprocs());
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

      recv_lock.lock();
      recv_buffers.push_back(buffer_record());
      buffer_record& rec = recv_buffers.back();
      rec.proc = src_proc;
      rec.buffer.swap(tmp);
      recv_lock.unlock();
    } // end of rpc rcv


    // create a new buffer for send_buffer[index], returning the old buffer
    oarchive* swap_buffer(size_t index) {
      oarchive* swaparc = rpc.RPC_CALL(split_call_begin,buffered_exchange::rpc_recv)();
      std::swap(send_buffers[index].oarc, swaparc);
      // write the length at the end of the buffere are returning
      (*swaparc).write(reinterpret_cast<char*>(&send_buffers[index].numinserts), sizeof(size_t));

      //std::cout << "Sending : " << (send_buffers[index].numinserts)<< "\n";
      // reset the insertion count
      send_buffers[index].numinserts = 0;
      // write the current procid into the new buffer
      (*(send_buffers[index].oarc)) << rpc.procid();
      return swaparc;
    }


  }; // end of buffered exchange


}; // end of graphlab namespace
#include <graphlab/macros_undef.hpp>

#endif


