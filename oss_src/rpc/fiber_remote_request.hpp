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


#ifndef GRAPHLAB_FIBER_RPC_FUTURE_HPP
#define GRAPHLAB_FIBER_RPC_FUTURE_HPP
#include <rpc/request_future.hpp>
#include <rpc/request_reply_handler.hpp>
#include <fiber/fiber_control.hpp>
#include <parallel/pthread_tools.hpp>
namespace graphlab {

/**
 * A implementation of the ireply_container interface
 * that will wait for rpc requests, but if the request is issued from within 
 * a fiber, will deschedule the fiber.
 */
struct fiber_reply_container: public dc_impl::ireply_container {
  dc_impl::blob val;
  mutex lock;
  conditional cond;
  // if wait is in a fiber, this will contain the ID of the fiber to wake up
  // If 0, the wait is not in a fiber.
  size_t waiting_tid;
  // true when the blob is assigned
  bool valready;

  fiber_reply_container():waiting_tid(0),valready(false) { }

  ~fiber_reply_container() {
    val.free();
  }

  void wait() {
    if (fiber_control::in_fiber()) {    
      // if I am in a fiber, use the deschedule mechanism 
      lock.lock();
      waiting_tid = fiber_control::get_tid();
      while(!valready) {
        // set the waiting tid value
        // deschedule myself. This will deschedule the fiber
        // and unlock the lock atomically
        fiber_control::deschedule_self(&lock.m_mut);
        // unlock the condition variable, this does not re-lock the lock
        lock.lock();
      }
      lock.unlock();
    } else {
      // Otherwise use the condition variable
      waiting_tid = 0;
      lock.lock();
      while(!valready) cond.wait(lock);
      lock.unlock();
    }
  }

  void receive(procid_t source, dc_impl::blob b) {
    lock.lock();
    val = b;
    valready = true;
    if (waiting_tid) {
      // it is a fiber. wake it up.
      fiber_control::schedule_tid(waiting_tid);
    } else {
      // not in fiber. This is just a condition signal
      cond.signal();
    }
    lock.unlock();
  }
  bool ready() const {
    return valready;
  }

  dc_impl::blob& get_blob() {
    return val;
  }
};


} // namespace graphlab

#endif
