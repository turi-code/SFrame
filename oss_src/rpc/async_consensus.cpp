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


#include <rpc/async_consensus.hpp>
#include <rpc/dc_macros.hpp>

namespace graphlab {
  async_consensus::async_consensus(distributed_control &dc,
                                   size_t required_threads_in_done,
                                   const dc_impl::dc_dist_object_base *attach)
    :rmi(dc, this), attachedobj(attach),
     last_calls_sent(0), last_calls_received(0),
     numactive(required_threads_in_done),
     ncpus(required_threads_in_done),
     done(false),
     trying_to_sleep(0),
     critical(ncpus, 0),
     sleeping(ncpus, 0),
     hastoken(dc.procid() == 0),
     cond(ncpus){

    cur_token.total_calls_sent = 0;
    cur_token.total_calls_received = 0;
    cur_token.last_change = (procid_t)(rmi.numprocs() - 1);
  }

  void async_consensus::reset() {
    last_calls_sent = 0;
    last_calls_received = 0;
    numactive = ncpus;
    done = false;
    trying_to_sleep = false;
    critical = std::vector<char>(ncpus, 0);
    sleeping = std::vector<char>(ncpus, 0);
    hastoken = (rmi.procid() == 0);
    cur_token.total_calls_sent = 0;
    cur_token.total_calls_received = 0;
    cur_token.last_change = (procid_t)(rmi.numprocs() - 1);
  }

  void async_consensus::force_done() {
    m.lock();
    done = true;
    m.unlock();
    cancel();
  }

  void async_consensus::begin_done_critical_section(size_t cpuid) {
    trying_to_sleep.inc();
    critical[cpuid] = true;
    m.lock();
  }


  void async_consensus::cancel_critical_section(size_t cpuid) {
    m.unlock();
    critical[cpuid] = false;
    trying_to_sleep.dec();
  }
  
  bool async_consensus::end_done_critical_section(size_t cpuid) {
    // if done flag is set, quit immediately
    if (done) {
      m.unlock();
      critical[cpuid] = false;
      trying_to_sleep.dec();
      return true;
    }
    /*
      Assertion: Since numactive is decremented only within 
      a critical section, and is incremented only within the same critical 
      section. Therefore numactive is a valid counter of the number of threads 
      outside of this critical section. 
    */
    --numactive;
  
    /*
      Assertion: If numactive is ever 0 at this point, the algorithm is done.
      WLOG, let the current thread which just decremented numactive be thread 0
    
      Since there is only 1 active thread (0), there must be no threads 
      performing insertions, and are no othe threads which are waking up.
      All threads must therefore be sleeping in cond.wait().
    */
    if (numactive == 0) {
      logstream(LOG_INFO) << rmi.procid() << ": Termination Possible" << std::endl;
      if (hastoken) pass_the_token();
    }
    sleeping[cpuid] = true;
    while(1) {
      // here we are protected by the mutex again.
      
      // woken up by someone else. leave the 
      // terminator
      if (sleeping[cpuid] == false || done) {
        break;
      }
      cond[cpuid].wait(m);
    }
    m.unlock();
    critical[cpuid] = false;
    trying_to_sleep.dec();
    return done;
  }
  
  void async_consensus::cancel() {
    /*
      Assertion: numactive > 0 if there is work to do.
      If there are threads trying to sleep, lets wake them up
    */
    if (trying_to_sleep > 0 || numactive < ncpus) {
      m.lock();
      size_t oldnumactive = numactive;
      // once I acquire this lock, all threads must be
      // in the following states
      // 1: still running and has not reached begin_critical_section()
      // 2: is sleeping in cond.wait()
      // 3: has called begin_critical_section() but has not acquired
      //    the mutex
      // In the case of 1,3: These threads will perform one more sweep
      // of their task queues. Therefore they will see any new job if available
      // in the case of 2: numactive must be < ncpus since numactive
      // is mutex protected. Then I can wake them up by
      // clearing their sleeping flags and broadcasting.
      if (numactive < ncpus) {
        // this is safe. Note that it is done from within 
        // the critical section.
        for (size_t i = 0;i < ncpus; ++i) {
          numactive += sleeping[i];
          if (sleeping[i]) {
            sleeping[i] = 0;
            cond[i].signal();
          }
        }
        if (oldnumactive == 0 && !done) {
          logstream(LOG_INFO) << rmi.procid() << ": Waking" << std::endl;
        }

      }
      m.unlock();
    }
  }

  void async_consensus::cancel_one(size_t cpuhint) {
    if (critical[cpuhint]) {
      m.lock();
      size_t oldnumactive = numactive;
      // see new_job() for detailed comments
      if (sleeping[cpuhint]) {
        numactive += sleeping[cpuhint];
        sleeping[cpuhint] = 0;
        if (oldnumactive == 0 && !done) {
          logstream(LOG_INFO) << rmi.procid() << ": Waking" << std::endl;
        }
        cond[cpuhint].signal();
      }
      m.unlock();
    }
  }

  void async_consensus::receive_the_token(token &tok) {
    m.lock();
    // save the token
    hastoken = true;
    cur_token = tok;
    // if I am waiting on done, pass the token.
    logstream(LOG_INFO) << rmi.procid() << ": Token Received" << std::endl;
    if (numactive == 0) {
      pass_the_token();
    }
    m.unlock();
  }

  void async_consensus::pass_the_token() {
    // note that this function does not acquire the token lock
    // the caller must acquire it 
    assert(hastoken);
    // first check if we are done
    if (cur_token.last_change == rmi.procid() && 
        cur_token.total_calls_received == cur_token.total_calls_sent) {
      logstream(LOG_INFO) << "Completed Token: " 
                          << cur_token.total_calls_received << " " 
                          << cur_token.total_calls_sent << std::endl;
      // we have completed a loop around!
      // broadcast a completion
      for (procid_t i = 0;i < rmi.numprocs(); ++i) {
        if (i != rmi.procid()) {
          rmi.RPC_CALL(control_call,async_consensus::force_done)(i);
        }
      }
      // set the complete flag
      // we can't call consensus() since it will deadlock
      done = true;
      // this is the same code as cancel(), but we can't call cancel 
      // since we are holding on to a lock
      if (numactive < ncpus) {
        // this is safe. Note that it is done from within 
        // the critical section.
        for (size_t i = 0;i < ncpus; ++i) {
          numactive += sleeping[i];
          if (sleeping[i]) {
            sleeping[i] = 0;
            cond[i].signal();
          }
        }
      }

    }
    else {
      // update the token
      size_t callsrecv;
      size_t callssent;
    
      if (attachedobj) {
        callsrecv = attachedobj->calls_received();
        callssent = attachedobj->calls_sent();
      }
      else {
        callsrecv = rmi.dc().calls_received();
        callssent = rmi.dc().calls_sent();
      }

      if (callssent != last_calls_sent ||
          callsrecv != last_calls_received) {
        cur_token.total_calls_sent += callssent - last_calls_sent;
        cur_token.total_calls_received += callsrecv - last_calls_received;
        cur_token.last_change = rmi.procid();
      }
      //std::cerr << "Sending token: (" << cur_token.total_calls_sent
      //<< ", " << cur_token.total_calls_received << ")" << std::endl;

      last_calls_sent = callssent;
      last_calls_received = callsrecv;
      // send it along.
      hastoken = false;
      logstream(LOG_INFO) << "Passing Token " << rmi.procid() << "-->" 
                          << (rmi.procid() + 1) % rmi.numprocs() << ": "
                          << cur_token.total_calls_received << " " 
                          << cur_token.total_calls_sent << std::endl;

      rmi.RPC_CALL(control_call,async_consensus::receive_the_token)
                   ((procid_t)((rmi.procid() + 1) % rmi.numprocs()),
                       cur_token);
    }
  }
}


