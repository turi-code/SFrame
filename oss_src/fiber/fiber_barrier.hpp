/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_PARALLEL_FIBER_BARRIER_HPP
#define GRAPHLAB_PARALLEL_FIBER_BARRIER_HPP
#include <parallel/pthread_tools.hpp>
#include <fiber/fiber_control.hpp>
namespace graphlab {
  class fiber_barrier {
  private:
    graphlab::mutex mutex;
    graphlab::conditional conditional;
    mutable int needed;
    mutable int called;   
    
    mutable bool barrier_sense;
    mutable bool barrier_release;
    bool alive;

    mutable std::vector<size_t> fiber_handles;

    // not copyconstructible
    fiber_barrier(const fiber_barrier&) { }


    // not copyable
    void operator=(const fiber_barrier& m) { }

  public:
    /// Construct a barrier which will only fall when numthreads enter
    fiber_barrier(size_t numthreads) {
      needed = numthreads;
      called = 0;
      barrier_sense = false;
      barrier_release = true;
      alive = true;
      fiber_handles.resize(needed);
    }

    void resize_unsafe(size_t numthreads) {
      needed = numthreads;
      fiber_handles.resize(needed);
    }
    
    /// Wait on the barrier until numthreads has called wait
    inline void wait() const {
      if (!alive) return;
      mutex.lock();
      // set waiting;
      fiber_handles[called] = fiber_control::get_tid();
      called++;
      bool listening_on = barrier_sense;
      if (called == needed) {
        // if I have reached the required limit, wait up. Set waiting
        // to 0 to make sure everyone wakes up
        std::vector<size_t> to_wake = fiber_handles;
        called = 0;
        barrier_release = barrier_sense;
        barrier_sense = !barrier_sense;
        // clear all waiting, wake everyone. (less 1 since current thread
        // is already awake)
        for (size_t i = 0;i < to_wake.size() - 1; ++i) {
          fiber_control::schedule_tid(to_wake[i]);
        } 
      } else {
        // while no one has broadcasted, sleep
        while(barrier_release != listening_on && alive) {
          fiber_control::deschedule_self(&mutex.m_mut);
          mutex.lock();
        }
      }
      mutex.unlock();
    }
  }; // end of conditional
  
}
#endif 
