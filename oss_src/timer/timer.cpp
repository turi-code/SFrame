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
#include <signal.h>
#include <sys/time.h>
#include <boost/bind.hpp>
#include <parallel/pthread_tools.hpp>
#include <timer/timer.hpp>

std::ostream&  operator<<(std::ostream& out, const graphlab::timer& t) {
  return out << t.current_time();
} 

namespace graphlab {
  
  
class hundredms_timer {
  thread timer_thread;
 public:
  hundredms_timer() {    
    stop = false;
    ti.start();
    timer_thread.launch(boost::bind(&hundredms_timer::alarm_thread, this));
  }
  size_t ctr; 
  timer ti;
  mutex lock;
  conditional cond;
  bool stop;

  void alarm_thread() {
    lock.lock();
    while(!stop) {
      cond.timedwait_ms(lock, 50);
      double realtime = ti.current_time() ;
      ctr = (size_t)(realtime * 10);
    }
    lock.unlock();
  }

  void stop_timer() {
    if (stop == false) {
      lock.lock();
      stop = true;
      cond.signal();
      lock.unlock();
      timer_thread.join();
    }
  }

  ~hundredms_timer() {  
    stop_timer();
  }
};

static hundredms_timer& get_hms_timer() {
  static hundredms_timer hmstimer;
  return hmstimer;
}
  
  

  /**
   * Precision of deciseconds 
   */
  float timer::approx_time_seconds() {
    return float(get_hms_timer().ctr) / 10;
  }

  /**
   * Precision of deciseconds 
   */
  size_t timer::approx_time_millis() {
    return get_hms_timer().ctr * 100;
  }

  void timer::stop_approx_timer() {
    get_hms_timer().stop_timer();
  }


  void timer::sleep(size_t sleeplen) {
    struct timespec timeout;
    timeout.tv_sec = sleeplen;
    timeout.tv_nsec = 0;
    while (nanosleep(&timeout, &timeout) == -1);
  }


  /**
  Sleeps for sleeplen milliseconds.
  */
  void timer::sleep_ms(size_t sleeplen) {
    struct timespec timeout;
    timeout.tv_sec = sleeplen / 1000;
    timeout.tv_nsec = (sleeplen % 1000) * 1000000;
    while (nanosleep(&timeout, &timeout) == -1);
  }
  

  
  
static unsigned long long rtdsc_ticks_per_sec = 0; 
static mutex rtdsc_ticks_per_sec_mutex;

unsigned long long estimate_ticks_per_second() {
  if (rtdsc_ticks_per_sec == 0) {
    rtdsc_ticks_per_sec_mutex.lock();
      if (rtdsc_ticks_per_sec == 0) {
      unsigned long long tstart = rdtsc();
      graphlab::timer::sleep(1);
      unsigned long long tend = rdtsc();
      rtdsc_ticks_per_sec = tend - tstart;
      }
    rtdsc_ticks_per_sec_mutex.unlock();
  }
  return rtdsc_ticks_per_sec;
}

}

