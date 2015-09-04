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


#ifndef GRAPHLAB_TIMER_HPP
#define GRAPHLAB_TIMER_HPP

#ifndef _MSC_VER
#include <sys/time.h>
#endif

#include <stdio.h>

#include <iostream>

namespace graphlab {
  /**
   * \ingroup util 
   *
   * \brief A simple class that can be used for benchmarking/timing up
   * to microsecond resolution.
   *
   * Standard Usage
   * =================
   *
   * The timer is used by calling \ref graphlab::timer::start and then
   * by getting the current time since start by calling 
   * \ref graphlab::timer::current_time.
   * 
   * For example:
   * 
   * \code
   * #include <graphlab.hpp>
   *
   *
   * graphlab::timer timer;
   * timer.start();
   * // do something
   * std::cout << "Elapsed time: " << timer.current_time() << std::endl; 
   * \endcode
   *
   * Fast approximate time
   * ====================
   *
   * Calling current item in a tight loop can be costly and so we
   * provide a faster less accurate timing primitive which reads a
   * local time variable that is updated roughly every 100 millisecond.
   * These are the \ref graphlab::timer::approx_time_seconds and
   * \ref graphlab::timer::approx_time_millis.
   */
  class timer {
  private:
    /**
     * \brief The internal start time for this timer object
     */
    timeval start_time_;   
  public:
    /**
     * \brief The timer starts on construction but can be restarted by
     * calling \ref graphlab::timer::start.
     */
    inline timer() { start(); }
    
    /**
     * \brief Reset the timer.
     */
    inline void start() { gettimeofday(&start_time_, NULL); }
    
    /** 
     * \brief Returns the elapsed time in seconds since 
     * \ref graphlab::timer::start was last called.
     *
     * @return time in seconds since \ref graphlab::timer::start was called.
     */
    inline double current_time() const {
      timeval current_time;
      gettimeofday(&current_time, NULL);
      double answer = 
       // (current_time.tv_sec + ((double)current_time.tv_usec)/1.0E6) -
       // (start_time_.tv_sec + ((double)start_time_.tv_usec)/1.0E6);
        (double)(current_time.tv_sec - start_time_.tv_sec) + 
        ((double)(current_time.tv_usec - start_time_.tv_usec))/1.0E6;
       return answer;
    } // end of current_time

    /** 
     * \brief Returns the elapsed time in milliseconds since 
     * \ref graphlab::timer::start was last called.
     *
     * @return time in milliseconds since \ref graphlab::timer::start was called.
     */
    inline double current_time_millis() const { return current_time() * 1000; }

    /**
     * \brief Get the number of seconds (as a floating point value)
     * since the Unix Epoch.
     */
    static double sec_of_day() {
      timeval current_time;
      gettimeofday(&current_time, NULL);
      double answer = 
        (double)current_time.tv_sec + ((double)current_time.tv_usec)/1.0E6;
      return answer;
    } // end of sec_of_day

    /**
     * \brief Returns only the micro-second component of the 
     * time since the Unix Epoch.
     */
    static size_t usec_of_day() {
      timeval current_time;
      gettimeofday(&current_time, NULL);
      size_t answer = 
        (size_t)current_time.tv_sec * 1000000 + (size_t)current_time.tv_usec;
      return answer;
    } // end of usec_of_day

    /**
     * \brief Returns the time since program start.
     * 
     * This value is only updated once every 100ms and is therefore
     * approximate (but fast).
     */
    static float approx_time_seconds();
    
    /**
     * \brief Returns the time since program start.
     * 
     * This value is only updated once every 100ms and is therefore
     * approximate (but fast).
     */
    static size_t approx_time_millis();

    /**
     * \brief Stops the approximate timer.
     *
     * This stops the approximate timer thread. Once stoped, the approximate
     * time will never be advanced again. This function should not generally
     * be used, but it seems like on certain platforms (windows for instance)
     * it does not like terminating threads inside DLLs at program terminiation.
     * This can be used to force thread termination.
     *
     * \see approx_time_seconds approx_time_millis
     */
    static void stop_approx_timer();

    /**
     * Sleeps for sleeplen seconds
     */
    static void sleep(size_t sleeplen);

    /**
     * Sleeps for sleeplen milliseconds.
     */
    static void sleep_ms(size_t sleeplen);
  }; // end of Timer
 
  typedef unsigned long long rdtsc_type; 

  rdtsc_type estimate_ticks_per_second();

  #if defined(__i386__)
  inline rdtsc_type rdtsc(void) {
    unsigned long long int x;
      __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
      return x;
  }
  #elif defined(__x86_64__)
  inline rdtsc_type rdtsc(void) {
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo) | ( ((unsigned long long)hi)<<32 );
  }
  #else
  inline rdtsc_type rdtsc(void) {
    return 0;
  }
  #endif


  /**
   * Very rudimentary timer class which allows tracking of fine grained
   * time with extremely low overhead using the RDTSC instruction.
   * Basic usage:
   * \code
   * rdtsc_time time;
   * ... // do stuff
   * time.ms(); // returns the number of milliseconds passed.
   * \endcode
   */
  struct rdtsc_time {
    rdtsc_type begin;
    /**
     * Constructs an rdtsc_time object and begin tracking the time.
     */
    rdtsc_time():begin(rdtsc()) { }

    /**
     * Returns the number of milliseconds passed since the rdtsc_time
     * object was constructed.
     */
    double ms() const {
      rdtsc_type end = rdtsc();
      double dtime = end - begin;
      dtime = dtime * 1000 / estimate_ticks_per_second();
      return dtime;
    }
  };
   


} // end of graphlab namespace

/** 
 * Convenience function. Allows you to call "cout << ti" where ti is
 * a timer object and it will print the number of seconds elapsed
 * since ti.start() was called.
 */
std::ostream&  operator<<(std::ostream& out, const graphlab::timer& t);


#endif

