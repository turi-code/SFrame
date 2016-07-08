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


#ifndef EVENT_LOG_HPP
#define EVENT_LOG_HPP
#include <iostream>
#include <boost/bind.hpp>
#include <parallel/pthread_tools.hpp>
#include <parallel/atomic.hpp>
#include <timer/timer.hpp>
#include <util/dense_bitset.hpp>
namespace graphlab {

#define EVENT_MAX_COUNTERS 256

class event_log{
 public:
  enum event_print_type{
    NUMBER,
    DESCRIPTION,
    RATE_BAR,
    LOG_FILE
  };
 private:
  std::ostream* out;  // output target
  volatile size_t flush_interval; // flush frequency (ms)
  event_print_type print_method;  // print method
  volatile bool finished; // set on destruction
  
  mutex m;          
  conditional cond;
  
  thread printing_thread;
  std::string descriptions[EVENT_MAX_COUNTERS];
  size_t maxcounter[EVENT_MAX_COUNTERS];
  atomic<size_t> counters[EVENT_MAX_COUNTERS];
  atomic<size_t> totalcounter[EVENT_MAX_COUNTERS];

  std::vector<std::pair<unsigned char, size_t> > immediate_events;
  
  double prevtime;  // last time flush() was called
  bool hasevents;   // whether there are logging events
  size_t noeventctr; // how many times flush() was called with no events
                     // zeroed when the events are next observed.

  size_t max_desc_length; // maximum descriptor length
  fixed_dense_bitset<EVENT_MAX_COUNTERS> hascounter; 
 public:
  inline event_log():out(NULL),
                     flush_interval(0),
                     print_method(DESCRIPTION),
                     finished(false),
                     prevtime(0),
                     hasevents(false),
                     noeventctr(0),
                     max_desc_length(0) {
    hascounter.clear();
    for (size_t i = 0;i < EVENT_MAX_COUNTERS; ++i) {
      maxcounter[i] = 0;
      totalcounter[i].value = 0;
    }
    printing_thread.launch(boost::bind(&event_log::thread_loop, this));
  }
  
  void initialize(std::ostream &ostrm,
                  size_t flush_interval_ms,
                  event_print_type event_print);

  void close();
  
  void thread_loop();

  void add_event_type(unsigned char eventid, std::string description);
  void add_immediate_event_type(unsigned char eventid, std::string description);

  inline void accumulate_event(unsigned char eventid,
                               size_t count)  __attribute__((always_inline)) {
    hasevents = true;
    counters[eventid].inc(count);
    totalcounter[eventid].inc(count);
  }

  void immediate_event(unsigned char eventid);

  void flush();

  ~event_log();
};

}
/**
 * DECLARE_EVENT_LOG(name)
 * creates an event log with a given name. This creates a variable
 * called "name" which is of type event_log. and is equivalent to:
 *
 * graphlab::event_log name;
 *
 * The primary reason to use this macro instead of just writing
 * the code above directly, is that the macro is ignored and compiles
 * to nothing when event logs are disabled.
 *
 *
 * INITIALIZE_EVENT_LOG(name, ostrm, flush_interval, printdesc)
 * ostrm is the output std::ostream object. A pointer of the stream
 * is taken so the stream should not be destroyed until the event log is closed
 * flush_interval is the flush frequency in milliseconds.
 * printdesc is either graphlab::event_log::NUMBER, or graphlab::event_log::DESCRIPTION or
 * graphlab::event_log::RATE_BAR. There is also graphlab::event_log::LOG_FILE
 * which outputs to a centralized log. If this is used, the ostrm argument
 * is ignored and an output of the DESCRIPTION format will be written to an
 * eventlog.txt file in the current directory.
 * 
 * ADD_EVENT_TYPE(name, id, desc)
 * Creates an event type with an integer ID, and a description.
 * Event types should be mostly consecutive since the internal
 * storage format is a array. Valid ID range is [0, 255]
 *
 * ACCUMULATE_EVENT(name, id, count)
 * Adds 'count' events of type "id"
 *
 * FLUSH_EVENT_LOG(name)
 * Forces a flush of the accumulated events to the provided output stream
 */
#ifdef USE_EVENT_LOG
#define DECLARE_EVENT_LOG(name) graphlab::event_log name;
#define INITIALIZE_EVENT_LOG(name, ostrm, flush_interval, printdesc)    \
                    name.initialize(ostrm, flush_interval, printdesc);
#define ADD_EVENT_TYPE(name, id, desc) name.add_event_type(id, desc);
#define ADD_IMMEDIATE_EVENT_TYPE(name, id, desc) name.add_immediate_event_type(id, desc);
#define ACCUMULATE_EVENT(name, id, count) name.accumulate_event(id, count);
#define IMMEDIATE_EVENT(name, id) name.immediate_event(id);
#define FLUSH_EVENT_LOG(name) name.flush();
#define CLOSE_EVENT_LOG(name) name.close();

#else
#define DECLARE_EVENT_LOG(name) 
#define INITIALIZE_EVENT_LOG(name, ostrm, flush_interval, printdesc)
#define ADD_EVENT_TYPE(name, id, desc) 
#define ADD_IMMEDIATE_EVENT_TYPE(name, id, desc) 
#define ACCUMULATE_EVENT(name, id, count) 
#define IMMEDIATE_EVENT(name, id)
#define FLUSH_EVENT_LOG(name) 
#define CLOSE_EVENT_LOG(name)
#endif

#define PERMANENT_DECLARE_EVENT_LOG(name) graphlab::event_log name;
#define PERMANENT_INITIALIZE_EVENT_LOG(name, ostrm, flush_interval, printdesc)    \
                    name.initialize(ostrm, flush_interval, printdesc);
#define PERMANENT_ADD_EVENT_TYPE(name, id, desc) name.add_event_type(id, desc);
#define PERMANENT_ADD_IMMEDIATE_EVENT_TYPE(name, id, desc) name.add_immediate_event_type(id, desc);
#define PERMANENT_ACCUMULATE_EVENT(name, id, count) name.accumulate_event(id, count);
#define PERMANENT_IMMEDIATE_EVENT(name, id) name.immediate_event(id);
#define PERMANENT_FLUSH_EVENT_LOG(name) name.flush();
#define PERMANENT_CLOSE_EVENT_LOG(name) name.close();


#endif
