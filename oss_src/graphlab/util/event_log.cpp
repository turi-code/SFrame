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


#include <graphlab/util/event_log.hpp>
#include <timer/timer.hpp>
#include <logger/assertions.hpp>

#define EVENT_BAR_WIDTH 40
#define BAR_CHARACTER '#'

namespace graphlab {
  
static std::ofstream eventlog_file;
static mutex eventlog_file_mutex;
static bool eventlog_file_open = false;

static timer event_timer;
static bool event_timer_started = false;
static mutex event_timer_mutex;


void event_log::initialize(std::ostream &ostrm,
                           size_t flush_interval_ms,
                           event_print_type event_print) {
  m.lock();
  out = &ostrm;
  flush_interval = flush_interval_ms;
  print_method = event_print;
  
  event_timer_mutex.lock();
  if (event_timer_started == false) {
    event_timer_started = true;
    event_timer.start();
  }
  event_timer_mutex.unlock();
  prevtime = event_timer.current_time_millis();
  
  cond.signal(); 
  m.unlock();
  
  if (event_print == LOG_FILE) {
    eventlog_file_mutex.lock();
    if (!eventlog_file_open) {
      eventlog_file_open = true;
      eventlog_file.open("eventlog.txt");
    }
    out = &eventlog_file;
    eventlog_file_mutex.unlock();
  }

}

event_log::~event_log() {
  finished = true;
  m.lock();
  cond.signal();
  m.unlock();
  printing_thread.join();
  if (print_method != LOG_FILE) {
    size_t pos;
    if (hascounter.first_bit(pos)) {
      do {
        (*out) << descriptions[pos]  << ":\t" << totalcounter[pos].value << " Events\n";
      } while(hascounter.next_bit(pos));
    }
  }
  else{
    size_t pos;
    if (hascounter.first_bit(pos)) {
      do {
        std::cerr << descriptions[pos]  << ":\t" << totalcounter[pos].value << " Events\n";
      } while(hascounter.next_bit(pos));
    }
  }
}


void event_log::immediate_event(unsigned char eventid) {
  m.lock();
  immediate_events.push_back(std::make_pair(eventid, event_timer.current_time_millis()));
  m.unlock();
}

void event_log::close() {
  out = NULL;
  m.lock();
  flush_interval = 0;
  m.unlock();
}

void event_log::add_event_type(unsigned char eventid,
                               std::string description) {
  descriptions[eventid] = description;
  max_desc_length = std::max(max_desc_length, description.length());
  ASSERT_MSG(max_desc_length <= 30, "Event Description length must be <= 30 characters");
  counters[eventid].value = 0;
  hascounter.set_bit(eventid);
}

void event_log::add_immediate_event_type(unsigned char eventid, std::string description) {
  descriptions[eventid] = description;
  max_desc_length = std::max(max_desc_length, description.length());
  ASSERT_MSG(max_desc_length <= 30, "Event Description length must be <= 30 characters");
  counters[eventid].value = 0;
}

void event_log::flush() {
  size_t pos;
  if (!hascounter.first_bit(pos)) return;
  double curtime = event_timer.current_time_millis();
  double timegap = curtime - prevtime;
  prevtime = curtime;

  if (hasevents == false && noeventctr == 1) return;
  
  bool found_events = false;
  if (print_method == NUMBER) {
    do {
      size_t ctrval = counters[pos].exchange(0);
      found_events = found_events || ctrval > 0;
      (*out) << pos  << ":\t" << curtime << "\t" << ctrval << "\t" << 1000 * ctrval / timegap << " /s\n";
    } while(hascounter.next_bit(pos));
    // flush immediate events
    if (!immediate_events.empty()) { 
      std::vector<std::pair<unsigned char, size_t> > cur;
      cur.swap(immediate_events);
      for (size_t i = 0;i < cur.size(); ++i) {
        (*out) << (size_t)cur[i].first << ":\t" << cur[i].second << "\t" << -1 << "\t" << 0 << " /s\n";
      }
    }
    out->flush();
  }
  else if (print_method == DESCRIPTION) {
    do {
      size_t ctrval = counters[pos].exchange(0);
      found_events = found_events || ctrval > 0;
      (*out) << descriptions[pos]  << ":\t" << curtime << "\t" << ctrval << "\t" << 1000 * ctrval / timegap << " /s\n";
    } while(hascounter.next_bit(pos));
    if (!immediate_events.empty()) { 
      std::vector<std::pair<unsigned char, size_t> > cur;
      cur.swap(immediate_events);
      for (size_t i = 0;i < cur.size(); ++i) {
        (*out) << descriptions[cur[i].first] << ":\t" << cur[i].second << "\t" << -1 << "\t" << 0 << " /s\n";
      }
    }
    out->flush();
  }
  else if (print_method == LOG_FILE) {
    eventlog_file_mutex.lock();
    do {
      size_t ctrval = counters[pos].exchange(0);
      found_events = found_events || ctrval > 0;
      (*out) << descriptions[pos]  << ":\t" << curtime << "\t" << ctrval << "\t" << 1000 * ctrval / timegap << "\n";
    } while(hascounter.next_bit(pos));
    if (!immediate_events.empty()) { 
      std::vector<std::pair<unsigned char, size_t> > cur;
      cur.swap(immediate_events);
      for (size_t i = 0;i < cur.size(); ++i) {
        (*out) << descriptions[cur[i].first] << ":\t" << cur[i].second << "\t" << -1 << "\t" << 0 << " /s\n";
      }
    }
    eventlog_file_mutex.unlock();
    out->flush();
  }
  else if (print_method == RATE_BAR) {
    (*out) << "Time: " << "+"<<timegap << "\t" << curtime << "\n";
    char spacebuf[60];
    char pbuf[61];
    memset(spacebuf, ' ', EVENT_BAR_WIDTH);
    memset(pbuf, BAR_CHARACTER, 60);
    do {
      size_t ctrval = counters[pos].exchange(0);
      found_events = found_events || ctrval > 0;
      maxcounter[pos] = std::max(maxcounter[pos], ctrval);
      size_t barlen = 0;
      size_t mc = maxcounter[pos]; 
      if (mc > 0) barlen = ctrval * EVENT_BAR_WIDTH / mc;
      if (barlen > EVENT_BAR_WIDTH) barlen = EVENT_BAR_WIDTH;
      
      pbuf[barlen] = '\0';
      spacebuf[max_desc_length - descriptions[pos].length() + 1] = 0;
      (*out) << descriptions[pos]  << spacebuf << "|" << pbuf;
      spacebuf[max_desc_length - descriptions[pos].length() + 1] =' ';
      pbuf[barlen] = BAR_CHARACTER;
      // now print the remaining spaces
      spacebuf[EVENT_BAR_WIDTH - barlen] = '\0';
      (*out) << spacebuf << "| " << ctrval << " : " << mc << " \n";
      spacebuf[EVENT_BAR_WIDTH - barlen] = ' ';
      
    } while(hascounter.next_bit(pos));
    out->flush();
  }
  if (found_events == false) {
      ++noeventctr;
  }
  else {
      noeventctr = 0;
  }
  hasevents = false;

}

void event_log::thread_loop() {
  m.lock();
  while(!finished) {
    if (flush_interval == 0) {
      cond.wait(m);
    }
    else {
      m.unlock();
      my_sleep_ms(flush_interval);
      m.lock();
      if (flush_interval > 0) flush();
    }
  }
  m.unlock();
}

} // namespace
