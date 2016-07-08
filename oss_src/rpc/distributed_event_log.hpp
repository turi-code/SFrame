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


#ifndef GRAPHLAB_DISTRIBUTED_EVENT_LOG_HPP
#define GRAPHLAB_DISTRIBUTED_EVENT_LOG_HPP
#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <parallel/pthread_tools.hpp>
#include <parallel/atomic.hpp>
#include <timer/timer.hpp>
#include <util/dense_bitset.hpp>
#include <util/stl_util.hpp>
#include <rpc/dc_compile_parameters.hpp>
namespace graphlab {

// forward declaration because we need this in the
// class but we want dc_dist_object to be able
// to use this class too.
template <typename T>
class dc_dist_object;
class distributed_control;



const size_t MAX_LOG_SIZE = 32;
const size_t MAX_LOG_THREADS = 1024;
const double TICK_FREQUENCY = 0.5;
const double RECORD_FREQUENCY = 5.0;

/// A single entry in time
struct log_entry: public IS_POD_TYPE {
  // The value at the time. If this is a CUMULATIVE entry, this
  // will contain the total number of events since the start
  double value;

  explicit log_entry(double value = 0): value(value) { }
};


namespace log_type {
enum log_type_enum {
  INSTANTANEOUS = 0, ///< Sum of log values over time are not meaningful 
  CUMULATIVE = 1,    ///< Sum of log values over time are meaningful 
  AVERAGE = 2        ///< A meta event; this event is one event divided by another
};
}

/// Logging information for a particular log entry (say \#updates)
struct log_group{
  log_group() {
    earliest_modified_log = 0;
    machine_log_modified = false;
    callback = callback;
    is_callback_entry = false;
    sum_of_instantaneous_entries = 0.0;
    count_of_instantaneous_entries = 0;
    max_rate_bar_value = 0;
    numeratorid = 0;
    denominatorid = 0;
    max_rate_bar_value = 0;
  }
  mutex lock;

  /// name of the group
  std::string name;

  /// unit of measurement
  std::string units;

  /// Set to true if this is a callback entry
  bool is_callback_entry = false;

  /// The type of log. Instantaneous or Cumulative 
  log_type::log_type_enum logtype;

  boost::function<double(void)> callback;

  size_t sum_of_instantaneous_entries = 0;
  size_t count_of_instantaneous_entries = 0;

  bool machine_log_modified = 0;

  size_t earliest_modified_log = 0;
  double max_rate_bar_value; // only updated by emit_log_to_file

  // only used if this is an average event
  size_t numeratorid = 0;
  size_t denominatorid = 0;

  /// machine[i] holds a vector of entries from machine i
  std::vector<std::vector<log_entry> > machine;
  /// aggregate holds vector of totals
  std::vector<log_entry> aggregate;
};


/**
 * This is the type that is held in the thread local store
 */
struct event_log_thread_local_type {
  /** The values written to by each thread. 
   * An array with max length MAX_LOG_SIZE 
   */
  double values[MAX_LOG_SIZE];
  size_t thlocal_slot;

  // These are used for time averaging instantaneous values
};

class distributed_event_logger {
 public:
  enum distributed_event_log_print_type {
    DESCRIPTION,
    RATE_BAR
  };

 private:
  // a key to allow multiple threads, each to have their
  // own counter. Decreases performance penalty of the
  // the event logger.
  pthread_key_t key;

  dc_dist_object<distributed_event_logger>* rmi;

  // The array of logs. We can only have a maximum of MAX_LOG_SIZE logs
  // This is only created on machine 0
  log_group* logs[MAX_LOG_SIZE];

  // this bit field is used to identify which log entries are active
  fixed_dense_bitset<MAX_LOG_SIZE> has_log_entry;
  size_t num_log_entries = 0;
  mutex log_entry_lock;

  // A collection of slots, one for each thread, to hold 
  // the current thread's active log counter.
  // Threads will write directly into here
  // and a master timer will sum it all up periodically
  event_log_thread_local_type* thread_local_count[MAX_LOG_THREADS];
  // a bitset which lets me identify which slots in thread_local_counts
  // are used.
  fixed_dense_bitset<MAX_LOG_THREADS> thread_local_count_slots; 
  mutex thread_local_count_lock;

  // timer managing the frequency at which logs are transmitted to the root
  timer ti; 
  thread tick_thread;
  size_t record_ctr = 0;

  std::ostream* fileoutput = 0;
  mutex fileoutputlock;
  distributed_event_log_print_type fileoutput_mode;
  size_t fileoutput_last_written_event = 0;

  size_t allocate_log_entry(log_group* group);

  void* allocate_thr_specific_counter();
  /**
   * Returns a pointer to the current thread log counter
   * creating one if one does not already exist.
   */
  inline event_log_thread_local_type* get_thread_counter_ref() {
    void* v = pthread_getspecific(key);
    if (v == NULL) {
      v = allocate_thr_specific_counter();
      pthread_setspecific(key, v);
    }
    event_log_thread_local_type* entry = (event_log_thread_local_type*)(v);
    return entry;
  }

  /**
   * Receives the log information from each machine
   */    
  void rpc_collect_log(size_t srcproc, size_t record_ctr,
                       std::vector<double> srccounts);

  void collect_instantaneous_log(); 
  /** 
   *  Collects the machine level
   *  log entry. and sends it to machine 0
   */
  void local_collect_log(size_t record_ctr); 

  // Called only by machine 0 to get the aggregate log
  void build_aggregate_log();

  mutex periodic_timer_lock;
  conditional periodic_timer_cond;
  bool periodic_timer_stop;

  /** a new thread spawns here and sleeps for RECORD_FREQUENCY seconds at a time
   *  when it wakes up it will insert log entries
   */
  void periodic_timer();

  /**
   * If fileoutput is set writes the log to a file.
   */
  void emit_log_to_file();

  /**
   * If simple metrics export is requested, emits simple metrics
   */
  void emit_simple_metrics();


  /// called by the destruction of distributed_control
  void destroy_event_logger();

  static bool event_log_singleton_created;

  mutex simple_metrics_lock;
  std::set<std::string> simple_metrics_export;
  size_t simple_metrics_last_written_event = 0;
  size_t simple_metrics_base_event = 0;
 public:
  distributed_event_logger();
  ~distributed_event_logger();


  /**
   * \internal
   * Gets the singleton instance of the event log. 
   */
  static distributed_event_logger& get_instance();

  /**
   * \internal
   * Destroys the singleton instance of the event log, but without
   * deleting it. Do not use.
   */
  static void destroy_instance();

  /**
   * \internal
   * Deletes the singleton instance of the event log.
   * Do not use.
   */
  static void delete_instance();


  /**
   * Associates the event log with a DC object.
   * Must be called by all machines simultaneously.
   * Can be called more than once, but only the first call will have
   * an effect.
   */
  void set_dc(distributed_control& dc);
  /**
   * Creates a new log entry with a given name and log type.
   * Returns the ID of the log. Must be called by 
   * all machines simultaneously with the same settings.
   * units is the unit of measurement.
   */
  size_t create_log_entry(std::string name, std::string units, 
                          log_type::log_type_enum logtype);

  /**
   * Creates a new callback log entry with a given name and log type.
   * Returns the ID of the log. Must be called by 
   * all machines simultaneously with the same settings.
   * units is the unit of measurement.
   * Callback will be triggered periodically.
   * Callback entries must be deleted once the callback goes
   * out of scope.
   */
  size_t create_callback_entry(std::string name, 
                               std::string units,
                               boost::function<double(void)> callback,
                               log_type::log_type_enum logtype);

  /**
   * Creates a new event with a given name and units corresponding to 
   * dividing one event counter with another.
   */
  void create_average_event(size_t numeratorid,
                            size_t denominatorid,
                            std::string name,
                            std::string units);

  inline void clear_simple_metric_export() {
    simple_metrics_lock.lock();
    simple_metrics_export.clear();
    simple_metrics_lock.unlock();
  } 
 
  /**
   * Export a set of aggregate metrics to appear in the simple_metrics list
   * These will update once every RECORD_FREQUENCY seconds.
   */
  inline void export_metric_as_simple_metric(std::string metric_name) {
    simple_metrics_lock.lock();
    simple_metrics_export.insert(metric_name);
    simple_metrics_lock.unlock();
  }

  /**
   * This resets the simple_metrics time counter so that future simple metrics
   * insertions will start at 0.
   */
  inline void reset_simple_metric_time_counter() {
    simple_metrics_lock.lock();
    simple_metrics_last_written_event = record_ctr;
    simple_metrics_base_event = simple_metrics_last_written_event;
    simple_metrics_lock.unlock();
  }

  void free_callback_entry(size_t entry);

  /**
   * Increments the value of a log entry
   */
  inline void thr_inc_log_entry(size_t entry, double value) {
    event_log_thread_local_type* ev = get_thread_counter_ref();
    DASSERT_LT(entry, MAX_LOG_SIZE);
    DASSERT_EQ(logs[entry]->is_callback_entry, false);
    ev->values[entry] += value;
  }

  /**
   * Increments the value of a log entry
   */
  void thr_dec_log_entry(size_t entry, double value) {
    event_log_thread_local_type* ev = get_thread_counter_ref();
    DASSERT_LT(entry, MAX_LOG_SIZE);
    // does not work for cumulative logs
    DASSERT_NE((int)logs[entry]->logtype, (int) log_type::CUMULATIVE);
    DASSERT_EQ(logs[entry]->is_callback_entry, false);
    ev->values[entry] -= value;
  }

  /**
   * Also logs to a given output stream.
   * Only has effect on machine 0. 
   * The print option specifies the type of output to emit.
   *
   * - \b distributed_event_logger::DESCRIPTION : prints the log name, time, counter value and rate in rows.
   * - \b distributed_event_logger::RATE_BAR: Prints a bar per value.
   */
  void set_output_stream(std::ostream& os,
                         distributed_event_log_print_type print);

  /**
   * Stops outputing to the output stream
   */
  void clear_output_stream();

  /// \cond GRAPHLAB_INTERNAL
  inline double get_current_time() const {
    return ti.current_time();
  }

  inline log_group** get_logs_ptr() {
    return logs;
  }

  inline fixed_dense_bitset<MAX_LOG_SIZE>& get_logs_bitset() {
    return has_log_entry;
  }

  /// \endcond

};

} // namespace graphlab

#ifdef DISABLE_EVENT_LOG

#define DECLARE_EVENT(name) 

#define INITIALIZE_EVENT_LOG 
#define ADD_CUMULATIVE_EVENT(name, desc, units)

#define ADD_INSTANTANEOUS_EVENT(name, desc, units) 

#define ADD_CUMULATIVE_CALLBACK_EVENT(name, desc, units, callback) 


#define ADD_INSTANTANEOUS_CALLBACK_EVENT(name, desc, units, callback) 


#define ADD_AVERAGE_EVENT(numerator_name, denominator_name, desc, units) 

#define FREE_CALLBACK_EVENT(name) 

#define INCREMENT_EVENT(name, count) 
#define DECREMENT_EVENT(name, count)

#else

#define DECLARE_EVENT(name) size_t name;

#define INITIALIZE_EVENT_LOG graphlab::distributed_event_logger::get_instance().set_dc(*graphlab::distributed_control::get_instance());
#define ADD_CUMULATIVE_EVENT(name, desc, units) \
    name = graphlab::distributed_event_logger::get_instance().create_log_entry(desc, units, graphlab::log_type::CUMULATIVE);

#define ADD_INSTANTANEOUS_EVENT(name, desc, units) \
    name = graphlab::distributed_event_logger::get_instance().create_log_entry(desc, units, graphlab::log_type::INSTANTANEOUS);

#define ADD_CUMULATIVE_CALLBACK_EVENT(name, desc, units, callback) \
    name = graphlab::distributed_event_logger::get_instance().create_callback_entry(desc, units, callback, \
          graphlab::log_type::CUMULATIVE);


#define ADD_INSTANTANEOUS_CALLBACK_EVENT(name, desc, units, callback) \
    name = graphlab::distributed_event_logger::get_instance().create_callback_entry(desc, units, callback, \
           graphlab::log_type::INSTANTANEOUS);


#define ADD_AVERAGE_EVENT(numerator_name, denominator_name, desc, units) \
    graphlab::distributed_event_logger::get_instance().create_average_event(numerator_name, \
                                                   denominator_name, \
                                                   desc, units);



#define FREE_CALLBACK_EVENT(name) \
  graphlab::distributed_event_logger::get_instance().free_callback_entry(name);

#define INCREMENT_EVENT(name, count) graphlab::distributed_event_logger::get_instance().thr_inc_log_entry(name, count);
#define DECREMENT_EVENT(name, count) graphlab::distributed_event_logger::get_instance().thr_dec_log_entry(name, count);
#endif
#endif
