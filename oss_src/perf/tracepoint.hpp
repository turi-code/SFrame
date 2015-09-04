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


#ifndef GRAPHLAB_UTIL_TRACEPOINT_HPP
#define GRAPHLAB_UTIL_TRACEPOINT_HPP
#include <iostream>
#include <vector>
#include <string>
#include <timer/timer.hpp>
#include <util/branch_hints.hpp>
#include <parallel/atomic.hpp>
#include <parallel/atomic_ops.hpp>

namespace graphlab{

struct trace_count{
  std::string name;
  std::string description;
  bool print_on_destruct;
  graphlab::atomic<unsigned long long> count;
  graphlab::atomic<unsigned long long> total;
  unsigned long long minimum;
  unsigned long long maximum;
  inline trace_count(std::string name = "",
                    std::string description = "",
                    bool print_on_destruct = true):
                      name(name),
                      description(description),
                      print_on_destruct(print_on_destruct),
                      count(0),
                      total(0),
                      minimum(std::numeric_limits<unsigned long long>::max()),
                      maximum(0) { }

  /**
   * Initializes the tracer with a name, a description
   * and whether to print on destruction
   */
  inline void initialize(std::string n,
                  std::string desc,
                  bool print_out = true) {
    name = n;
    description = desc;
    print_on_destruct = print_out;
  }

  /**
   * Adds an event time to the trace
   */
  inline void incorporate(unsigned long long val)  __attribute__((always_inline)) {
    count.inc();
    total.inc(val);
    while(1) {
      unsigned long long m = minimum;
      if (__likely__(val > m || graphlab::atomic_compare_and_swap(minimum, m, val))) break;
    }
    while(1) {
      unsigned long long m = maximum;
      if (__likely__(val < m || graphlab::atomic_compare_and_swap(maximum, m, val))) break;
    }
  }
  
  /**
   * Adds the counts in a second tracer to the current tracer.
   */  
  inline void incorporate(const trace_count &val)  __attribute__((always_inline)) {
    count.inc(val.count.value);
    total.inc(val.total.value);
    while(1) {
      unsigned long long m = minimum;
      if (__likely__(val.minimum > m || graphlab::atomic_compare_and_swap(minimum, m, val.minimum))) break;
    }
    while(1) {
      unsigned long long m = maximum;
      if (__likely__(val.maximum < m || graphlab::atomic_compare_and_swap(maximum, m, val.maximum))) break;
    }
  }

  /**
   * Adds the counts in a second tracer to the current tracer.
   */
  inline trace_count& operator+=(trace_count &val) {
    incorporate(val);
    return *this;
  }

  /**
   * Destructor. Will print to cerr if initialize() is called
   * with "true" as the 3rd argument
   */
  ~trace_count();

  /**
   * Prints the tracer counts
   */
  void print(std::ostream& out, unsigned long long tpersec = 0) const;
};

} // namespace

/**
 * DECLARE_TRACER(name)
 * creates a tracing object with a given name. This creates a variable
 * called "name" which is of type trace_count. and is equivalent to:
 *
 * graphlab::trace_count name;
 * 
 * The primary reason to use this macro instead of just writing
 * the code above directly, is that the macro is ignored and compiles
 * to nothing when tracepoints are disabled.
 *
 *
 * INITIALIZE_TRACER(name, desc)
 * The object with name "name" created by DECLARE_TRACER must be in scope.
 * This initializes the tracer "name" with a description, and
 * configures the tracer to print when the tracer "name" is destroyed.
 *
 *
 * INITIALIZE_TRACER_NO_PRINT(name, desc)
 * The object with name "name" created by DECLARE_TRACER must be in scope.
 * This initializes the tracer "name" with a description, and
 * configures the tracer to NOT print when the tracer "name" is destroyed.
 *
 * BEGIN_TRACEPOINT(name)
 * END_TRACEPOINT(name)
 * The object with name "name" created by DECLARE_TRACER must be in scope.
 * Times a block of code. Every END_TRACEPOINT must be matched with a
 * BEGIN_TRACEPOINT within the same scope. Tracepoints are parallel.
 * 

  
Example Usage:
  DECLARE_TRACER(classname_someevent)
  INITIALIZE_TRACER(classname_someevent, "hello world");
  Then later on...
  BEGIN_TRACEPOINT(classname_someevent)
  ...
  END_TRACEPOINT(classname_someevent)
 *
*/


#ifdef USE_TRACEPOINT
#define DECLARE_TRACER(name) graphlab::trace_count name;

#define INITIALIZE_TRACER(name, description) name.initialize(#name, description);
#define INITIALIZE_TRACER_NO_PRINT(name, description) name.initialize(#name, description, false);

#define BEGIN_TRACEPOINT(name) unsigned long long __ ## name ## _trace_ = rdtsc();
#define END_TRACEPOINT(name) name.incorporate(rdtsc() - __ ## name ## _trace_);
#define END_AND_BEGIN_TRACEPOINT(endname, beginname) unsigned long long __ ## beginname ## _trace_ = rdtsc(); \
                                                     endname.incorporate(__ ## beginname ## _trace_ - __ ## endname ## _trace_);

#define CREATE_ACCUMULATING_TRACEPOINT(name) graphlab::trace_count __ ## name ## _acc_trace_; \
                                             unsigned long long __ ## name ## _acc_trace_elem_;
#define BEGIN_ACCUMULATING_TRACEPOINT(name) __ ## name ## _acc_trace_elem_ = rdtsc();
#define END_ACCUMULATING_TRACEPOINT(name) __ ## name ## _acc_trace_.incorporate(rdtsc() - __ ## name ## _acc_trace_elem_);

#define END_AND_BEGIN_ACCUMULATING_TRACEPOINT(endname, beginname) __ ## beginname ## _acc_trace_elem_ = rdtsc(); \
                                                                  __ ## endname ## _acc_trace_.incorporate(__ ## beginname ## _acc_trace_elem_ - __ ## endname ## _acc_trace_elem_)

#define STORE_ACCUMULATING_TRACEPOINT(name) name.incorporate(__ ## name ## _acc_trace_);
#else
#define DECLARE_TRACER(name)
#define INITIALIZE_TRACER(name, description)
#define INITIALIZE_TRACER_NO_PRINT(name, description) 

#define BEGIN_TRACEPOINT(name) 
#define END_TRACEPOINT(name) 

#define CREATE_ACCUMULATING_TRACEPOINT(name) 
#define BEGIN_ACCUMULATING_TRACEPOINT(name) 
#define END_ACCUMULATING_TRACEPOINT(name)
#define STORE_ACCUMULATING_TRACEPOINT(name)

#define END_AND_BEGIN_ACCUMULATING_TRACEPOINT(endname, beginname)
#define END_AND_BEGIN_TRACEPOINT(endname, beginname) 
#endif           

#endif
