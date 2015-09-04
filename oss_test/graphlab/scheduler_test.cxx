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


#include <fstream>
#include <vector>
#include <graphlab/scheduler/scheduler_includes.hpp>
#include <graphlab/rpc/async_consensus.hpp>
#include <graphlab/parallel/atomic.hpp>
#include <cxxtest/TestSuite.h>


using namespace graphlab;

distributed_control dc;

struct message_type {
  int value;
  double pr;

  explicit message_type(int value = 0, double pr = 0):value(value),pr(pr) { }
  
  double priority() const {
    return pr;
  }
  message_type& operator+=(const message_type& other) {
    value += other.value;
    pr = other.value;
    return *this;
  }
};


const size_t NCPUS = 4;
const size_t NUM_VERTICES = 101;
std::vector<atomic<int> > correctness_counter;


template <typename SchedulerType>
void test_scheduler_basic_functionality_single_threaded() {
  graphlab_options opts;
  opts.set_ncpus(NCPUS);
  SchedulerType sched(NUM_VERTICES, opts);
  const size_t target_value = 100;
  
  // inject a sequence of messages which will sum to 100 per vertex
  for (size_t c = 0;c < target_value; ++c) {
    for (size_t i = 0; i < NUM_VERTICES; ++i) {
      sched.schedule(i, message_type(1, 1.0));
    }
  }
  correctness_counter.clear();
  correctness_counter.resize(NUM_VERTICES, atomic<int>(0));
  sched.start();
  
  // pull stuff out
  bool allcpus_done = false; 
  while(!allcpus_done) {
    allcpus_done = true;
    for (size_t i = 0; i < NCPUS; ++i) {
      vertex_id_type v; message_type m;
      sched_status::status_enum ret = sched.get_next(i, v, m);
      if (ret == sched_status::NEW_TASK) {
        allcpus_done = false;
        correctness_counter[v].inc(m.value);
      }
    }
  }

  // check the counters
  for(size_t i = 0; i < NUM_VERTICES; ++i) {
    TS_ASSERT_EQUALS(correctness_counter[i].value, (int)target_value);
  }
}





template <typename SchedulerType>
void test_basic_functionality_thread(SchedulerType& sched, 
                                     async_consensus& consensus, 
                                     size_t schedule_count,
                                     size_t threadid) {
  size_t c = 0;
  vertex_id_type v; message_type m;
  while(1) {
    // process as many tasks as I can
    while(1) {
      sched_status::status_enum ret = sched.get_next(threadid, v, m);
      if (ret == sched_status::NEW_TASK) {
        correctness_counter[v].inc(m.value);
      }
      else {
        break;
      }
    }
    
    // schedule 1 cycle. If I schedule stuff I go back to processing tasks
    if (c < schedule_count) {
      for (size_t i = 0; i < NUM_VERTICES; ++i) {
        sched.schedule(i, message_type(1, 1.0));
        consensus.cancel();
      }
      ++c;
      continue;
    }
    
    // nothing to schedule, nothing to run. try to quit
    consensus.begin_done_critical_section(threadid);
    sched_status::status_enum ret = sched.get_next(threadid, v, m);
    if (ret == sched_status::NEW_TASK) {
      // there is task. cancel, process it, and look back
      consensus.cancel_critical_section(threadid);
      correctness_counter[v].inc(m.value);
    }
    else {
      // no more tasks try to finish up
      bool ret = consensus.end_done_critical_section(threadid);
      if (ret) break;
    }
  }
}


template <typename SchedulerType>
void test_scheduler_basic_functionality_parallel() {
  graphlab_options opts;
  opts.set_ncpus(NCPUS);
  SchedulerType sched(NUM_VERTICES, opts);
  async_consensus consensus(dc, NCPUS);
  
  const size_t schedule_count = 10000;
  const size_t target_value = schedule_count * NCPUS + 1;

  correctness_counter.clear();
  correctness_counter.resize(NUM_VERTICES, atomic<int>(0));

  // inject a sequence of messages which will sum to 100 per vertex
  for (size_t i = 0; i < NUM_VERTICES; ++i) {
    sched.schedule(i, message_type(1, 1.0));
  }
  sched.start();
  
  thread_group group;
  for (size_t i = 0;i < NCPUS;++i) {
    group.launch(boost::bind(test_basic_functionality_thread<SchedulerType>,
                             boost::ref(sched), boost::ref(consensus), schedule_count, i));
  }

  group.join();
  // check the counters
  for(size_t i = 0; i < NUM_VERTICES; ++i) {
    TS_ASSERT_EQUALS(correctness_counter[i].value, (int)target_value);
  }
}







/*
 * Like test_basic_functionality_thread, but only increments the 
 * correctness_counter by 1, and checks to make sure the priority is at least
 * 100.0
 */
template <typename SchedulerType>
void test_scheduler_min_priority_thread(SchedulerType& sched, 
                                        async_consensus& consensus, 
                                        size_t schedule_count,
                                        size_t threadid) {
  size_t c = 0;
  vertex_id_type v; message_type m;
  while(1) {
    // process as many tasks as I can
    while(1) {
      sched_status::status_enum ret = sched.get_next(threadid, v, m);
      if (ret == sched_status::NEW_TASK) {
        TS_ASSERT_LESS_THAN_EQUALS(100.0, m.priority());
        correctness_counter[v].inc(1);
      }
      else {
        break;
      }
    }
    
    // schedule 1 cycle. If I schedule stuff I go back to processing tasks
    if (c < schedule_count) {
      for (size_t i = 0; i < NUM_VERTICES; ++i) {
        sched.schedule(i, message_type(1, 1.0));
        consensus.cancel();
      }
      ++c;
      continue;
    }
    
    // nothing to schedule, nothing to run. try to quit
    consensus.begin_done_critical_section(threadid);
    sched_status::status_enum ret = sched.get_next(threadid, v, m);
    if (ret == sched_status::NEW_TASK) {
      TS_ASSERT_LESS_THAN_EQUALS(100.0, m.priority());
      // there is task. cancel, process it, and look back
      consensus.cancel_critical_section(threadid);
      correctness_counter[v].inc(1);
    }
    else {
      // no more tasks try to finish up
      bool ret = consensus.end_done_critical_section(threadid);
      if (ret) break;
    }
  }
}


template <typename SchedulerType>
void test_scheduler_min_priority_parallel() {
  graphlab_options opts;
  opts.set_ncpus(NCPUS);
  opts.get_scheduler_args().set_option("min_priority", 100.0);
  
  SchedulerType sched(NUM_VERTICES, opts);
  async_consensus consensus(dc, NCPUS);
  
  const size_t schedule_count = 10000;
  const size_t maximum_value = (size_t)((schedule_count * NCPUS + 101.0) / 100.0);

  correctness_counter.clear();
  correctness_counter.resize(NUM_VERTICES, atomic<int>(0));

  // inject a sequence of messages which will sum to 100 per vertex
  for (size_t i = 0; i < NUM_VERTICES; ++i) {
    sched.schedule(i, message_type(1, 101.0));
  }
  sched.start();
  
  thread_group group;
  for (size_t i = 0;i < NCPUS;++i) {
    group.launch(boost::bind(test_scheduler_min_priority_thread<SchedulerType>,
                             boost::ref(sched), boost::ref(consensus), schedule_count, i));
  }

  group.join();
  // check the counters
  for(size_t i = 0; i < NUM_VERTICES; ++i) {
    TS_ASSERT_LESS_THAN_EQUALS(correctness_counter[i].value, (int)maximum_value);
  }
}

class SerializeTestSuite : public CxxTest::TestSuite {
public:
  void test_scheduler_basic_single_threaded() {
    test_scheduler_basic_functionality_single_threaded<sweep_scheduler<message_type> >();
    test_scheduler_basic_functionality_single_threaded<fifo_scheduler<message_type> >();
    test_scheduler_basic_functionality_single_threaded<priority_scheduler<message_type> >();
    test_scheduler_basic_functionality_single_threaded<queued_fifo_scheduler<message_type> >();
  }
  
  void test_scheduler_basic_parallel() {
    test_scheduler_basic_functionality_parallel<sweep_scheduler<message_type> >();
    test_scheduler_basic_functionality_parallel<fifo_scheduler<message_type> >();
    test_scheduler_basic_functionality_parallel<priority_scheduler<message_type> >();
    test_scheduler_basic_functionality_parallel<queued_fifo_scheduler<message_type> >();
  }
  
    
  void test_scheduler_min_priority() {
    test_scheduler_min_priority_parallel<sweep_scheduler<message_type> >();
    test_scheduler_min_priority_parallel<fifo_scheduler<message_type> >();
    test_scheduler_min_priority_parallel<priority_scheduler<message_type> >();
    test_scheduler_min_priority_parallel<queued_fifo_scheduler<message_type> >();
  }

};

