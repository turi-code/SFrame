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


#include <unistd.h>
#include <sys/param.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#include <map>
#include <sstream>

#include <boost/unordered_map.hpp>
#include <boost/bind.hpp>
#include <logger/assertions.hpp>
#include <parallel/pthread_tools.hpp>
#include <util/stl_util.hpp>
#include <network/net_util.hpp>
#include <rpc/mpi_tools.hpp>

#include <rpc/dc.hpp>
#include <rpc/dc_tcp_comm.hpp>
//#include <rpc/dc_sctp_comm.hpp>
#include <rpc/dc_buffered_stream_send2.hpp>
#include <rpc/dc_stream_receive.hpp>
#include <rpc/request_reply_handler.hpp>
#include <rpc/dc_services.hpp>

#include <rpc/dc_init_from_env.hpp>
#include <rpc/dc_init_from_mpi.hpp>
#include <rpc/dc_init_from_zookeeper.hpp>
#include <rpc/dc_registry.hpp>
#include <rpc/dc_global.hpp>


namespace graphlab {

namespace dc_impl {


bool thrlocal_sequentialization_key_initialized = false;
pthread_key_t thrlocal_sequentialization_key;

bool thrlocal_send_buffer_key_initialized = false;
pthread_key_t thrlocal_send_buffer_key;

void thrlocal_send_buffer_key_deleter(void* p) {
  if (p != NULL) {
    thread_local_buffer* buf = (thread_local_buffer*)(p);
    if (buf != NULL) {
      delete buf;
    } 
  }
}

} // namespace dc_impl




procid_t distributed_control::get_instance_procid() {
  return distributed_control_global::get_instance_procid();
}

distributed_control* distributed_control::get_instance() {
  return distributed_control_global::get_instance();
}




unsigned char distributed_control::set_sequentialization_key(unsigned char newkey) {
  size_t oldval = reinterpret_cast<size_t>(pthread_getspecific(dc_impl::thrlocal_sequentialization_key));
  size_t newval = newkey;
  pthread_setspecific(dc_impl::thrlocal_sequentialization_key, reinterpret_cast<void*>(newval));
  assert(oldval < 256);
  return (unsigned char)oldval;
}

unsigned char distributed_control::new_sequentialization_key() {
  size_t oldval = reinterpret_cast<size_t>(pthread_getspecific(dc_impl::thrlocal_sequentialization_key));
  size_t newval = (oldval + 1) % 256;
  pthread_setspecific(dc_impl::thrlocal_sequentialization_key, reinterpret_cast<void*>(newval));
  assert(oldval < 256);
  return (unsigned char)oldval;
}

unsigned char distributed_control::get_sequentialization_key() {
  size_t oldval = reinterpret_cast<size_t>(pthread_getspecific(dc_impl::thrlocal_sequentialization_key));
  assert(oldval < 256);
  return (unsigned char)oldval;
}


distributed_control::distributed_control() {
  dc_init_param initparam;
  if (init_param_from_env(initparam)) {
    logstream(LOG_INFO) << "Distributed Control Initialized from Environment" << std::endl;
  } else if (init_param_from_zookeeper(initparam)) {
      logstream(LOG_INFO) << "Distributed Control Initialized from Zookeeper" << std::endl;
  } else if (mpi_tools::initialized() && init_param_from_mpi(initparam)) {
      logstream(LOG_INFO) << "Distributed Control Initialized from MPI" << std::endl;
  }
  else {
    logstream(LOG_INFO) << "Shared Memory Execution" << std::endl;
    // get a port and socket
    std::pair<size_t, int> port_and_sock = get_free_tcp_port();
    size_t port = port_and_sock.first;
    int sock = port_and_sock.second;

    initparam.machines.push_back(std::string("localhost:") + tostr(port));
    initparam.curmachineid = 0;
    initparam.initstring = std::string(" __sockhandle__=") + tostr(sock) + " ";
    initparam.numhandlerthreads = RPC_DEFAULT_NUMHANDLERTHREADS;
    initparam.commtype = RPC_DEFAULT_COMMTYPE;
  }
  init(initparam.machines,
        initparam.initstring,
        initparam.curmachineid,
        initparam.numhandlerthreads,
        initparam.commtype,
        initparam.is_singleton);
  INITIALIZE_TRACER(dc_receive_queuing, "dc: time spent on enqueue");
  INITIALIZE_TRACER(dc_receive_multiplexing, "dc: time spent exploding a chunk");
  INITIALIZE_TRACER(dc_call_dispatch, "dc: time spent issuing RPC calls");
}

distributed_control::distributed_control(dc_init_param initparam) {
  init(initparam.machines,
        initparam.initstring,
        initparam.curmachineid,
        initparam.numhandlerthreads,
        initparam.commtype,
        initparam.is_singleton);
  INITIALIZE_TRACER(dc_receive_queuing, "dc: time spent on enqueue");
  INITIALIZE_TRACER(dc_receive_multiplexing, "dc: time spent exploding a chunk");
  INITIALIZE_TRACER(dc_call_dispatch, "dc: time spent issuing RPC calls");
}


distributed_control::~distributed_control() {
  distributed_control_global::get_instance() = NULL;
  distributed_control_global::get_instance_procid() = 0;
  logstream(LOG_INFO) << "dc destructor full barrier " << procid() << std::endl;
  distributed_services->full_barrier();
  logstream(LOG_INFO) << "dc destructor full barrier complete" << std::endl;
  comm->expect_close();
  distributed_services->barrier();
  if (is_singleton) {
    FREE_CALLBACK_EVENT(EVENT_NETWORK_BYTES);
    FREE_CALLBACK_EVENT(EVENT_RPC_CALLS);
  }

  size_t bytessent = bytes_sent();
  for (size_t i = 0;i < senders.size(); ++i) {
    senders[i]->flush();
  }

  comm->close();

  for (size_t i = 0;i < senders.size(); ++i) {
    delete senders[i];
  }
  senders.clear();

  pthread_key_delete(dc_impl::thrlocal_sequentialization_key);
  pthread_key_delete(dc_impl::thrlocal_send_buffer_key);

  size_t bytesreceived = bytes_received();
  for (size_t i = 0;i < receivers.size(); ++i) {
    receivers[i]->shutdown();
    delete receivers[i];
  }
  receivers.clear();
  // shutdown function call handlers
  for (size_t i = 0;i < fcallqueue.size(); ++i) fcallqueue[i].stop_blocking();
  fcallhandlers.join();
  logstream(LOG_INFO) << "Bytes Sent: " << bytessent << std::endl;
  logstream(LOG_INFO) << "Calls Sent: " << calls_sent() << std::endl;
  logstream(LOG_INFO) << "Network Sent: " << network_bytes_sent() << std::endl;
  logstream(LOG_INFO) << "Bytes Received: " << bytesreceived << std::endl;
  logstream(LOG_INFO) << "Calls Received: " << calls_received() << std::endl;

  // clear all callback
  if (is_singleton) {
    while (fiber_control::get_instance().num_active_workers()) {
      usleep(1e4); // 10 miliseconds
    }
    fiber_control::get_instance().set_fiber_exit_callback(NULL);
    fiber_control::get_instance().set_context_switch_periodic_callback(NULL);
    fiber_control::get_instance().set_context_switch_callback(NULL);
  }

  delete comm;

}


void distributed_control::exec_function_call(procid_t source,
                                            unsigned char packet_type_mask,
                                            const char* data,
                                            const size_t len) {
  BEGIN_TRACEPOINT(dc_call_dispatch);
  // extract the dispatch function
  iarchive arc(data, len);
  function_dispatch_id_type f;
  arc >> f;
  // a regular funcion call
  dc_impl::dispatch_type dispatch = dc_impl::get_from_function_registry<dc_impl::dispatch_type>(f);
  dispatch(*this, source, packet_type_mask, data + arc.off, len - arc.off);
  if ((packet_type_mask & CONTROL_PACKET) == 0) inc_calls_received(source);
  END_TRACEPOINT(dc_call_dispatch);
}

unsigned char distributed_control::get_block_sequentialization_key(fcallqueue_entry& fcallblock) {
  unsigned char seq_key = 0;
  char* data = fcallblock.chunk_src;
  size_t remaininglen = fcallblock.chunk_len;
  // loop through all the messages
  while(remaininglen > 0) {
    ASSERT_GE(remaininglen, sizeof(dc_impl::packet_hdr));
    dc_impl::packet_hdr hdr = *reinterpret_cast<dc_impl::packet_hdr*>(data);
    ASSERT_LE(hdr.len, remaininglen);
    if (hdr.sequentialization_key != 0) {
      seq_key = hdr.sequentialization_key;
      break;
    }
    data += sizeof(dc_impl::packet_hdr) + hdr.len;
    remaininglen -= sizeof(dc_impl::packet_hdr) + hdr.len;
  }
  return seq_key;
}

void distributed_control::deferred_function_call_chunk(char* buf, size_t len, procid_t src) {
  BEGIN_TRACEPOINT(dc_receive_queuing);
  fcallqueue_entry* fc = new fcallqueue_entry;
  fc->chunk_src = buf;
  fc->chunk_len = len;
  fc->chunk_ref_counter = NULL;
  fc->is_chunk = true;
  fc->source = src;
  fcallqueue_length.inc();

#ifdef RPC_BLOCK_STRIPING
  static size_t __idx;
  // approximate balancing
  size_t idx = __idx++ % fcallqueue.size();
  fcallqueue[idx].enqueue(fc, !fcall_handler_blockers.get(idx));
#else
  idx = src % fcallqueue.size();
  fcallqueue[idx].enqueue(fc, !fcall_handler_blockers.get(idx));
#endif
/*
  if (get_block_sequentialization_key(*fc) > 0) {
    fcallqueue[src % fcallqueue.size()].enqueue(fc);
  } else {
    const uint32_t prod = 
        random::fast_uniform(uint32_t(0), 
                             uint32_t(fcallqueue.size() * fcallqueue.size() - 1));
    const uint32_t r1 = prod / fcallqueue.size();
    const uint32_t r2 = prod % fcallqueue.size();
    uint32_t idx = (fcallqueue[r1].size() < fcallqueue[r2].size()) ? r1 : r2;  
    fcallqueue[idx].enqueue(fc);
  } */

//   const uint32_t prod = 
//       random::fast_uniform(uint32_t(0), 
//                            uint32_t(fcallqueue.size() * fcallqueue.size() - 1));
//   const uint32_t r1 = prod / fcallqueue.size();
//   const uint32_t r2 = prod % fcallqueue.size();
//   uint32_t idx = (fcallqueue[r1].size() < fcallqueue[r2].size()) ? r1 : r2;  
//   fcallqueue[idx].enqueue(fc);
//   END_TRACEPOINT(dc_receive_queuing);
}


void distributed_control::process_fcall_block(fcallqueue_entry &fcallblock) {
  if (fcallblock.is_chunk == false) {
    for (size_t i = 0;i < fcallblock.calls.size(); ++i) {
      fcallqueue_length.dec();
      exec_function_call(fcallblock.source, fcallblock.calls[i].packet_mask,
                        fcallblock.calls[i].data, fcallblock.calls[i].len);
    }
    if (fcallblock.chunk_ref_counter != NULL) {
      if (fcallblock.chunk_ref_counter->dec(fcallblock.calls.size()) == 0) {
        delete fcallblock.chunk_ref_counter;
        free(fcallblock.chunk_src);
      }
    }
  }
#ifdef RPC_DO_NOT_BREAK_BLOCKS
  else {
    fcallqueue_length.dec();

    //parse the data in fcallblock.data
    char* data = fcallblock.chunk_src;
    size_t remaininglen = fcallblock.chunk_len;
    //PERMANENT_ACCUMULATE_DIST_EVENT(eventlog, BYTES_EVENT, remaininglen);
    while(remaininglen > 0) {
      ASSERT_GE(remaininglen, sizeof(dc_impl::packet_hdr));
      dc_impl::packet_hdr hdr = *reinterpret_cast<dc_impl::packet_hdr*>(data);
      ASSERT_LE(hdr.len, remaininglen);

      if (!(hdr.packet_type_mask & CONTROL_PACKET)) {
        global_bytes_received[hdr.src].inc(hdr.len);
      }

      exec_function_call(fcallblock.source, hdr.packet_type_mask,
                         data + sizeof(dc_impl::packet_hdr),
                         hdr.len);
      data += sizeof(dc_impl::packet_hdr) + hdr.len;
      remaininglen -= sizeof(dc_impl::packet_hdr) + hdr.len;
    }
    free(fcallblock.chunk_src);
  }
#else
  else {
    fcallqueue_length.dec();
    BEGIN_TRACEPOINT(dc_receive_multiplexing);
    fcallqueue_entry* queuebufs[fcallqueue.size()];
    atomic<size_t>* refctr = new atomic<size_t>(0);

    fcallqueue_entry immediate_queue;

    immediate_queue.chunk_src = fcallblock.chunk_src;
    immediate_queue.chunk_ref_counter = refctr;
    immediate_queue.chunk_len = 0;
    immediate_queue.source = fcallblock.source;
    immediate_queue.is_chunk = false;

    for (size_t i = 0;i < fcallqueue.size(); ++i) {
      queuebufs[i] = new fcallqueue_entry;
      queuebufs[i]->chunk_src = fcallblock.chunk_src;
      queuebufs[i]->chunk_ref_counter = refctr;
      queuebufs[i]->chunk_len = 0;
      queuebufs[i]->source = fcallblock.source;
      queuebufs[i]->is_chunk = false;
    }

    //parse the data in fcallblock.data
    char* data = fcallblock.chunk_src;
    size_t remaininglen = fcallblock.chunk_len;
    //PERMANENT_ACCUMULATE_DIST_EVENT(eventlog, BYTES_EVENT, remaininglen);
    size_t stripe = 0;
    while(remaininglen > 0) {
      ASSERT_GE(remaininglen, sizeof(dc_impl::packet_hdr));
      dc_impl::packet_hdr hdr = *reinterpret_cast<dc_impl::packet_hdr*>(data);
      ASSERT_LE(hdr.len, remaininglen);

      refctr->value++;


      if ((hdr.packet_type_mask & CONTROL_PACKET)) {
        // control calls are handled immediately with priority.
        immediate_queue.calls.push_back(function_call_block(
                                            data + sizeof(dc_impl::packet_hdr),
                                            hdr.len,
                                            hdr.packet_type_mask));
      } else {
        global_bytes_received[hdr.src].inc(hdr.len);
        if (hdr.sequentialization_key == 0) {
          queuebufs[stripe]->calls.push_back(function_call_block(
                                              data + sizeof(dc_impl::packet_hdr),
                                              hdr.len,
                                              hdr.packet_type_mask));
          ++stripe;
          if (stripe == (fcallblock.source % fcallqueue.size())) ++stripe;
          if (stripe >= fcallqueue.size()) stripe -= fcallqueue.size();
        }
        else {
          size_t idx = (hdr.sequentialization_key % (fcallqueue.size()));
          queuebufs[idx]->calls.push_back(function_call_block(
                                              data + sizeof(dc_impl::packet_hdr),
                                              hdr.len,
                                              hdr.packet_type_mask));
        }
      }
      data += sizeof(dc_impl::packet_hdr) + hdr.len;
      remaininglen -= sizeof(dc_impl::packet_hdr) + hdr.len;
    }
    END_TRACEPOINT(dc_receive_multiplexing);
    BEGIN_TRACEPOINT(dc_receive_queuing);
    for (size_t i = 0;i < fcallqueue.size(); ++i) {
      if (queuebufs[i]->calls.size() > 0) {
        fcallqueue_length.inc(queuebufs[i]->calls.size());
        fcallqueue[i].enqueue(queuebufs[i]);
      }
      else {
        delete queuebufs[i];
      }
    }
    END_TRACEPOINT(dc_receive_queuing);
    if (immediate_queue.calls.size() > 0) process_fcall_block(immediate_queue);
  }
#endif
}

void distributed_control::stop_handler_threads(size_t threadid,
                                                size_t total_threadid) {
  stop_handler_threads_no_wait(threadid, total_threadid);
}

void distributed_control::stop_handler_threads_no_wait(size_t threadid,
                                                       size_t total_threadid) {
  for (size_t i = threadid;i < fcallqueue.size(); i += total_threadid) {
    fcall_handler_blockers.set_bit(i);
  }
}


void distributed_control::start_handler_threads(size_t threadid,
                                                size_t total_threadid) {
  for (size_t i = threadid;i < fcallqueue.size(); i += total_threadid) {
    fcall_handler_blockers.clear_bit(i);
    fcallqueue[i].broadcast();
  }
}

void distributed_control::handle_incoming_calls(size_t threadid,
                                                size_t total_threadid) {
  for (size_t i = threadid;i < fcallqueue.size(); i += total_threadid) {
    if (fcallqueue[i].empty_unsafe() == false) {
      std::deque<fcallqueue_entry*> q;
      fcallqueue[i].swap(q);
      while (!q.empty()) {
        fcallqueue_entry* entry;
        entry = q.front();
        q.pop_front();

        process_fcall_block(*entry);
        delete entry;
      }
    }
  }
}

void distributed_control::fcallhandler_loop(size_t id) {
  // pop an element off the queue
//  float t = timer::approx_time_seconds();
  fcall_handler_active[id].inc();
  // std::cerr << "Handler " << id << " Started." << std::endl;
  while(fcallqueue[id].is_alive()) {
    fcallqueue[id].wait_for_data();
    std::deque<fcallqueue_entry*> q;
    fcallqueue[id].swap(q);
    while (!q.empty()) {
      fcallqueue_entry* entry;
      entry = q.front();
      q.pop_front();

      process_fcall_block(*entry);
      delete entry;
    }
  }
  // std::cerr << "Handler " << id << " died." << std::endl;
  fcall_handler_active[id].dec();
}


std::map<std::string, std::string>
  distributed_control::parse_options(std::string initstring) {
  std::map<std::string, std::string> options;
  std::replace(initstring.begin(), initstring.end(), ',', ' ');
  std::replace(initstring.begin(), initstring.end(), ';', ' ');
  std::string opt, value;
  // read till the equal
  std::stringstream s(initstring);
  while(s.good()) {
    getline(s, opt, '=');
    if (s.bad() || s.eof()) break;
    getline(s, value, ' ');
    if (s.bad()) break;
    options[trim(opt)] = trim(value);
  }
  return options;
}

void distributed_control::init(const std::vector<std::string> &machines,
            const std::string &initstring,
            procid_t curmachineid,
            size_t numhandlerthreads,
            dc_comm_type commtype,
            bool is_singleton) {

  this->is_singleton = is_singleton;

  if (numhandlerthreads == RPC_DEFAULT_NUMHANDLERTHREADS) {
    // autoconfigure
    if (thread::cpu_count() > 2) numhandlerthreads = thread::cpu_count() - 2;
    else numhandlerthreads = 2;
  }

  ASSERT_MSG(distributed_control_global::get_instance() == NULL,
      "Only one dc object can be constructed at any time");
  // set the value of the last_dc for the get_instance function
  ASSERT_MSG(machines.size() <= RPC_MAX_N_PROCS,
             "Number of processes exceeded hard limit of %d", RPC_MAX_N_PROCS);

  // initialize thread local storage
  if (dc_impl::thrlocal_sequentialization_key_initialized == false) {
    dc_impl::thrlocal_sequentialization_key_initialized = true;
    int err = pthread_key_create(&dc_impl::thrlocal_sequentialization_key, NULL);
    ASSERT_EQ(err, 0);
  }

  if (dc_impl::thrlocal_send_buffer_key_initialized == false) {
    dc_impl::thrlocal_send_buffer_key = true;
    int err = pthread_key_create(&dc_impl::thrlocal_send_buffer_key, dc_impl::thrlocal_send_buffer_key_deleter);
    ASSERT_EQ(err, 0);
  }

  //-------- Initialize the full barrier ---------
  full_barrier_in_effect = false;
  procs_complete.resize(machines.size());
  //-----------------------------------------------

  // initialize the counters

  global_calls_sent.resize(machines.size());
  global_calls_received.resize(machines.size());
  global_bytes_received.resize(machines.size());
  fcallqueue.resize(numhandlerthreads);

  // options
  set_fast_track_requests(true);

  // parse the initstring
  std::map<std::string,std::string> options = parse_options(initstring);

  if (commtype == TCP_COMM) {
    comm = new dc_impl::dc_tcp_comm();
  } else {
    ASSERT_MSG(false, "Unexpected value for comm type");
  }
  for (procid_t i = 0; i < machines.size(); ++i) {
    receivers.push_back(new dc_impl::dc_stream_receive(this, i));
    senders.push_back(new dc_impl::dc_buffered_stream_send2(this, comm, i));
  }

  // Get the thread local index for the dc object
  size_t current_dc_idx = distributed_control_global::get_current_dc_idx();

  if (is_singleton) {
    // set up all the callbacks
    // whenever a fiber exits, we need to flush all buffers in case the fiber
    // left stuff sitting in a thread local buffer.
    fiber_control::get_instance().set_fiber_exit_callback(
        [=](size_t) {
          distributed_control* dc = distributed_control_global::get_instance(current_dc_idx);
          if (dc) dc->flush();
        });

    // every so often we request for a thread local flush
    fiber_control::get_instance().set_context_switch_periodic_callback(
        [=](size_t) {
          distributed_control* dc = distributed_control_global::get_instance(current_dc_idx);
          if (dc) dc->flush_soon();
        });

    // we handle incoming calls aggressively whenever possible
    fiber_control::get_instance().set_context_switch_callback(
        [=](size_t workerid) {
          distributed_control* dc = distributed_control_global::get_instance(current_dc_idx);
          if (dc && workerid < dc->num_handler_threads()) {
            dc->handle_incoming_calls(workerid, dc->num_handler_threads());
          }
        });
  }


  // create the handler threads
  // store the threads in the threadgroup
  fcall_handler_active.resize(numhandlerthreads);
  fcall_handler_blockers.resize(numhandlerthreads);
  fcallhandlers.set_stacksize(256*1024); // 256K
  for (size_t i = 0;i < numhandlerthreads; ++i) {
    fiber_control::affinity_type affinity;
    affinity.clear();
    affinity.set_bit(i);
    fcallhandlers.launch(boost::bind(&distributed_control::fcallhandler_loop,
                                      this, i), affinity);
  }

  // wait for all the handlers to startup
  {
    size_t num_started = 0;
    while(num_started != fcall_handler_active.size()) {
      num_started = 0;
      for (size_t i = 0;i < fcall_handler_active.size(); ++i) {
        num_started += fcall_handler_active[i].value;
      }
    }
    if (num_started != fcall_handler_active.size()) timer::sleep_ms(100);
  }

  // set the local proc values
  localprocid = curmachineid;
  localnumprocs = machines.size();

  // construct the services
  distributed_services = new dc_services(*this);
  // start the machines

  // improves reliability of initialization
#ifdef HAS_MPI
  if (mpi_tools::initialized()) MPI_Barrier(MPI_COMM_WORLD);
#endif

  comm->init(machines, options, curmachineid,
              receivers, senders);
  logstream(LOG_INFO) << "TCP Communication layer constructed." << std::endl;
  if (localprocid == 0) {
    logstream(LOG_EMPH) << "Cluster of " << machines.size() << " instances created." << std::endl;
    // check for duplicate IP addresses
    std::map<std::string, size_t> ipaddresses;
    for (size_t i = 0; i < machines.size(); ++i ){
      size_t pos = machines[i].find(":");
      ASSERT_NE(pos, std::string::npos);
      std::string address = machines[i].substr(0, pos);
      ipaddresses[address]++;
    }
    bool hasduplicate = false;
    std::map<std::string, size_t>::const_iterator iter = ipaddresses.begin();
    while (iter != ipaddresses.end()) {
      if (iter->second > 1) {
        hasduplicate = true;
        logstream(LOG_WARNING) << "Duplicate IP address: " << iter->first << std::endl;
      }
      ++iter;
    }
    if (hasduplicate) {
      logstream(LOG_WARNING) << "For maximum performance, GraphLab strongly prefers running just one process per machine." << std::endl;
    }
  }

  // set the static variable for the get_instance_procid() function
  distributed_control_global::set_current_dc(this, localprocid);

  // improves reliability of initialization
#ifdef HAS_MPI
  if (mpi_tools::initialized()) MPI_Barrier(MPI_COMM_WORLD);
#endif

  barrier();

  logstream(LOG_DEBUG) << "dc init barrier complete" << std::endl;

  // initialize the empty stream
  nullstrm.open(boost::iostreams::null_sink());

  // initialize the event log

  if (is_singleton) {
    INITIALIZE_EVENT_LOG;
    ADD_CUMULATIVE_CALLBACK_EVENT(EVENT_NETWORK_BYTES, "Network Utilization",
        "MB", boost::bind(&distributed_control::network_megabytes_sent, this));
    ADD_CUMULATIVE_CALLBACK_EVENT(EVENT_RPC_CALLS, "RPC Calls",
        "Calls", boost::bind(&distributed_control::calls_sent, this));
  }
}



void distributed_control::barrier() {
  distributed_services->barrier();
}

void distributed_control::flush() {
  for (procid_t i = 0;i < senders.size(); ++i) {
    senders[i]->flush();
  }
}

void distributed_control::flush(procid_t p) {
  senders[p]->flush();
}

void distributed_control::flush_soon() {
  for (procid_t i = 0;i < senders.size(); ++i) {
    senders[i]->flush_soon();
  }
}


void distributed_control::flush_soon(procid_t p) {
  senders[p]->flush_soon();
}
/*****************************************************************************
                      Implementation of Full Barrier
*****************************************************************************/
/* It is unfortunate but this is copy paste code from dc_dist_object.hpp
  I thought for a long time how to implement this without copy pasting and
  I can't think of a simple enough solution.

  Part of the issue is that the "context" concept was not built into to the
  RPC system to begin with and is currently folded in through the dc_dist_object system.
  As a result, the global context becomes very hard to define properly.
  Including a dc_dist_object as a member only resolves the high level contexts
  such as barrier, broadcast, etc which do not require intrusive access into
  deeper information about the context. The full barrier however, requires deep
  information about the context which cannot be resolved easily.
*/

/**
This barrier ensures globally across all machines that
all calls issued prior to this barrier are completed before
returning. This function could return prematurely if
other threads are still issuing function calls since we
cannot differentiate between calls issued before the barrier
and calls issued while the barrier is being evaluated.
*/
void distributed_control::full_barrier() {
  // gather a sum of all the calls issued to machine 0
  std::vector<size_t> calls_sent_to_target(numprocs(), 0);
  for (size_t i = 0;i < numprocs(); ++i) {
    calls_sent_to_target[i] = global_calls_sent[i].value;
  }

  // tell node 0 how many calls there are
  std::vector<std::vector<size_t> > all_calls_sent(numprocs());
  all_calls_sent[procid()] = calls_sent_to_target;
  all_gather(all_calls_sent, true);

  // get the number of calls I am supposed to receive from each machine
  calls_to_receive.clear(); calls_to_receive.resize(numprocs(), 0);
  for (size_t i = 0;i < numprocs(); ++i) {
    calls_to_receive[i] += all_calls_sent[i][procid()];
//    std::cout << "Expecting " << calls_to_receive[i] << " calls from " << i << std::endl;
  }
  // clear the counters
  num_proc_recvs_incomplete.value = numprocs();
  procs_complete.clear();
  // activate the full barrier
  full_barrier_in_effect = true;
  __asm("mfence");
  // begin one pass to set all which are already completed
  for (procid_t i = 0;i < numprocs(); ++i) {
    if (global_calls_received[i].value >= calls_to_receive[i]) {
      if (procs_complete.set_bit(i) == false) {
        num_proc_recvs_incomplete.dec();
      }
    }
  }

  full_barrier_lock.lock();
  while (num_proc_recvs_incomplete.value > 0) full_barrier_cond.wait(full_barrier_lock);
  full_barrier_lock.unlock();
  full_barrier_in_effect = false;
  barrier();
//   for (size_t i = 0; i < numprocs(); ++i) {
//     std::cout << "Received " << global_calls_received[i].value << " from " << i << std::endl;
//   }
}


} //namespace graphlab

