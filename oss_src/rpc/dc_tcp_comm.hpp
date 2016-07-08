/**
 * Copyright (C) 2016 Turi
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


#ifndef DC_TCP_COMM_HPP
#define DC_TCP_COMM_HPP

#include <sys/socket.h>
#include <netinet/in.h>

#include <vector>
#include <string>
#include <map>

#include <parallel/pthread_tools.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/dc_comm_base.hpp>
#include <rpc/circular_iovec_buffer.hpp>
#include <perf/tracepoint.hpp>
#include <util/dense_bitset.hpp>

#ifndef __APPLE__
// prefix mangling if not Mac
#include <rpc/evwrapdef.h>
#endif
#include <event2/event.h>
namespace graphlab {
namespace dc_impl {


void on_receive_event(int fd, short ev, void* arg);
void on_send_event(int fd, short ev, void* arg);

/**
 \ingroup rpc
 \internal
TCP implementation of the communications subsystem.
Provides a single object interface to sending/receiving data streams to
a collection of machines.
*/
class dc_tcp_comm:public dc_comm_base {
 public:

  DECLARE_TRACER(tcp_send_call);

  inline dc_tcp_comm() {
    is_closed = true;
    INITIALIZE_TRACER(tcp_send_call, "dc_tcp_comm: send syscall");
  }

  size_t capabilities() const {
    return COMM_STREAM;
  }

  /**
   this fuction should pause until all communication has been set up
   and returns the number of systems in the network.
   After which, all other remaining public functions (numprocs(), send(), etc)
   should operate normally. Every received message should immediate trigger the
   attached receiver

   machines: a vector of strings where each string is of the form [IP]:[portnumber]
   initopts: unused
   curmachineid: The ID of the current machine. machines[curmachineid] will be
                 the listening address of this machine

   recvcallback: A function pointer to the receiving function. This function must be thread-safe
   tag: An additional pointer passed to the receiving function.
  */
  void init(const std::vector<std::string> &machines,
            const std::map<std::string,std::string> &initopts,
            procid_t curmachineid,
            std::vector<dc_receive*> receiver,
            std::vector<dc_send*> senders);

  /** shuts down all sockets and cleans up */
  void close();

  /** Socket will be closing soon. closed connections are no longer 
   * fatal errors after this.
   */
  void expect_close();

  ~dc_tcp_comm() {
    close();
  }

  inline bool channel_active(size_t target) const {
    return (sock[target].outsock != -1);
  }

  /**
    Returns the number of machines in the network.
    Only valid after call to init()
  */
  inline procid_t numprocs() const {
    return nprocs;
  }

  /**
   * Returns the current machine ID.
   * Only valid after call to init()
   */
  inline procid_t procid() const {
    return curid;
  }

  /**
   * Returns the total number of bytes sent
   */
  inline size_t network_bytes_sent() const {
    return network_bytessent.value;
  }

  /**
   * Returns the total number of bytes received
   */
  inline size_t network_bytes_received() const {
    return network_bytesreceived.value;
  }

  inline size_t send_queue_length() const {
    size_t a = network_bytessent.value;
    size_t b = buffered_len.value;
    return b - a;
  }

  /**
   Sends the string of length len to the target machine dest.
   Only valid after call to init();
   Establishes a connection if necessary
  */
  void send(size_t target, const char* buf, size_t len);

  void trigger_send_timeout(procid_t target, bool urgent);

 private:
  /// Sets TCP_NO_DELAY on the socket passed in fd
  void set_tcp_no_delay(int fd);

  void set_non_blocking(int fd);

  /// called when listener receives an incoming socket request
  void new_socket(int newsock, sockaddr_in* otheraddr, procid_t remotemachineid);


  /// The number of incoming connections established
  size_t num_in_connected() const;

  /** opens the listening sock and spawns a thread to listen on it.
   * Uses sockhandle if non-zero
   */
  void open_listening(int sockhandle = 0);


  /// constructs a connection to the target machine
  void connect(size_t target);

  /// wrapper around the standard send. but loops till the buffer is all sent
  int sendtosock(int sockfd, const char* buf, size_t len);


  procid_t curid;   /// if od the current processor
  procid_t nprocs;  /// number of processors
  bool is_closed;   /// whether this socket is closed

  std::string program_md5;  /// MD5 hash of current program


  /// all_addrs[i] will contain the IP address of machine i
  std::vector<uint32_t> all_addrs;
  std::map<uint32_t, procid_t> addr2id;
  std::vector<uint16_t> portnums;

  std::vector<dc_receive*> receiver;
  std::vector<dc_send*> sender;
  atomic<size_t> buffered_len;


  struct initial_message {
    procid_t id;
    char md5[32];
  };


  /// All information about stuff regarding a particular sock
  /// Passed to the receive handler
  struct socket_info{
    size_t id;    /// which machine this is connected to
    dc_tcp_comm* owner; /// this object
    int outsock;  /// FD of the outgoing socket
    int insock;   /// FD of the incoming socket
    struct event* inevent;  /// event object for incoming information
    struct event* outevent;  /// event object for outgoing information
    bool wouldblock;
    mutex m;

    circular_iovec_buffer outvec;  /// outgoing data
    struct msghdr data;
  };

  mutex insock_lock; /// locks the insock field in socket_info
  conditional insock_cond; /// triggered when the insock field in socket_info changes

  struct timeout_event {
    bool send_all;
    dc_tcp_comm* owner;
  };

  std::vector<socket_info> sock;

  /**
   * Sends as much of the buffer inside the sockinfo as possible
   * until the send call will block or all sends are complete.
   * Returns true when the buffer has been completely sent
   * If wouldblock returns true, the next call to send_till_block may block
   */
  void send_all(socket_info& sockinfo);
  bool send_till_block(socket_info& sockinfo);
  void check_for_new_data(socket_info& sockinfo);
  void construct_events();



  // counters
  atomic<size_t> network_bytessent;
  atomic<size_t> network_bytesreceived;

  ////////////       Receiving Sockets      //////////////////////
  thread_group inthreads;
  void receive_loop(struct event_base*);

  friend void process_sock(socket_info* sockinfo);
  friend void on_receive_event(int fd, short ev, void* arg);
  struct event_base* inevbase;


  ////////////       Sending Sockets      //////////////////////
  thread_group outthreads;
  void send_loop(struct event_base*);
  friend void on_send_event(int fd, short ev, void* arg);
  struct event_base* outevbase;
  struct event* send_triggered_event;
  struct event* send_all_event;
  timeout_event send_triggered_timeout;
  timeout_event send_all_timeout;

  fixed_dense_bitset<256> triggered_timeouts;
  ////////////       Listening Sockets     //////////////////////
  int listensock;
  thread listenthread;
  void accept_handler();

  /// flag that remote closes are no longer fatal errors
  volatile bool m_expect_close = false;
};

void process_sock(dc_tcp_comm::socket_info* sockinfo);

} // namespace dc_impl
} // namespace graphlab

#ifndef __APPLE__
// prefix mangling if not Mac
#include <rpc/evwrapundef.h>
#endif

#endif

