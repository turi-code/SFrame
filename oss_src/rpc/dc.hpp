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


#ifndef GRAPHLAB_DC_HPP
#define GRAPHLAB_DC_HPP
#include <iostream>
#include <boost/iostreams/stream.hpp>
#include <boost/function.hpp>
#include <parallel/pthread_tools.hpp>
#include <fiber/fiber_group.hpp>
#include <fiber/fiber_conditional.hpp>
#include <graphlab/util/resizing_array_sink.hpp>
#include <graphlab/util/fiber_blocking_queue.hpp>
#include <util/dense_bitset.hpp>
#include <serialization/serialization_includes.hpp>

#include <rpc/dc_macros.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/dc_receive.hpp>
#include <rpc/dc_send.hpp>
#include <rpc/dc_comm_base.hpp>
#include <rpc/dc_dist_object_base.hpp>
#include <rpc/is_rpc_call.hpp>
#include <rpc/function_call_issue.hpp>
#include <rpc/function_broadcast_issue.hpp>
#include <rpc/request_issue.hpp>
#include <rpc/request_reply_handler.hpp>
#include <rpc/function_ret_type.hpp>
#include <rpc/dc_compile_parameters.hpp>
#include <rpc/thread_local_send_buffer.hpp>
#include <rpc/distributed_event_log.hpp>
#include <rpc/function_arg_types_def.hpp>
#include <rpc/fiber_remote_request.hpp>
#include <boost/preprocessor.hpp>
#include <perf/tracepoint.hpp>


namespace graphlab {


/**
 *  \ingroup rpc
 *  \brief Distributed control constructor parameters.
 *
 *  Provides the  communication layer with a list of ip addresses and
 *  port numbers which enumerate all the machines to establish connections
 *  with.
 *
 *  You should not need to this. The default constructor in
 *  graphlab::distributed_control does it for you.
 *  See \ref RPC for usage details.
 */
struct dc_init_param{
  /** A vector containing a list of hostnames/ipaddresses and port numbers
  * of all machines participating in this RPC program.
  * for instance:
  * \code
  * machines.push_back("127.0.0.1:10000");
  * machines.push_back("127.0.0.1:10001");
  * \endcode
  */
  std::vector<std::string> machines;

  /** Additional construction options of the form
    "key1=value1,key2=value2".

    There are no available options at this time.

    Internal options which should not be used
    \li \b __sockhandle__=NUMBER Forces TCP comm to use this socket number for its
                             listening socket instead of creating a new one.
                             The socket must already be bound to the listening
                             port.
  */
  std::string initstring;

  /** The index of this machine into the machines vector */
  procid_t curmachineid;
  /** Number of background RPC handling threads to create */
  size_t numhandlerthreads;
  /** The communication method. */
  dc_comm_type commtype;
  /** If there is only one dc in the process */
  bool is_singleton = true;

  /**
   * Constructs a dc_init_param object.
   * \param numhandlerthreads Optional Argument. The number of handler
   *                          threads to create. Defaults to
   *                          \ref RPC_DEFAULT_NUMHANDLERTHREADS
   * \param commtype The Communication type. The only accepted value now is
   *                 TCP_COMM
   */
  dc_init_param(size_t numhandlerthreads = RPC_DEFAULT_NUMHANDLERTHREADS,
                dc_comm_type commtype = RPC_DEFAULT_COMMTYPE):
    numhandlerthreads(numhandlerthreads),
    commtype(commtype) {
  }
};



// forward declarations
class dc_services;

namespace dc_impl {
  class dc_buffered_stream_send2;
  class dc_stream_receive;
}

/**
 * \ingroup rpc
 * \brief The distributed control object is primary means of communication
 * between the distributed GraphLab processes.
 *
 * The distributed_control object provides asynchronous, multi-threaded
 * Remote Procedure Call (RPC) services to allow distributed GraphLab
 * processes to communicate with each other. Currently, the only
 * communication method implemented is TCP/IP.
 * There are several ways of setting up the communication layer, but the most
 * reliable, and the preferred method, is to "bootstrap" using MPI. See your
 * local MPI documentation for details on how to launch MPI jobs.
 *
 * To construct a distributed_control object, the simplest method is to just
 * invoke the default constructor.
 *
 * \code
 *  // initialize MPI
 *  mpi_tools::init(argc, argv);
 *  // construct distributed control object
 *  graphlab::distributed_control dc;
 * \endcode
 *
 * After which all distributed control services will operate correctly.
 *
 * Each process is assigned a sequential process ID starting at 0.
 * i.e. The first process will have a process ID of 0, the second process
 * will have an ID of 1, etc. distributed_control::procid() can be used to
 * obtain the current machine's process ID, and distributed_control::numprocs()
 * can be used to obtain the total number of processes.
 *
 * The primary functions used to communicate between processes are
 * distributed_control::remote_call() and
 * distributed_control::remote_request(). These functions are thread-safe and
 * can be called very rapidly as they only write into a local buffer.
 * Communication is handled by a background thread. On the remote side,
 * RPC calls are handled in parallel by a thread pool, and thus may be
 * parallelized arbitrarily. Operations such as
 * distributed_control::full_barrier(), or the sequentialization key
 * can be used to get finer grained control over order of execution on the
 * remote machine.
 *
 * A few other additional helper functions are also provided to support
 * "synchronous" modes of communication. These functions are not thread-safe
 * and can only be called on one thread per machine. These functions block
 * until all machines call the same function. For instance, if gather() is
 * called on one machine, it will not return until all machines call gather().
 *
 * \li distributed_control::barrier()
 * \li distributed_control::full_barrier()
 * \li distributed_control::broadcast()
 * \li distributed_control::all_reduce()
 * \li distributed_control::all_reduce2()
 * \li distributed_control::gather()
 * \li distributed_control::all_gather()
 *
 * \note These synchronous operations are modeled after some MPI collective
 * operations. However, these operations here are not particularly optimized
 * and will generally be slower than their MPI counterparts. However, the
 * implementations here are much easier to use, relying extensively on
 * serialization to simplify communication.
 *
 * To support Object Oriented Programming like methodologies, we allow the
 * creation of <b>Distributed Objects</b> through graphlab::dc_dist_object.
 * dc_dist_object allows a class to construct its own local copy of
 * a distributed_control object allowing instances of the class to communicate
 * with each other across the network.
 *
 * See \ref RPC for usage examples.
 */
class distributed_control{
  public:
        /**  \internal
         * Each element of the function call queue is a data/len pair */
    struct function_call_block{
      function_call_block() {}

      function_call_block(char* data, size_t len,
                          unsigned char packet_mask):
                          data(data), len(len), packet_mask(packet_mask){}

      char* data;
      size_t len;
      unsigned char packet_mask;
    };
  private:
   /// initialize receiver threads. private form of the constructor
   void init(const std::vector<std::string> &machines,
             const std::string &initstring,
             procid_t curmachineid,
             size_t numhandlerthreads,
             dc_comm_type commtype = RPC_DEFAULT_COMMTYPE,
             bool is_singleton = true);

  /// a pointer to the communications subsystem
  dc_impl::dc_comm_base* comm;

  /// senders and receivers to all machines
  std::vector<dc_impl::dc_receive*> receivers;
  std::vector<dc_impl::dc_send*> senders;

  /// A thread group of function call handlers
  fiber_group fcallhandlers;
  std::vector<atomic<size_t> > fcall_handler_active;
  dense_bitset fcall_handler_blockers;

  struct fcallqueue_entry {
    std::vector<function_call_block> calls;
    char* chunk_src;
    size_t chunk_len;
    atomic<size_t>* chunk_ref_counter;
    procid_t source;
    bool is_chunk;
  };
  /// a queue of functions to be executed
  std::vector<fiber_blocking_queue<fcallqueue_entry*> > fcallqueue;
  // number of blocks waiting to be deserialized + the number of
  // incomplete function calls
  atomic<size_t> fcallqueue_length;

  /// object registrations;
  std::vector<void*> registered_objects;
  std::vector<dc_impl::dc_dist_object_base*> registered_rmi_instance;

  /// For convenience, we provide a instance of dc_services
  dc_services* distributed_services;

  /// ID of the local machine
  procid_t localprocid;


  /// Number of machines
  procid_t localnumprocs;

  bool is_singleton = true;

  std::vector<atomic<size_t> > global_calls_sent;
  std::vector<atomic<size_t> > global_calls_received;

  std::vector<atomic<size_t> > global_bytes_received;

  template <typename T> friend class dc_dist_object;
  friend class dc_impl::dc_stream_receive;
  friend class dc_impl::dc_buffered_stream_send2;
  friend struct dc_impl::thread_local_buffer;

  /// disable the operator= by placing it in private
  distributed_control& operator=(const distributed_control& dc) { return *this; }


  std::map<std::string, std::string> parse_options(std::string initstring);

  volatile inline size_t num_registered_objects() {
    return registered_objects.size();
  }



  DECLARE_TRACER(dc_receive_queuing);
  DECLARE_TRACER(dc_receive_multiplexing);
  DECLARE_TRACER(dc_call_dispatch);

  DECLARE_EVENT(EVENT_NETWORK_BYTES);
  DECLARE_EVENT(EVENT_RPC_CALLS);
 public:

  /**
   * Default constructor. Automatically tries to read the initialization
   * from environment variables, or from MPI (if MPI is initialized).
   */
  distributed_control();

  /**
   * Passes custom constructed initialization parameters in
   * \ref dc_init_param
   *
   * Though dc_init_param can be obtained from environment variables using
   * dc_init_from_env() or from MPI using dc_init_from_mpi(),
   * using the default constructor is prefered.
   */
  explicit distributed_control(dc_init_param initparam);

  ~distributed_control();

  /**
   * Gets the procid of the last distributed_control instance created.
   * If there is no distributed_control instance, this returns 0.
   * For instance, this returns the current machine's procid if there is only
   * one distributed_control.
   */
  static procid_t get_instance_procid();

  inline size_t num_handler_threads() const {
    return fcallqueue.size();
  }

  /**
   * Gets a pointer to the current distributed_control instance.
   *
   * The instance must be created using distributed_control_global::create_instance().
   *
   * If there is no distributed_control instance, this returns NULL.
   */
  static distributed_control* get_instance();


  /// returns the id of the current process
  inline procid_t procid() const {
    return localprocid;
  }

  /// returns the number of processes in total.
  inline procid_t numprocs() const {
    return localnumprocs;
  }


  bool use_fast_track_requests;

  /// Sets the fast track status, returning the previous value
  bool set_fast_track_requests(bool val) {
    bool ret = use_fast_track_requests;
    use_fast_track_requests = val;
    return ret;
  }

  /// Returns true if we should fast track all request messages
  bool fast_track_requests() {
    return use_fast_track_requests;
  }

  /**
  \brief Sets the sequentialization key to a new value, returning the
  previous value.

  All RPC calls made using the same key value (as long as the key is non-zero)
  will sequentialize. RPC calls made while the key value is 0 can be
  run in parallel in arbitrary order.

  \code
  oldval = distributed_control::set_sequentialization_key(new_key);
  // ...
  // ... do stuff
  // ...
  set_sequentialization_key(oldval);
  \endcode

  The key value is <b>thread-local</b> thus setting the key value in
  one thread does not affect the key value in another thread.
  */
  static unsigned char set_sequentialization_key(unsigned char newkey);

  /**
  \brief Creates a new sequentialization key, returning the old value.

  All RPC calls made using the same key value (as long as the key is non-zero)
  will sequentialize.   RPC calls made while the key value is 0 can be run in
  parallel in arbitrary order.  However, since new_sequentialization_key() uses
  a very naive key selection system, we recommend the use of
  set_sequentialization_key().

  User should
  \code
  oldval = distributed_control::new_sequentialization_key();
  // ...
  // ... do stuff
  // ...
  set_sequentialization_key(oldval);
  \endcode

  The key value is <b>thread-local</b> thus setting the key value in
  one thread does not affect the key value in another thread.
  */
  static unsigned char new_sequentialization_key();

  /** \brief gets the current sequentialization key. This function is not
   * generally useful.
   */
  static unsigned char get_sequentialization_key();




  /*
   * The key RPC communication functions are all macro generated
   * and doxygen does not like them so much.
   * Here, we will block all of them out
   * and have another set of "fake" functions later on which are wrapped
   * with a #if 0 so C++ will ignore them.
   */

  /// \cond GRAPHLAB_INTERNAL



  /*
  This generates the interface functions for the standard calls, basic calls
  The generated code looks like this:

  template<typename F, F remote_function, typename T0> void remote_call (procid_t target, const T0 &i0 )
  {
    ASSERT_LT(target, senders.size());
    dc_impl::remote_call_issue1 <F, remote_function, T0> ::exec(senders[target],
                                                STANDARD_CALL,
                                                target,
                                                i0 );
  }
  The arguments passed to the RPC_INTERFACE_GENERATOR ARE: (interface name, issue processor name, flags)

  */
  #define GENARGS(Z,N,_)  BOOST_PP_CAT(const T, N) BOOST_PP_CAT(&i, N)
  #define GENI(Z,N,_) BOOST_PP_CAT(i, N)
  #define GENT(Z,N,_) BOOST_PP_CAT(T, N)
  #define GENARC(Z,N,_) arc << BOOST_PP_CAT(i, N);

  #define RPC_INTERFACE_GENERATOR(Z,N,FNAME_AND_CALL) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
  void  BOOST_PP_TUPLE_ELEM(3,0,FNAME_AND_CALL) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, senders.size()); \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(3,1,FNAME_AND_CALL),N) \
        <F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(senders[target],  BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL), target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
  }   \

  /*
  Generates the interface functions. 3rd argument is a tuple (interface name, issue name, flags)
  */
  BOOST_PP_REPEAT(6, RPC_INTERFACE_GENERATOR, (remote_call, dc_impl::remote_call_issue, STANDARD_CALL) )
  BOOST_PP_REPEAT(6, RPC_INTERFACE_GENERATOR, (reply_remote_call,dc_impl::remote_call_issue, STANDARD_CALL | FLUSH_PACKET) )
  BOOST_PP_REPEAT(6, RPC_INTERFACE_GENERATOR, (control_call, dc_impl::remote_call_issue, (STANDARD_CALL | CONTROL_PACKET)) )


#define BROADCAST_INTERFACE_GENERATOR(Z,N,FNAME_AND_CALL) \
  template<typename F, F remote_function, typename Iterator BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
  void  BOOST_PP_TUPLE_ELEM(3,0,FNAME_AND_CALL) (Iterator target_begin, Iterator target_end BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    if (target_begin == target_end) return;               \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(3,1,FNAME_AND_CALL),N) \
        <Iterator, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(senders,  BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL), target_begin, target_end BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
  }   \

  BOOST_PP_REPEAT(6, BROADCAST_INTERFACE_GENERATOR, (broadcast_call, dc_impl::remote_broadcast_issue, STANDARD_CALL) )


  #define CUSTOM_REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
    BOOST_PP_TUPLE_ELEM(2,0,ARGS) (procid_t target, size_t handle, unsigned char flags BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(2,1,ARGS),N) \
        <F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(senders[target],  handle, flags, target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
  }   


  #define FUTURE_REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
    BOOST_PP_TUPLE_ELEM(2,0,ARGS) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, senders.size()); \
    request_future<__GLRPC_FRESULT> reply;      \
    custom_remote_request<F, remote_function>(target,  reply.get_handle(), BOOST_PP_TUPLE_ELEM(2,1,ARGS) BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
    return reply; \
  }  


  #define REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
    BOOST_PP_TUPLE_ELEM(2,0,ARGS) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    request_future<__GLRPC_FRESULT> reply;      \
    custom_remote_request<F, remote_function>(target,  reply.get_handle(), BOOST_PP_TUPLE_ELEM(2,1,ARGS) BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
    return reply(); \
  } 

  #define FIBER_REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
  BOOST_PP_TUPLE_ELEM(1,0,ARGS) (procid_t target \
                                 BOOST_PP_COMMA_IF(N) \
                                 BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    request_future<__GLRPC_FRESULT> reply(new fiber_reply_container);      \
    custom_remote_request<F, remote_function>(target, reply.get_handle(), STANDARD_CALL BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
    return reply; \
  } 

  /*
  Generates the interface functions. 3rd argument is a tuple (interface name, issue name, flags)
  */
  BOOST_PP_REPEAT(7, CUSTOM_REQUEST_INTERFACE_GENERATOR, (void custom_remote_request, dc_impl::remote_request_issue) )
  BOOST_PP_REPEAT(7, REQUEST_INTERFACE_GENERATOR, (typename dc_impl::function_ret_type<__GLRPC_FRESULT>::type remote_request, (STANDARD_CALL | FLUSH_PACKET)) )
  BOOST_PP_REPEAT(7, FUTURE_REQUEST_INTERFACE_GENERATOR, (request_future<__GLRPC_FRESULT> future_remote_request, (STANDARD_CALL)) )
  BOOST_PP_REPEAT(7, FIBER_REQUEST_INTERFACE_GENERATOR, (request_future<__GLRPC_FRESULT> fiber_remote_request) )


  #undef FIBER_REQUEST_INTERFACE_GENERATOR
  #undef RPC_INTERFACE_GENERATOR
  #undef BROADCAST_INTERFACE_GENERATOR
  #undef REQUEST_INTERFACE_GENERATOR
  #undef FUTURE_REQUEST_INTERFACE_GENERATOR
  #undef CUSTOM_REQUEST_INTERFACE_GENERATOR
  #undef GENARC
  #undef GENT
  #undef GENI
  #undef GENARGS
  /// \endcond

/*************************************************************************
 *           Here begins the Doxygen fake functions block                *
 *************************************************************************/

#if DOXYGEN_DOCUMENTATION

/**
 * \brief Performs a non-blocking RPC call to the target machine
 * to run the provided function pointer.
 *
 * remote_call() calls the function "fn" on a target remote machine. Provided
 * arguments are serialized and sent to the target.
 * Therefore, all arguments are necessarily transmitted by value.
 * If the target function has a return value, the return value is lost.
 *
 * remote_call() is non-blocking and does not wait for the target machine
 * to complete execution of the function. Different remote_calls may be handled
 * by different threads on the target machine and thus the target function
 * should be made thread-safe.
 * Alternatively, see set_sequentialization_key()
 * to force sequentialization of groups of remote calls.
 *
 * If blocking operation is desired, remote_request() may be used.
 * Alternatively, a full_barrier() may also be used to wait for completion of
 * all incomplete RPC calls.
 *
 * Example:
 * \code
 * // A print function is defined
 * void print(std::string s) {
 *   std::cout << s << "\n";
 * }
 *
 * ... ...
 * // call the print function on machine 1 to print "hello"
 * dc.RPC_CALL(remote_call, print)(1, "hello");
 *
 * // which is equivalent to 
 * dc.template remote_call<decltype(print), print>(1, "hello");
 *
 * \endcode
 *
 *
 *
 * \tparam F Type of function
 * \tparam fn The function to run on the target machine
 * \param targetmachine The ID of the machine to run the function on
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 */
  template <typename F, F fn>
  void remote_call(procid_t targetmachine, ...);



/**
 * \brief Performs a non-blocking RPC call to a collection of machines
 * to run the provided function pointer.
 *
 * This function calls the provided function pointer on a collection of
 * machines contained in the iterator range [begin, end).
 * Provided arguments are serialized and sent to the target.
 * Therefore, all arguments are necessarily transmitted by value.
 * If the target function has a return value, the return value is lost.
 *
 * This function is functionally equivalent to:
 *
 * \code
 * while(machine_begin != machine_end) {
 *  remote_call(*machine_begin, fn, ...);
 *  ++machine_begin;
 * }
 * \endcode
 *
 * However, this function makes some optimizations to ensure all arguments
 * are only serialized once instead of \#calls times.
 *
 * This function is non-blocking and does not wait for the target machines
 * to complete execution of the function. Different remote_calls may be handled
 * by different threads on the target machines and thus the target function
 * should be made thread-safe. Alternatively, see set_sequentialization_key()
 * to force sequentialization of groups of remote_calls. A full_barrier()
 * may also be issued to wait for completion of all RPC calls issued prior
 * to the full barrier.
 *
 * Example:
 * \code
 * // A print function is defined
 * void print(std::string s) {
 *   std::cout << s << "\n";
 * }
 *
 * ... ...
 * // call the print function on machine 1, 3 and 5 to print "hello"
 * std::vector<procid_t> procs;
 * procs.push_back(1); procs.push_back(3); procs.push_back(5);
 * dc.RPC_CALL(broadcast_call, print)(procs.begin(), procs.end(), "hello");
 * // which is equivalent to 
 * dc.template broadcast_call<decltype(print), print>(procs.begin(), procs.end(), "hello");
 * \endcode
 *
 *
 * \tparam F Type of function
 * \tparam fn The function to run on the target machine
 * \param machine_begin The beginning of an iterator range containing a list
 *                      machines to call.  Iterator::value_type must be
 *                      castable to procid_t.
 * \param machine_end   The end of an iterator range containing a list
 *                      machines to call.  Iterator::value_type must be
 *                      castable to procid_t.
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 */
  template <typename F, F fn>
  void broadcast_call(Iterator machine_begin, Iterator machine_end, ...);


/**
 * \brief Performs a blocking RPC request to the target machine
 * to run the provided function pointer.
 *
 * remote_request() calls the function "fn" on a target remote machine. Provided
 * arguments are serialized and sent to the target.
 * Therefore, all arguments are necessarily transmitted by value.
 * If the target function has a return value, it is sent back to calling
 * machine.
 *
 * Unlike remote_call(), remote_request() is blocking and waits for the target
 * machine to complete execution of the function. However, different
 * remote_requests may be still be handled by different threads on the target
 * machine.
 *
 * Example:
 * \code
 * // A print function is defined
 * int add_one(int i) {
 *   return i + 1;
 * }
 *
 * ... ...
 * // call the add_one function on machine 1
 * int i = 10;
 * i = dc.RPC_CALL(remote_request, add_one)(1, i);
 * // which is equivalent to
 * i = dc.template remote_request<decltype(add_one), add_one>(1, i);
 * // i will now be 11
 * \endcode
 *
 * \see graphlab::distributed_control::fiber_remote_request
 *      graphlab::distributed_control::future_remote_request
 *
 * \tparam F Type of function
 * \tparam fn The function to run on the target machine
 * \param targetmachine The ID of the machine to run the function on
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 *
 * \returns Returns the same return type as the function fn
 */
  template <typename F, F fn>
  RetVal remote_request(procid_t targetmachine, ...);

/**
 * \brief Performs a nonblocking RPC request to the target machine for use with fibers
 * to run the provided function pointer which has an expected return value.
 *
 * fiber_remote_request() calls the function "fn" on a target remote machine.
 * Provided arguments are serialized and sent to the target.
 * Therefore, all arguments are necessarily transmitted by value.
 * If the target function has a return value, it is sent back to calling
 * machine.  fiber_remote_request() returns immediately a \ref
 * graphlab::request_future object which will allow you wait for the return
 * value.
 *
 * fiber_remote_request() has an identical interface to 
 * \ref graphlab::distributed_control::future_remote_request() , but has the 
 * additional capability that if a \ref graphlab::request_future::wait() is 
 * called on the request while within a fiber, it deschedules the fiber and
 * context switches, returning only when the future is ready. This allows
 * the future to be used from within a fiber.
 *
 * Example:
 * \code
 * // A print function is defined
 * int add_one(int i) {
 *   return i + 1;
 * }
 *
 * ... ...
 * // call the add_one function on machine 1
 * int i = 10;
 * graphlab::request_future<int> ret = dc.RPC_CALL(fiber_remote_request, add_one)(1, i);
 *  // which is equivalent to
 * graphlab::request_future<int> ret = dc.template fiber_remote_request<decltype(add_one), add_one>(1, i);
 *
 * // this is safe to do within a fiber as it will not halt other fibers.
 * int result = ret();
 * // result will be 11
 * \endcode
 *
 * \see graphlab::distributed_control::remote_request
 *      graphlab::distributed_control::future_remote_request
 *      graphlab::dc_dist_object::fiber_remote_request
 *
 * \tparam F Type of function
 * \tparam fn The function to run on the target machine
 * \param targetmachine The ID of the machine to run the function on
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 *
 * \returns Returns a future templated around the same type as the return 
 *          value of the called function
 */
 template <typename F, F fn>
 request_future<RetVal> fiber_remote_request(procid_t targetmachine, ...);



/**
 * \brief Performs a non-blocking RPC request to the target machine 
 *
 * future_remote_request() calls the function "fn" on a target remote machine. Provided
 * arguments are serialized and sent to the target.
 * Therefore, all arguments are necessarily transmitted by value.
 * If the target function has a return value, it is sent back to calling
 * machine.
 *
 * future_remote_request() is like remote_request(), but is non-blocking.
 * Instead, it returns immediately a \ref graphlab::request_future object
 * which will allow you wait for the return value.
 *
 * Example:
 * \code
 * // A print function is defined
 * int add_one(int i) {
 *   return i + 1;
 * }
 *
 * ... ...
 * // call the add_one function on machine 1
 * int i = 10;
 * graphlab::request_future<int> ret = dc.RPC_CALL(future_remote_request, add_one)(1, i);
 *  // which is equivalent to
 * graphlab::request_future<int> ret = dc.template future_remote_request<decltype(add_one), add_one>(1, i);
 *
 * // result will be 11
 * int result = ret();
 * \endcode
 *
 * \see graphlab::distributed_control::fiber_remote_request
 *      graphlab::distributed_control::remote_request
 *
 * \param targetmachine The ID of the machine to run the function on
 * \param fn The function to run on the target machine
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 *
 * \returns Returns the same return type as the function fn
 */
  request_future<RetVal> future_remote_request(procid_t targetmachine, Fn fn, ...);



#endif
/*************************************************************************
 *              Here end the Doxygen fake functions block                *
 *************************************************************************/


 private:
  /**
   *
   * l
  Immediately calls the function described by the data
  inside the buffer. This should not be called directly.
  */
  void exec_function_call(procid_t source, unsigned char packet_type_mask, const char* data, const size_t len);



  /**
   * \internal
   * Called by handler threads to process the function call block
   */
  void process_fcall_block(fcallqueue_entry &fcallblock);


  /**
   * \internal
   * Receive a collection of serialized function calls.
   * This function will take ownership of the pointer
   */
  void deferred_function_call_chunk(char* buf, size_t len, procid_t src);


  /**
   * \internal
   * Gets the sequentialization key of a block if any.
   */
  unsigned char get_block_sequentialization_key(fcallqueue_entry& fcallblock);

  /**
   * \internal
  This is called by the function handler threads
  */
  void fcallhandler_loop(size_t id);

 public:
   /// \cond GRAPHLAB_INTERNAL
  /**
   * \internal
   * Stops one group of handler threads and wait for them to complete.
   * May be used to allow external threads to take over RPC processing.
   *
   * \param threadid Group number to stop
   * \param total_threadid Number of groups
   */
  void stop_handler_threads(size_t threadid, size_t total_threadid);

  /**
   * \internal
   * Stops one group of handler threads and returns immediately without
   * waiting for them to complete.
   * May be used to allow external threads to take over RPC processing.
   *
   * \param threadid Group number to stop
   * \param total_threadid Number of groups
   */
  void stop_handler_threads_no_wait(size_t threadid, size_t total_threadid);

  /**
   * \internal
   * Performs RPC processing for a group of threads in lieu of the built-in
   * RPC threads. The group must be stopped before using stop_handler_threads
   *
   * \param threadid Group number to handle
   * \param total_threadid Number of groups
   */
  void handle_incoming_calls(size_t threadid, size_t total_threadid);


  /**
   * \internal
   * Restarts internal RPC threads for a group.
   * The group must be stopped before using stop_handler_threads
   *
   * \param threadid Group number to restart
   * \param total_threadid Number of groups
   */
  void start_handler_threads(size_t threadid, size_t total_threadid);

  /// \internal
  size_t recv_queue_length() const {
    return fcallqueue_length.value;
  }
  /// \internal
  size_t send_queue_length() const {
    return comm->send_queue_length();
  }

  /// \endcond

 private:
  inline void inc_calls_sent(procid_t procid) {
    //PERMANENT_ACCUMULATE_DIST_EVENT(eventlog, CALLS_EVENT, 1);
    global_calls_sent[procid].inc();
  }

  inline void inc_calls_received(procid_t procid) {

    if (!full_barrier_in_effect) {
      size_t t = global_calls_received[procid].inc();
      if (full_barrier_in_effect) {
        if (t == calls_to_receive[procid]) {
          // if it was me who set the bit
          if (procs_complete.set_bit(procid) == false) {
            // then decrement the incomplete count.
            // if it was me to decreased it to 0
            // lock and signal
            full_barrier_lock.lock();
            if (num_proc_recvs_incomplete.dec() == 0) {
              full_barrier_cond.signal();
            }
            full_barrier_lock.unlock();
          }
        }
      }
    }
    else {
      //check the proc I just incremented.
      // If I just exceeded the required size, I need
      // to decrement the full barrier counter
      if (global_calls_received[procid].inc() == calls_to_receive[procid]) {
        // if it was me who set the bit
        if (procs_complete.set_bit(procid) == false) {
          // then decrement the incomplete count.
          // if it was me to decreased it to 0
          // lock and signal
          full_barrier_lock.lock();
          if (num_proc_recvs_incomplete.dec() == 0) {
            full_barrier_cond.signal();
          }
          full_barrier_lock.unlock();
        }
      }
    }
  }

 public:
   /// \brief  Returns the total number of RPC calls made
  inline size_t calls_sent() const {
    size_t ctr = 0;
    for (size_t i = 0;i < numprocs(); ++i) {
      ctr += global_calls_sent[i].value;
    }
    return ctr;
  }

   /// \brief  Returns the total number of RPC calls made in millions
  inline double mega_calls_sent() const {
    size_t ctr = 0;
    for (size_t i = 0;i < numprocs(); ++i) {
      ctr += global_calls_sent[i].value;
    }
    return double(ctr)/(1024 * 1024);
  }



  /// \brief Returns the total number of RPC calls received
  inline size_t calls_received() const {
    size_t ctr = 0;
    for (size_t i = 0;i < numprocs(); ++i) {
      ctr += global_calls_received[i].value;
    }
    return ctr;
  }

  /** \brief Returns the total number of bytes sent excluding headers and other
   *  control overhead. Also see network_bytes_sent()
   */
  inline size_t bytes_sent() const {
    size_t ret = 0;
    for (size_t i = 0;i < senders.size(); ++i) ret += senders[i]->bytes_sent();
    return ret;
  }

  /** \brief Returns the total number of bytes sent including all headers
   * and other control overhead. Also see bytes_sent()
   */
  inline size_t network_bytes_sent() const {
    return comm->network_bytes_sent();
  }

  /** \brief Returns the total number of megabytes sent including all headers
   * and other control overhead. Also see network_bytes_sent()
   */
  inline double network_megabytes_sent() const {
    return double(comm->network_bytes_sent()) / (1024 * 1024);
  }



  /** \brief Returns the total number of bytes received excluding all headers
   * and other control overhead. Also see bytes_sent().
   */
  inline size_t bytes_received() const {
    size_t ret = 0;
    for (size_t i = 0;i < global_bytes_received.size(); ++i) {
      ret += global_bytes_received[i].value;
    }
    return ret;
  }

  /// \cond GRAPHLAB_INTERNAL

  /// \internal
  inline size_t register_object(void* v, dc_impl::dc_dist_object_base *rmiinstance) {
    ASSERT_NE(v, (void*)NULL);
    registered_objects.push_back(v);
    registered_rmi_instance.push_back(rmiinstance);
    return registered_objects.size() - 1;
  }

  /// \internal
  inline void* get_registered_object(size_t id) {
    while(__builtin_expect((id >= num_registered_objects()), 0)) sched_yield();
    while (__builtin_expect(registered_objects[id] == NULL, 0)) sched_yield();
    return registered_objects[id];
  }

  /// \internal
  inline dc_impl::dc_dist_object_base* get_rmi_instance(size_t id) {
    while(id >= num_registered_objects()) sched_yield();
    ASSERT_NE(registered_rmi_instance[id], (void*)NULL);
    return registered_rmi_instance[id];
  }

  /// \internal
  inline void clear_registered_object(size_t id) {
    registered_objects[id] = (void*)NULL;
    registered_rmi_instance[id] = NULL;
  }

  inline void register_send_buffer(dc_impl::thread_local_buffer* buffer) {
    for (size_t i = 0;i < senders.size(); ++i) {
      senders[i]->register_send_buffer(buffer);
    }
  }

  inline void unregister_send_buffer(dc_impl::thread_local_buffer* buffer) {
    for (size_t i = 0;i < senders.size(); ++i) {
      senders[i]->unregister_send_buffer(buffer);
    }
  }

  /// \endcond

  /**
   * \brief Performs a local flush of all send buffers
   */
  void flush();

  /**
   * \brief Performs a local flush of all send buffers
   */
  void flush(procid_t p);

  /**
   * \brief Requests a flush of all send buffers to happen soon;
   */
  void flush_soon();

  /**
   * \brief Requests a flush of one send buffers to happen soon;
   */
  void flush_soon(procid_t p);

  /**
   * \brief Writes a string to the send buffer and flushes
   */
  inline void write_to_buffer(procid_t target, char* c, size_t len) {
    senders[target]->write_to_buffer(c, len);
  }


  /**
   * \brief Sends an object to a target machine and blocks until the
   * target machine calls recv_from() to receive the object.
   *
   * This function sends a \ref sec_serializable object "t" to the target
   * machine, but waits for the target machine to call recv_from()
   * before returning to receive the object before returning.
   *
   * Example:
   * \code
   * int i;
   * if (dc.procid() == 0) {
   *   i = 10;
   *   // if I am machine 0, I send the value i = 10 to machine 1
   *   dc.send_to(1, i);
   * } else if (dc.procid() == 1) {
   *   // machine 1 receives the value of i from machine 0
   *   dc.recv_from(0, i);
   * }
   * // at this point machines 0 and 1 have the value i = 10
   * \endcode
   *
   * \tparam U the type of object to send. This should be inferred by the
   *           compiler.
   * \param target The target machine to send to. Target machine must call
   *               recv_from() before this call will return.
   * \param t      The object to send. It must be serializable. The type must
   *               match the target machine's call to recv_from()
   * \param control Optional parameter. Defaults to false. If set to true,
   *                this will marked as control plane communication and will
   *                not register in bytes_received() or bytes_sent(). This must
   *                match the "control" parameter on the target machine's
   *                recv_from() call.
   *
   * \note Behavior is undefined if multiple threads on the same machine
   * call send_to simultaneously
   *
   */
  template <typename U>
  inline void send_to(procid_t target, U& t, bool control = false);

   /**
   * \brief Waits to receives an object a source machine sent via send_to()
   *
   * This function waits to receives a \ref sec_serializable object "t" from a
   * source machine. The source machine must send the object using
   * send_to(). The source machine will wait for the target machine's
   * recv_from() to complete before returning.
   *
   * Example:
   * \code
   * int i;
   * if (dc.procid() == 0) {
   *   i = 10;
   *   // if I am machine 0, I send the value i = 10 to machine 1
   *   dc.send_to(1, i);
   * } else if (dc.procid() == 1) {
   *   // machine 1 receives the value of i from machine 0
   *   dc.recv_from(0, i);
   * }
   * // at this point machines 0 and 1 have the value i = 10
   * \endcode
   *
   * \tparam U the type of object to receive. This should be inferred by the
   *           compiler.
   * \param source The target machine to receive from. This function will block
   *               until data is received.
   * \param t      The object to receive. It must be serializable and the type
   *               must match the source machine's call to send_to()
   * \param control Optional parameter. Defaults to false. If set to true,
   *                this will marked as control plane communication and will
   *                not register in bytes_received() or bytes_sent(). This must
   *                match the "control" parameter on the source machine's
   *                send_to() call.
   *
   * \note Behavior is undefined if multiple threads on the same machine
   * call recv_from simultaneously
   *
   */
  template <typename U>
  inline void recv_from(procid_t source, U& t, bool control = false);


  /**
   * \brief This function allows one machine to broadcasts an object to all
   * machines.
   *
   * The originator calls broadcast with data provided in
   * in 'data' and originator set to true.
   * All other callers call with originator set to false.
   *
   * The originator will then return 'data'. All other machines
   * will receive the originator's transmission in the "data" parameter.
   *
   * This call is guaranteed to have barrier-like behavior. That is to say,
   * this call will block until all machines enter the broadcast function.
   *
   * Example:
   * \code
   * int i;
   * if (procid() == 0) {
   *   // if I am machine 0, I broadcast the value i = 10 to all machines
   *   i = 10;
   *   dc.broadcast(i, true);
   * } else {
   *   // all other machines receive the broadcast value
   *   dc.broadcast(i, false);
   * }
   * // at this point, all machines have i = 10
   * \endcode
   *
   * \note Behavior is undefined if more than one machine calls broadcast
   * with originator set to true.
   *
   * \note Behavior is undefined if multiple threads on the same machine
   * call broadcast simultaneously
   *
   * \param data If this is the originator, this will contain the object to
   *             broadcast. Otherwise, this will be a reference to the object
   *             receiving the broadcast.
   * \param originator Set to true if this is the source of the broadcast.
   *                   Set to false otherwise.
   * \param control Optional parameter. Defaults to false. If set to true,
   *                this will marked as control plane communication and will
   *                not register in bytes_received() or bytes_sent(). This must
   *                be the same on all machines.
   */
  template <typename U>
  inline void broadcast(U& data, bool originator, bool control = false);

  /**
   * \brief Collects information contributed by each machine onto
   *         one machine.
   *
   * The goal is to collect some information from each machine onto a single
   * target machine (sendto). To accomplish this,
   * each machine constructs a vector of length numprocs(), and stores
   * the data to communicate in the procid()'th entry in the vector.
   * Then calling gather with the vector and the target machine will send
   * the contributed value to the target.
   * When the function returns, machine sendto will have the complete vector
   * where data[i] is the data contributed by machine i.
   *
   * Example:
   * \code
   * // construct the vector of values
   * std::vector<int> values;
   * values.resize(dc.numprocs());
   *
   * // set my contributed value
   * values[dc.procid()] = dc.procid();
   * dc.gather(values, 0);
   * // at this point machine 0 will have a vector with length equal to the
   * // number of processes, and containing values [0, 1, 2, ...]
   * // All other machines value vector will be unchanged.
   * \endcode
   *
   * \note Behavior is undefined machines call gather with different values for
   * sendto
   *
   * \note Behavior is undefined if multiple threads on the same machine
   * call gather simultaneously
   *
   * \param data  A vector of length equal to the number of processes. The
   *              information to communicate is in the entry data[procid()]
   * \param sendto Machine which will hold the complete vector at the end
   *               of the operation. All machines must have the same value
   *               for this parameter.
   * \param control Optional parameter. Defaults to false. If set to true,
   *                this will marked as control plane communication and will
   *                not register in bytes_received() or bytes_sent(). This must
   *                be the same on all machines.
   */
  template <typename U>
  inline void gather(std::vector<U>& data, procid_t sendto, bool control = false);

  /**
   * \brief Sends some information contributed by each machine to all machines
   *
   * The goal is to have each machine broadcast a piece of information to all
   * machines. This is like gather(), but all machines have the complete vector
   * at the end.  To accomplish this, each machine constructs a vector of
   * length numprocs(), and stores the data to communicate in the procid()'th
   * entry in the vector.  Then calling all_gather with the vector will result
   * in all machines having a complete copy of the vector containing all
   * contributions (entry 0 from machine 0, entry 1 from machine 1, etc).
   *
   * Example:
   * \code
   * // construct the vector of values
   * std::vector<int> values;
   * values.resize(dc.numprocs());
   *
   * // set my contributed value
   * values[dc.procid()] = dc.procid();
   * dc.all_gather(values);
   * // at this point all machine will have a vector with length equal to the
   * // number of processes, and containing values [0, 1, 2, ...]
   * \endcode
   *
   * \note Behavior is undefined if multiple threads on the same machine
   * call all_gather simultaneously
   *
   * \param data  A vector of length equal to the number of processes. The
   *              information to communicate is in the entry data[procid()]
   * \param control Optional parameter. Defaults to false. If set to true,
   *                this will marked as control plane communication and will
   *                not register in bytes_received() or bytes_sent(). This must
   *                be the same on all machines.
   */
  template <typename U>
  inline void all_gather(std::vector<U>& data, bool control = false);


  /**
   * \brief Combines a value contributed by each machine, making the result
   * available to all machines.
   *
   * Each machine calls all_reduce() with a object which is serializable
   * and has operator+= implemented. When all_reduce() returns, the "data"
   * variable will contain a value corresponding to adding up the objects
   * contributed by each machine.
   *
   * Example:
   * \code
   * int i = 1;
   * dc.all_reduce(i);
   * // since each machine contributed the value "1",
   * // all machines will have i = numprocs() here.
   * \endcode
   *
   * \param data  A piece of data to perform a reduction over.
   *              The type must implement operator+=.
   * \param control Optional parameter. Defaults to false. If set to true,
   *                this will marked as control plane communication and will
   *                not register in bytes_received() or bytes_sent(). This must
   *                be the same on all machines.
   */
  template <typename U>
  inline void all_reduce(U& data, bool control = false);

  /**
   * \brief Combines a value contributed by each machine, making the result
   * available to all machines.
   *
   * This function is equivalent to all_reduce(), but with an externally
   * defined PlusEqual function.
   *
   * Each machine calls all_reduce() with a object which is serializable
   * and a function "plusequal" which combines two instances of the object.
   * When all_reduce2() returns, the "data"
   * variable will contain a value corresponding to adding up the objects
   * contributed by each machine using the plusequal function.
   *
   * Where U is the type of the object, the plusequal function must be of
   * the form:
   * \code
   * void plusequal(U& left, const U& right);
   * \endcode
   * and must implement the equivalent of <code>left += right; </code>
   *
   * Example:
   * \code
   * void int_plus_equal(int& a, const int& b) {
   *  a+=b;
   * }
   *
   * int i = 1;
   * dc.all_reduce2(i, int_plus_equal);
   * // since each machine contributed the value "1",
   * // all machines will have i = numprocs() here.
   * \endcode
   *
   * \param data  A piece of data to perform a reduction over.
   * \param plusequal A plusequal function on the data. Must have the prototype
   *                  void plusequal(U&, const U&)
   * \param control Optional parameter. Defaults to false. If set to true,
   *                this will marked as control plane communication and will
   *                not register in bytes_received() or bytes_sent(). This must
   *                be the same on all machines.
   */
  template <typename U, typename PlusEqual>
  inline void all_reduce2(U& data, PlusEqual plusequal, bool control = false);


   /**
    \brief A distributed barrier which waits for all machines to call the
          barrier() function before proceeding.

    A machine calling the barrier() will wait until every machine
    reaches this barrier before continuing. Only one thread from each machine
    should call the barrier.

    \see full_barrier
    */
  void barrier();




 /*****************************************************************************
                      Implementation of Full Barrier
*****************************************************************************/
  /**
   * \brief A distributed barrier which waits for all machines to call
   * the full_barrier() function before proceeding. Also waits for all
   * previously issued remote calls to complete.

   Similar to the barrier(), but provides additional guarantees that
   all calls issued prior to this barrier are completed before
  i returning.

  \note This function could return prematurely if
  other threads are still issuing function calls since we
  cannot differentiate between calls issued before the barrier
  and calls issued while the barrier is being evaluated.
  Therefore, when used in a multithreaded scenario, the user must ensure
  that all other threads which may perform operations using this object
  are stopped before the full barrier is initated.

  \see barrier
  */
  void full_barrier();


  /**
   * \brief A wrapper on cout, that outputs only on machine 0
   */
  std::ostream& cout() const {
    if (procid() == 0) return std::cout;
    else return nullstrm;
  }

  /**
   * \brief A wrapper on cerr, that outputs only on machine 0
   */
  std::ostream& cerr() const {
    if (procid() == 0) return std::cerr;
    else return nullstrm;
  }

 private:
  mutex full_barrier_lock;
  fiber_conditional full_barrier_cond;
  std::vector<size_t> calls_to_receive;
  // used to inform the counter that the full barrier
  // is in effect and all modifications to the calls_recv
  // counter will need to lock and signal
  volatile bool full_barrier_in_effect;

  /** number of 'source' processor counts which have
  not achieved the right recv count */
  atomic<size_t> num_proc_recvs_incomplete;

  /// Marked as 1 if the proc is complete
  dense_bitset procs_complete;
   ///\internal
  mutable boost::iostreams::stream<boost::iostreams::null_sink> nullstrm;

 /*****************************************************************************
                      Collection of Statistics
*****************************************************************************/

 private:
  struct collected_statistics {
    size_t callssent;
    size_t bytessent;
    size_t network_bytessent;
    collected_statistics(): callssent(0), bytessent(0), network_bytessent(0) { }
    void save(oarchive &oarc) const {
      oarc << callssent << bytessent << network_bytessent;
    }
    void load(iarchive &iarc) {
      iarc >> callssent >> bytessent >> network_bytessent;
    }
  };
 public:
  /** Gather RPC statistics. All machines must call
   this function at the same time. However, only proc 0 will
   return values */
  std::map<std::string, size_t> gather_statistics();
};




} // namespace graphlab

#define REGISTER_RPC(dc, f) dc.register_rpc<typeof(f)*, f>(std::string(BOOST_PP_STRINGIZE(f)))

#include <rpc/function_arg_types_undef.hpp>
#include <rpc/function_call_dispatch.hpp>
#include <rpc/request_dispatch.hpp>
#include <rpc/dc_dist_object.hpp>
#include <rpc/dc_services.hpp>

namespace graphlab {

template <typename U>
inline void distributed_control::send_to(procid_t target, U& t, bool control) {
  distributed_services->send_to(target, t, control);
}

template <typename U>
inline void distributed_control::recv_from(procid_t source, U& t, bool control) {
  distributed_services->recv_from(source, t, control);
}

template <typename U>
inline void distributed_control::broadcast(U& data, bool originator, bool control) {
  distributed_services->broadcast(data, originator, control);
}

template <typename U>
inline void distributed_control::gather(std::vector<U>& data, procid_t sendto, bool control) {
  distributed_services->gather(data, sendto, control);
}

template <typename U>
inline void distributed_control::all_gather(std::vector<U>& data, bool control) {
  distributed_services->all_gather(data, control);
}

template <typename U>
inline void distributed_control::all_reduce(U& data, bool control) {
  distributed_services->all_reduce(data, control);
}


template <typename U, typename PlusEqual>
inline void distributed_control::all_reduce2(U& data, PlusEqual plusequal, bool control) {
  distributed_services->all_reduce2(data, plusequal, control);
}




}

#include <rpc/mpi_tools.hpp>

#endif

