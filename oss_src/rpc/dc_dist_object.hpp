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


#include <rpc/dc.hpp>

#ifndef GRAPHLAB_DC_DIST_OBJECT_HPP
#define GRAPHLAB_DC_DIST_OBJECT_HPP
#include <vector>
#include <string>
#include <set>
#include <parallel/atomic.hpp>
#include <fiber/fiber_conditional.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/dc_dist_object_base.hpp>
#include <rpc/object_request_issue.hpp>
#include <rpc/object_call_issue.hpp>
#include <rpc/object_broadcast_issue.hpp>
#include <rpc/function_ret_type.hpp>
#include <rpc/mem_function_arg_types_def.hpp>
#include <graphlab/util/charstream.hpp>
#include <boost/preprocessor.hpp>
#include <perf/tracepoint.hpp>
#include <rpc/request_reply_handler.hpp>
#include <rpc/dc_macros.hpp>
#include <rpc/fiber_remote_request.hpp>
#include <graphlab/macros_def.hpp>
#include <parallel/mutex.hpp>

#define BARRIER_BRANCH_FACTOR 128


namespace graphlab {

/** This is needed to patch some issues with the inproc cluster use;
 *  the constructor of a distributed object does not seem to be
 *  threadsafe; this seems to make it work reliably.
 */
extern mutex distributed_object_construction_lock;

/**
\ingroup rpc
\brief Provides a class with its own distributed communication context, allowing
instances of the class to communicate with other remote instances.

The philosophy behind the dc_dist_object is the concept of "distributed
objects". The idea is that the user should be able to write code:

\code
void main() {
  // ... initialization of a distributed_control object dc ...

  distributed_vector vec(dc), vec2(dc);
  distributed_graph g(dc);
}
\endcode
where if run in a distributed setting, the "vec" variable, can behave as if it
is a single distributed object, and automatically coordinate its operations
across the network; communicating with the other instances of "vec" on the
other machines.  Essentially, each object (vec, vec2 and g) constructs its own
private communication context, which allows every machine's "vec" variable to
communicate only with other machine's "vec" variable. And similarly for "vec2"
and "g". This private communication context is provided by this dc_dist_object
class.

To construct a distributed object requires little work:
\code
class distributed_int_vector {
  private:
    // creates a local dc_dist_object context
    graphlab::dc_dist_object<distributed_int_vector> rmi;

  public:
    // context must be initialized on construction with the
    // root distributed_control object
    distributed_int_vector(distributed_control& dc): rmi(dc, this) {
      ... other initialization ...
      // make sure all machines finish constructing this object
      // before continuing
      rmi.barrier();
    }
};
\endcode

After which remote_call(), and remote_request() can be used to communicate
across the network with the same matching instance of the
distributed_int_vector.

Each dc_dist_object maintains its own private communication context which
is not influences by other communication contexts. In other words, the
<code>rmi.barrier()</code>, and all other operations in each instance of the
distributed_int_vector are independent of each other. In particular, the
<code>rmi.full_barrier()</code> only waits for completion of all RPC calls
from within the current communication context.

See the examples in \ref RPC for more usage examples.

\note While there is no real limit to the number of distributed
objects that can be created. However, each dc_dist_object does contain
a reasonably large amount of state, so frequent construction and deletion
of objects is not recommended.
*/
template <typename T>
class dc_dist_object : public dc_impl::dc_dist_object_base{
 private:
  distributed_control &dc_;
  size_t obj_id;
  size_t control_obj_id;  // object id of this object
  T* owner;
  std::vector<atomic<size_t> > callsreceived;
  std::vector<atomic<size_t> > callssent;
  std::vector<atomic<size_t> > bytessent;
  // make operator= private
  dc_dist_object<T>& operator=(const dc_dist_object<T> &d) {return *this;}
  friend class distributed_control;


  DECLARE_TRACER(distobj_remote_call_time);


 public:

  /// \cond GRAPHLAB_INTERNAL

  /// Should not be used by the user
  void inc_calls_received(procid_t p) {
    if (!full_barrier_in_effect) {
        size_t t = callsreceived[p].inc();
        if (full_barrier_in_effect) {
          if (t == calls_to_receive[p]) {
            // if it was me who set the bit
            if (procs_complete.set_bit(p) == false) {
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
      if (callsreceived[p].inc() == calls_to_receive[p]) {
        // if it was me who set the bit
        if (procs_complete.set_bit(p) == false) {
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

  /// Should not be used by the user
  void inc_calls_sent(procid_t p) {
    callssent[p].inc();
  }

  /// Should not be used by the user
  void inc_bytes_sent(procid_t p, size_t bytes) {
    bytessent[p].inc(bytes);
  }

  /// Should not be used by the user
  size_t get_obj_id() const {
    return obj_id;
  }
  /// \endcond GRAPHLAB_INTERNAL
 public:

  /**
   * \brief Constructs a distributed object context.
   *
   * The constructor constructs a distributed object context which is
   * associated with the "owner" object.
   *
   * \param dc_ The root distributed_control which provides the
   *            communication control plane.
   * \param owner The object to associate with
   */
  dc_dist_object(distributed_control &dc_, T* owner):
    dc_(dc_),owner(owner) {
    std::lock_guard<mutex> lg(distributed_object_construction_lock);
    callssent.resize(dc_.numprocs());
    callsreceived.resize(dc_.numprocs());
    bytessent.resize(dc_.numprocs());
    //------ Initialize the matched send/recv ------
    recv_froms.resize(dc_.numprocs());
    //------ Initialize the gatherer ------
    gather_receive.resize(dc_.numprocs());


    //------- Initialize the Barrier ----------
    child_barrier_counter.value = 0;
    barrier_sense = 1;
    barrier_release = -1;


    // compute my children
    childbase = size_t(dc_.procid()) * BARRIER_BRANCH_FACTOR + 1;
    if (childbase >= dc_.numprocs()) {
      numchild = 0;
    }
    else {
      size_t maxchild = std::min<size_t>(dc_.numprocs(),
                                         childbase + BARRIER_BRANCH_FACTOR);
      numchild = (procid_t)(maxchild - childbase);
    }

    parent =  (procid_t)((dc_.procid() - 1) / BARRIER_BRANCH_FACTOR)   ;

    //-------- Initialize all gather --------------
    ab_child_barrier_counter.value = 0;
    ab_barrier_sense = 1;
    ab_barrier_release = -1;


    //-------- Initialize the full barrier ---------

    full_barrier_in_effect = false;
    procs_complete.resize(dc_.numprocs());

    // register
    obj_id = dc_.register_object(owner, this);
    control_obj_id = dc_.register_object(this, this);

    //-------- Initialize Tracer
    std::string name = typeid(T).name();
    INITIALIZE_TRACER(distobj_remote_call_time,
                      std::string("dc_dist_object ") + name + ": remote_call time");
  }

  /// \brief The number of function calls received by this object
  size_t calls_received() const {
    size_t ctr = 0;
    for (size_t i = 0;i < numprocs(); ++i) {
      ctr += callsreceived[i].value;
    }
    return ctr;
  }

  /// \brief The number of function calls sent from this object
  size_t calls_sent() const {
    size_t ctr = 0;
    for (size_t i = 0;i < numprocs(); ++i) {
      ctr += callssent[i].value;
    }
    return ctr;
  }

  /** \brief The number of bytes sent from this object, excluding
   * headers and other control overhead.
   */
  size_t bytes_sent() const {
    size_t ctr = 0;
    for (size_t i = 0;i < numprocs(); ++i) {
      ctr += bytessent[i].value;
    }
    return ctr;
  }

  /// \brief A reference to the underlying distributed_control object
  distributed_control& dc() {
    return dc_;
  }

  /// \brief A const reference to the underlying distributed_control object
  const distributed_control& dc() const {
    return dc_;
  }

  /// \brief The current process ID
  inline procid_t procid() const {
    return dc_.procid();
  }

  /// \brief The number of processes in the distributed program.
  inline procid_t numprocs() const {
    return dc_.numprocs();
  }

  /**
   * \brief A wrapper on cout, that outputs only on machine 0
   */
  std::ostream& cout() const {
    return dc_.cout();
  }

  /**
   * \brief A wrapper on cerr, that outputs only on machine 0
   */
  std::ostream& cerr() const {
    return dc_.cout();
  }

  /// \cond GRAPHLAB_INTERNAL

    /*
  This generates the interface functions for the standard calls, basic calls
  The function looks like this:
  \code
  template<typename F, F remote_function, typename T0> 
  void remote_call (procid_t target, T0 i0 )
  {
      ASSERT_LT(target, dc_.senders.size());
      if ((STANDARD_CALL & CONTROL_PACKET) == 0) inc_calls_sent(target);
      dc_impl::object_call_issue1 <T, F, remote_function, T0> ::exec(dc_.senders[target],
                                                      STANDARD_CALL,
                                                      target,obj_id,
                                                      remote_function ,
                                                      i0 );
  }

  The argument to the RPC_INTERFACE_GENERATOR are:
    - the name of the rpc call ("remote_call" in the first one)
    - the name of the issueing processor ("object_call_issue")
    - The flags to set on the call ("STANDARD_CALL")

  \endcode
  */
  #define GENARGS(Z,N,_)  BOOST_PP_CAT(T, N) BOOST_PP_CAT(i, N)
  #define GENI(Z,N,_) BOOST_PP_CAT(i, N)
  #define GENT(Z,N,_) BOOST_PP_CAT(T, N)
  #define GENARC(Z,N,_) arc << BOOST_PP_CAT(i, N);

  #define RPC_INTERFACE_GENERATOR(Z,N,FNAME_AND_CALL) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
  void  BOOST_PP_TUPLE_ELEM(3,0,FNAME_AND_CALL) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, dc_.senders.size()); \
    BEGIN_TRACEPOINT(distobj_remote_call_time); \
    if ((BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL) & CONTROL_PACKET) == 0) inc_calls_sent(target); \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(3,1,FNAME_AND_CALL),N) \
        <T, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(this, dc_.senders[target],  BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL), target,obj_id BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
    END_TRACEPOINT(distobj_remote_call_time); \
  }   \

  /*
  Generates the interface functions. 3rd argument is a tuple (interface name, issue name, flags)
  */
  BOOST_PP_REPEAT(7, RPC_INTERFACE_GENERATOR, (remote_call, dc_impl::object_call_issue, STANDARD_CALL) )
  BOOST_PP_REPEAT(7, RPC_INTERFACE_GENERATOR, (control_call,dc_impl::object_call_issue, (STANDARD_CALL | CONTROL_PACKET)) )

  /**
   * This generates a "split call". Where the header of the call message
   * is written to with split_call_begin, and the message actually sent with
   * split_call_end(). It is then up to the user to serialize the message arguments
   * into the oarchive returned. The split call can provide performance gains 
   * when the contents of the message are large, since this allows the user to
   * control the serialization process. 
   *
   * Example:
   * \code
   * struct mystruct {
   *   void function_to_call(size_t len, wild_pointer w) { 
   *      // w will contain all the serialized contents of ..stuff...
   *   }
   *
   *   void stuff() {
   *     oarchive* oarc = rmi.RPC_CALL(split_call_begin,mystruct::function_to_call)();
   *     (*oarc) << ... stuff...
   *     rmi.RPC_CALL(split_call_end,my_struct::function_to_call)
   *                       (1,  // to machine 1
   *                        oarc);
   *     
   *   }
   * }
   * \endcode
   */
  template <typename unused, void (T::*remote_function)(size_t, wild_pointer)>
  oarchive* split_call_begin() {
    return dc_impl::object_split_call<T, void(T::*)(size_t, wild_pointer), remote_function>::split_call_begin(this, obj_id);
  }

  /**
   * Sends a split call started by \ref split_call_begin
   * See \ref split_call_begin for details.
   */
  template <typename unused, void (T::*remote_function)(size_t, wild_pointer)>
  void split_call_end(procid_t target, oarchive* oarc) {
    inc_calls_sent(target);
    return dc_impl::object_split_call<T, void(T::*)(size_t, wild_pointer),
           remote_function>::split_call_end(this, oarc, dc_.senders[target],
                                            target, STANDARD_CALL);
  }

  /**
   * Cancels a split call began with split_call_begin
   */
  template <typename unused, void (T::*remote_function)(size_t, wild_pointer)>
  void split_call_cancel(oarchive* oarc) {
    return dc_impl::object_split_call<T, void(T::*)(size_t, wild_pointer), remote_function>::split_call_cancel(oarc);
  }



  #define BROADCAST_INTERFACE_GENERATOR(Z,N,FNAME_AND_CALL) \
  template<typename F, F remote_function, typename Iterator BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
  void  BOOST_PP_TUPLE_ELEM(3,0,FNAME_AND_CALL) (Iterator target_begin, Iterator target_end \
                      BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    if (target_begin == target_end) return;               \
    BEGIN_TRACEPOINT(distobj_remote_call_time); \
    if ((BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL) & CONTROL_PACKET) == 0) {            \
      Iterator iter = target_begin;       \
      while (iter != target_end){         \
        inc_calls_sent(*iter);            \
        ++iter;                           \
      }                                   \
    }                                     \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(3,1,FNAME_AND_CALL),N) \
        <Iterator, T, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(this, dc_.senders,  BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL), target_begin, target_end,obj_id BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
    END_TRACEPOINT(distobj_remote_call_time); \
  }

  BOOST_PP_REPEAT(7, BROADCAST_INTERFACE_GENERATOR, (broadcast_call, dc_impl::object_broadcast_issue, STANDARD_CALL) )

  /*
  The generation procedure for requests are the same. The only
  difference is that the function name has to be changed a little to
  be identify the return type of the function, (typename
  dc_impl::function_ret_type<__GLRPC_FRESULT>) and the issuing
  processor is object_request_issue.

    The call can be issued with
    \code
    ret = rmi.remote_request(target,
                              &object_type::function_name,
                              arg1,
                              arg2...)
    \endcode
  */
#define CUSTOM_REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
    BOOST_PP_TUPLE_ELEM(2,0,ARGS) (procid_t target, size_t handle, unsigned char flags BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, dc_.senders.size()); \
    if ((flags & CONTROL_PACKET) == 0) inc_calls_sent(target); \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(2,1,ARGS),N) \
        <T, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(this, dc_.senders[target],  handle, flags, target,obj_id BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
  }


#define FUTURE_REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
    BOOST_PP_TUPLE_ELEM(2,0,ARGS) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, dc_.senders.size()); \
    request_future<__GLRPC_FRESULT> reply;      \
    custom_remote_request<F, remote_function>(target, reply.get_handle(), BOOST_PP_TUPLE_ELEM(2,1,ARGS) BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
    return reply; \
  }   

  #define REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
    BOOST_PP_TUPLE_ELEM(2,0,ARGS) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, dc_.senders.size()); \
    request_future<__GLRPC_FRESULT> reply;      \
    custom_remote_request<F, remote_function>(target, reply.get_handle(), BOOST_PP_TUPLE_ELEM(2,1,ARGS) BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
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
  Generates the interface functions. 3rd argument is a tuple
  (interface name, issue name, flags)
  */
 BOOST_PP_REPEAT(6, CUSTOM_REQUEST_INTERFACE_GENERATOR, (void custom_remote_request, dc_impl::object_request_issue) )
 BOOST_PP_REPEAT(6, REQUEST_INTERFACE_GENERATOR, (typename dc_impl::function_ret_type<__GLRPC_FRESULT>::type remote_request, (STANDARD_CALL | FLUSH_PACKET)) )
 BOOST_PP_REPEAT(6, FUTURE_REQUEST_INTERFACE_GENERATOR, (request_future<__GLRPC_FRESULT> future_remote_request, (STANDARD_CALL)) )
 BOOST_PP_REPEAT(6, FIBER_REQUEST_INTERFACE_GENERATOR, (request_future<__GLRPC_FRESULT> fiber_remote_request) )



  #undef RPC_INTERFACE_GENERATOR
  #undef BROADCAST_INTERFACE_GENERATOR
  #undef REQUEST_INTERFACE_GENERATOR
  #undef CUSTOM_REQUEST_INTERFACE_GENERATOR
  #undef FUTURE_REQUEST_INTERFACE_GENERATOR
  /* Now generate the interface functions which allow me to call this
  dc_dist_object directly The internal calls are similar to the ones
  above. The only difference is that is that instead of 'obj_id', the
  parameter passed to the issue processor is "control_obj_id" which
  identifies the current RMI class.
  */
  #define RPC_INTERFACE_GENERATOR(Z,N,FNAME_AND_CALL) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
  void  BOOST_PP_TUPLE_ELEM(3,0,FNAME_AND_CALL) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, dc_.senders.size()); \
    if ((BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL) & CONTROL_PACKET) == 0) inc_calls_sent(target); \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(3,1,FNAME_AND_CALL),N) \
        <dc_dist_object<T>, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(this, dc_.senders[target],  BOOST_PP_TUPLE_ELEM(3,2,FNAME_AND_CALL), target,control_obj_id BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
  }   \

  BOOST_PP_REPEAT(6, RPC_INTERFACE_GENERATOR, (internal_call,dc_impl::object_call_issue, STANDARD_CALL) )
  BOOST_PP_REPEAT(6, RPC_INTERFACE_GENERATOR, (internal_control_call,dc_impl::object_call_issue, (STANDARD_CALL | CONTROL_PACKET)) )


  #define REQUEST_INTERFACE_GENERATOR(Z,N,ARGS) \
  template<typename F, F remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, typename T)> \
    BOOST_PP_TUPLE_ELEM(3,0,ARGS) (procid_t target BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENARGS ,_) ) {  \
    ASSERT_LT(target, dc_.senders.size()); \
    request_future<__GLRPC_FRESULT> reply;      \
    if ((BOOST_PP_TUPLE_ELEM(3,2,ARGS) & CONTROL_PACKET) == 0) inc_calls_sent(target); \
    BOOST_PP_CAT( BOOST_PP_TUPLE_ELEM(3,1,ARGS),N) \
        <dc_dist_object<T>, F, remote_function BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM_PARAMS(N, T)> \
          ::exec(this, dc_.senders[target],  reply.get_handle(), BOOST_PP_TUPLE_ELEM(3,2,ARGS), target,control_obj_id BOOST_PP_COMMA_IF(N) BOOST_PP_ENUM(N,GENI ,_) ); \
    return reply(); \
  }   \

  /*
  Generates the interface functions. 3rd argument is a tuple (interface name, issue name, flags)
  */
  BOOST_PP_REPEAT(6, REQUEST_INTERFACE_GENERATOR, (typename dc_impl::function_ret_type<__GLRPC_FRESULT>::type internal_request, dc_impl::object_request_issue, (STANDARD_CALL)) )
  BOOST_PP_REPEAT(6, REQUEST_INTERFACE_GENERATOR, (typename dc_impl::function_ret_type<__GLRPC_FRESULT>::type internal_control_request, dc_impl::object_request_issue, (STANDARD_CALL | CONTROL_PACKET)) )


  #undef RPC_INTERFACE_GENERATOR
  #undef REQUEST_INTERFACE_GENERATOR
  #undef GENARC
  #undef GENT
  #undef GENI
  #undef GENARGS

 /// \endcond

#if DOXYGEN_DOCUMENTATION

/**
 * \brief Performs a non-blocking RPC call to the target machine
 * to run the provided function pointer.
 *
 * remote_call() calls the function "fn" on a target remote machine.
 * "fn" may be public, private or protected within the owner class; there are
 * no access restrictions. Provided arguments are serialized and sent to the
 * target.  Therefore, all arguments are necessarily transmitted by value.
 * If the target function has a return value, the return value is lost.
 *
 * remote_call() is non-blocking and does not wait for the target machine
 * to complete execution of the function. Different remote_calls may be handled
 * by different threads on the target machine and thus the target function
 * should be made thread-safe.
 * Alternatively, see distributed_control::set_sequentialization_key()
 * to force sequentialization of groups of remote calls.
 *
 * If blocking operation is desired, remote_request() may be used.
 * Alternatively, a full_barrier() may also be used to wait for completion of
 * all incomplete RPC calls.
 *
 * Example:
 * \code
 * // A print function is defined in the distributed object
 * class distributed_obj_example {
 *  graphlab::dc_dist_object<distributed_obj_example> rmi;
 *   ... initialization and constructor ...
 *  private:
 *    void print(std::string s) {
 *       std::cout << s << "\n";
 *    }
 *  public:
 *    void print_on_machine_one(std::string s) {
 *      // calls the print function on machine 1 with the argument "s"
 *      rmi.RPC_CALL(remote_call, distributed_obj_example::print)(1, s);
 *      // this is equivalent to
 *      rmi.template remote_call<decltype(&distributed_obj_example::print)
 *                               distributed_obj_example::print>(1, s);
 *    }
 * }
 * \endcode
 *
 * Note the syntax for obtaining a pointer to a member function.
 * \tparam F Type of function
 * \tparam fn Pointer to the member function to run on the target machine
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
 *  remote_call<F, fn>(*machine_begin, ...);
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
 * should be made thread-safe. Alternatively, see
 * distributed_control::set_sequentialization_key() to force sequentialization
 * of groups of remote_calls. A full_barrier()
 * may also be issued to wait for completion of all RPC calls issued prior
 * to the full barrier.
 *
 * Example:
 * \code
 * // A print function is defined in the distributed object
 * class distributed_obj_example {
 *  graphlab::dc_dist_object<distributed_obj_example> rmi;
 *   ... initialization and constructor ...
 *  private:
 *    void print(std::string s) {
 *       std::cout << s << "\n";
 *    }
 *  public:
 *    void print_on_some_machines(std::string s) {
 *      std::vector<procid_t> procs;
 *      procs.push_back(1); procs.push_back(3); procs.push_back(5);
 *
 *      // calls the print function on machine 1,3,5 with the argument "s"
 *      rmi.RPC_CALL(broadcast_call, distributed_obj_example::print)(1, s);
 *
 *      // this is equivalent to
 *      rmi.
 *        template broadcast_call<decltype(&distributed_obj_example::print)
 *                                distributed_obj_example::print> 
 *              (procs.begin(), procs.end(), s);
 *    }
 * }
 * \endcode
 *
 *
 * \tparam F Type of function
 * \tparam fn Pointer to the member function to run on the target machine
 * \param machine_begin The beginning of an iterator range containing a list
 *                      machines to call.  Iterator::value_type must be
 *                      castable to procid_t.
 * \param machine_end   The end of an iterator range containing a list
 *                      machines to call.  Iterator::value_type must be
 *                      castable to procid_t.
 *            member function in the owning object.
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 */
  template <typename F, F fn>
  void broadcast_call(Iterator machine_begin, Iterator machine_end, ...);


/**
 * \brief Performs a blocking RPC call to the target machine
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
 * // A print function is defined in the distributed object
 * class distributed_obj_example {
 *  graphlab::dc_dist_object<distributed_obj_example> rmi;
 *   ... initialization and constructor ...
 *  private:
 *    int add_one(int i) {
 *      return i + 1;
 *    }
 *  public:
 *    int add_one_from_machine_1(int i) {
 *      // calls the add_one function on machine 1 with the argument i
 *      return rmi.RPC_CALL(remote_request, distributed_obj_example::add_one)(1, i);
 *      // this is equivalent to
 *      rmi.template remote_request<decltype(&distributed_obj_example::add_one)
 *                               distributed_obj_example::add_one>(1, i);
 *    }
 * }
 * \endcode
 *
 * \see graphlab::dc_dist_object::fiber_remote_request
 *      graphlab::dc_dist_object::future_remote_request
 *
 * \tparam F Type of function
 * \tparam fn Pointer to the member function to run on the target machine
 * \param targetmachine The ID of the machine to run the function on
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 *
 * \returns Returns the same return type as the function fn
 */
 template <typename F, F fn>
 RetVal remote_request(procid_t targetmachine, ...);



/**
 * \brief Performs a nonblocking RPC request to the target machine
 * to run the provided function pointer which has an expected return value.
 *
 * future_remote_request() calls the function "fn" on a target remote machine.
 * Provided arguments are serialized and sent to the target.
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
 * // A print function is defined in the distributed object
 * class distributed_obj_example {
 *  graphlab::dc_dist_object<distributed_obj_example> rmi;
 *   ... initialization and constructor ...
 *  private:
 *    int add_one(int i) {
 *      return i + 1;
 *    }
 *  public:
 *    int add_one_from_machine_1(int i) {
 *      // calls the add_one function on machine 1 with the argument i
 *      // this call returns immediately
 *
 *      graphlab::request_future<int> future = 
 *          rmi.RPC_CALL(future_remote_request, distributed_obj_example::add_one)(1, i);
 *      // this is equivalent to
 *      graphlab::request_future<int> future = 
 *          rmi.template future_remote_request<decltype(&distributed_obj_example::add_one)
 *                               distributed_obj_example::add_one>(1, i);
 *
 *      // ... we can do other stuff here
 *      // then when we want the answer
 *      int result = future();
 *      return result;
 *    }
 * }
 * \endcode
 *
 * \see graphlab::dc_dist_object::fiber_remote_request
 *      graphlab::dc_dist_object::remote_request
 *
 * \tparam F Type of function
 * \tparam fn Pointer to the member function to run on the target machine
 * \param targetmachine The ID of the machine to run the function on
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 *
 * \returns Returns a future templated around the same type as the return 
 *          value of the called function
 */
 template <typename F, F fn>
 request_future<RetVal> future_remote_request(procid_t targetmachine, ...);

/**
 * \brief Performs a nonblocking RPC request to the target machine
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
 * \ref graphlab::dc_dist_object::future_remote_request() , but has the 
 * additional capability that if a \ref graphlab::request_future::wait() is 
 * called on the request while within a fiber, it deschedules the fiber and
 * context switches, returning only when the future is ready. This allows
 * the future to be used from within a fiber.
 *
 * Example:
 * \code
 * // A print function is defined in the distributed object
 * class distributed_obj_example {
 *  graphlab::dc_dist_object<distributed_obj_example> rmi;
 *   ... initialization and constructor ...
 *  private:
 *    int add_one(int i) {
 *      return i + 1;
 *    }
 *  public:
 *    int add_one_from_machine_1(int i) {
 *      // calls the add_one function on machine 1 with the argument i
 *      // this call returns immediately
 *      graphlab::request_future<int> future = 
 *          rmi.RPC_CALL(fiber_remote_request, distributed_obj_example::add_one)(1, i);
 *      // this is equivalent to
 *      graphlab::request_future<int> future = 
 *          rmi.template fiber_remote_request<decltype(&distributed_obj_example::add_one)
 *                               distributed_obj_example::add_one>(1, i);
 *
 *      // ... we can do other stuff here
 *      // then when we want the answer
 *      int result = future();
 *      return result;
 *    }
 * }
 * \endcode
 *
 * \see graphlab::dc_dist_object::remote_request
 *      graphlab::dc_dist_object::future_remote_request
 *      graphlab::distributed_control::fiber_remote_request
 *
 * \tparam F Type of function
 * \tparam fn Pointer to the member function to run on the target machine
 * \param targetmachine The ID of the machine to run the function on
 * \param ... The arguments to send to Fn. Arguments must be serializable.
 *            and must be castable to the target types.
 *
 * \returns Returns a future templated around the same type as the return 
 *          value of the called function
 */
 template <typename F, F fn>
 request_future<RetVal> fiber_remote_request(procid_t targetmachine, ...);



#endif
/*****************************************************************************
                      Implementation of matched send_to / recv_from
 *****************************************************************************/


 private:
  std::vector<dc_impl::recv_from_struct> recv_froms;

  void block_and_wait_for_recv(size_t src,
                             std::string& str,
                             size_t tag) {
    recv_froms[src].lock.lock();
    recv_froms[src].data = str;
    recv_froms[src].tag = tag;
    recv_froms[src].hasdata = true;
    recv_froms[src].cond.signal();
    recv_froms[src].lock.unlock();
  }

 public:

  /**
    \copydoc distributed_control::send_to()
  */
  template <typename U>
  void send_to(procid_t target, U& t, bool control = false) {
    std::stringstream strm;
    oarchive oarc(strm);
    oarc << t;
    strm.flush();
    dc_impl::basic_reply_container rt;
    // I shouldn't use a request to block here since
    // that will take up a thread on the remote side
    // so I simulate a request here.
    size_t rtptr = reinterpret_cast<size_t>(&rt);
    if (control == false) {
      this->RPC_CALL(internal_call, dc_dist_object<T>::block_and_wait_for_recv)
               (target, procid(), strm.str(), rtptr);
    }
    else {
      this->RPC_CALL(internal_control_call, dc_dist_object<T>::block_and_wait_for_recv)
               (target, procid(), strm.str(), rtptr);
    }
    // wait for reply
    rt.wait();

    if (control == false) inc_calls_sent(target);
  }


  /**
    \copydoc distributed_control::recv_from()
  */
  template <typename U>
  void recv_from(procid_t source, U& t, bool control = false) {
    // wait on the condition variable until I have data
    dc_impl::recv_from_struct &recvstruct = recv_froms[source];
    recvstruct.lock.lock();
    while (recvstruct.hasdata == false) {
      recvstruct.cond.wait(recvstruct.lock);
    }

    // got the data. deserialize it
    std::stringstream strm(recvstruct.data);
    iarchive iarc(strm);
    iarc >> t;
    // clear the data
    std::string("").swap(recvstruct.data);
    // remember the tag so we can unlock it before the remote call
    size_t tag = recvstruct.tag;
    // clear the has data flag
    recvstruct.hasdata = false;
    // unlock
    recvstruct.lock.unlock();
    if (control == false) {
      // remote call to release the sender. Use an empty blob
      dc_.RPC_CALL(control_call, request_reply_handler)(source, tag, dc_impl::blob());
      // I have to increment the calls sent manually here
      // since the matched send/recv calls do not go through the
      // typical object calls. It goes through the DC, but I also want to charge
      // it to this object
      inc_calls_received(source);
    }
    else {
      dc_.RPC_CALL(control_call, request_reply_handler)(source, tag, dc_impl::blob());
    }
  }



/*****************************************************************************
                      Implementation of Broadcast
 *****************************************************************************/

private:

  std::string broadcast_receive;

  void set_broadcast_receive(const std::string &s) {
    broadcast_receive = s;
  }


 public:

  /// \copydoc distributed_control::broadcast()
  template <typename U>
  void broadcast(U& data, bool originator, bool control = false) {
    if (originator) {
      // construct the data stream
      std::stringstream strm;
      oarchive oarc(strm);
      oarc << data;
      strm.flush();
      broadcast_receive = strm.str();
      if (control == false) {
        for (size_t i = 0;i < numprocs(); ++i) {
          if (i != procid()) {
            this->RPC_CALL(internal_request,dc_dist_object<T>::set_broadcast_receive)
                (i, broadcast_receive);
          }
        }
      }
      else {
        for (size_t i = 0;i < numprocs(); ++i) {
          if (i != procid()) {
            this->RPC_CALL(internal_control_request,dc_dist_object<T>::set_broadcast_receive) 
                (i, broadcast_receive);
          }
        }
      }
    }

    // by the time originator gets here, all machines
    // will have received the data due to the broadcast_receive
    // set a barrier here.
    barrier();

    // all machines will now deserialize the data
    if (!originator) {
      std::stringstream strm(broadcast_receive);
      iarchive iarc(strm);
      iarc >> data;
    }
    barrier();
  }


/*****************************************************************************
      Implementation of Gather, all_gather
 *****************************************************************************/

 private:
  std::vector<std::string> gather_receive;
  atomic<size_t> gatherid;

  void set_gather_receive(procid_t source, const std::string &s, size_t gid) {
    while(gatherid.value != gid) sched_yield();
    gather_receive[source] = s;
  }
 public:

  /// \copydoc distributed_control::gather()
  template <typename U>
  void gather(std::vector<U>& data, procid_t sendto, bool control = false) {
    // if not root
    if (sendto != procid()) {
      std::stringstream strm( std::ios::out | std::ios::binary );
      oarchive oarc(strm);
      oarc << data[procid()];
      strm.flush();
      if (control == false) {
        this->RPC_CALL(internal_request, dc_dist_object<T>::set_gather_receive)
                       (sendto,
                        procid(),
                        strm.str(),
                        gatherid.value);
      }
      else {
        this->RPC_CALL(internal_control_request, dc_dist_object<T>::set_gather_receive)
                       (sendto,
                        procid(),
                        strm.str(),
                        gatherid.value);
      }
    }
    barrier();
    if (sendto == procid()) {
      // if I am the receiver
      for (procid_t i = 0; i < numprocs(); ++i) {
        if (i != procid()) {
          // receiving only from others
          std::stringstream strm(gather_receive[i],
                                 std::ios::in | std::ios::binary);
          assert(strm.good());
          iarchive iarc(strm);
          iarc >> data[i];
        }
      }
    }
    gatherid.inc();
    barrier();
  }

/********************************************************************
             Implementation of all gather
*********************************************************************/



 private:
  // ------- Sense reversing barrier data ----------
  /// The next value of the barrier. either +1 or -1
  int ab_barrier_sense;
  /// When this flag == the current barrier value. The barrier is complete
  int ab_barrier_release;
  /** when barrier sense is 1, barrier clears when
   * child_barrier_counter == numchild. When barrier sense is -1, barrier
   * clears when child_barrier_counter == 0;
   */
  atomic<int> ab_child_barrier_counter;
  /// condition variable and mutex protecting the barrier variables
  fiber_conditional ab_barrier_cond;
  mutex ab_barrier_mut;
  std::string ab_children_data[BARRIER_BRANCH_FACTOR];
  std::string ab_alldata;

  /**
    The child calls this function in the parent once the child enters the barrier
  */
  void __ab_child_to_parent_barrier_trigger(procid_t source, std::string collect) {
    ab_barrier_mut.lock();
    // assert childbase <= source <= childbase + BARRIER_BRANCH_FACTOR
    ASSERT_GE(source, childbase);
    ASSERT_LT(source, childbase + BARRIER_BRANCH_FACTOR);
    ab_children_data[source - childbase] = collect;
    ab_child_barrier_counter.inc(ab_barrier_sense);
    ab_barrier_cond.signal();
    ab_barrier_mut.unlock();
  }

  /**
    This is on the downward pass of the barrier. The parent calls this function
    to release all the children's barriers
  */
  void __ab_parent_to_child_barrier_release(int releaseval,
                                            std::string allstrings,
                                            int use_control_calls) {
    // send the release downwards
    // get my largest child
    logger(LOG_DEBUG, "AB Barrier Release %d\n", releaseval);
    ab_alldata = allstrings;
    for (procid_t i = 0;i < numchild; ++i) {
      if (use_control_calls) {
        this->RPC_CALL(internal_control_call, dc_dist_object<T>::__ab_parent_to_child_barrier_release)
                 ((procid_t)(childbase + i),
                              releaseval,
                              ab_alldata,
                              use_control_calls);
      }
      else {
        this->RPC_CALL(internal_call,dc_dist_object<T>::__ab_parent_to_child_barrier_release)
                 ((procid_t)(childbase + i),
                      releaseval,
                      ab_alldata,
                      use_control_calls);
      }
    }
    ab_barrier_mut.lock();
    ab_barrier_release = releaseval;
    ab_barrier_cond.signal();
    ab_barrier_mut.unlock();
  }


 public:

  /// \copydoc distributed_control::all_gather()
  template <typename U>
  void all_gather(std::vector<U>& data, bool control = false) {
    if (numprocs() == 1) return;
    // get the string representation of the data
    charstream strm(128);
    oarchive oarc(strm);
    oarc << data[procid()];
    strm.flush();
    // upward message
    int ab_barrier_val = ab_barrier_sense;
    ab_barrier_mut.lock();
    // wait for all children to be done
    while(1) {
      if ((ab_barrier_sense == -1 && ab_child_barrier_counter.value == 0) ||
          (ab_barrier_sense == 1 && ab_child_barrier_counter.value == (int)(numchild))) {
        // flip the barrier sense
        ab_barrier_sense = -ab_barrier_sense;
        // call child to parent in parent
        ab_barrier_mut.unlock();
        if (procid() != 0) {
          // collect all my children data
          charstream strstrm(128);
          oarchive oarc2(strstrm);
          oarc2 << std::string(strm->c_str(), strm->size());
          for (procid_t i = 0;i < numchild; ++i) {
            strstrm.write(ab_children_data[i].c_str(), ab_children_data[i].length());
          }
          strstrm.flush();
          if (control) {
            this->RPC_CALL(internal_control_call,dc_dist_object<T>::__ab_child_to_parent_barrier_trigger)
                     (parent,
                      procid(),
                      std::string(strstrm->c_str(), strstrm->size()));
          }
          else {
            this->RPC_CALL(internal_call,dc_dist_object<T>::__ab_child_to_parent_barrier_trigger)
                     (parent,
                      procid(),
                      std::string(strstrm->c_str(), strstrm->size()));
          }
        }
        break;
      }
      ab_barrier_cond.wait(ab_barrier_mut);
    }


    logger(LOG_DEBUG, "AB barrier phase 1 complete\n");
    // I am root. send the barrier release downwards
    if (procid() == 0) {
      ab_barrier_release = ab_barrier_val;
      // build the downward data
      charstream strstrm(128);
      oarchive oarc2(strstrm);
      oarc2 << std::string(strm->c_str(), strm->size());
      for (procid_t i = 0;i < numchild; ++i) {
        strstrm.write(ab_children_data[i].c_str(), ab_children_data[i].length());
      }
      strstrm.flush();
      ab_alldata = std::string(strstrm->c_str(), strstrm->size());
      for (procid_t i = 0;i < numchild; ++i) {
        logger(LOG_DEBUG, "Sending AB release to %d\n", childbase + i);
        this->RPC_CALL(internal_control_call, dc_dist_object<T>::__ab_parent_to_child_barrier_release)
                 ((procid_t)(childbase + i),
                             ab_barrier_val,
                             ab_alldata,
                             (int)control);

      }
    }
    // wait for the downward message releasing the barrier
    logger(LOG_DEBUG, "AB barrier waiting for %d\n", ab_barrier_val);
    ab_barrier_mut.lock();
    while(1) {
      if (ab_barrier_release == ab_barrier_val) break;
      ab_barrier_cond.wait(ab_barrier_mut);
    }
    // read the collected data and release the lock
    std::string local_ab_alldata = ab_alldata;
    ab_barrier_mut.unlock();

    logger(LOG_DEBUG, "barrier phase 2 complete\n");
    // now the data is a DFS search of a heap
    // I need to unpack it
    size_t heappos = 0;
    std::stringstream istrm(local_ab_alldata);
    iarchive iarc(istrm);

    for (size_t i = 0;i < numprocs(); ++i) {
      std::string s;
      iarc >> s;

      std::stringstream strm2(s);
      iarchive iarc2(strm2);
      iarc2 >> data[heappos];

      if (i + 1 == numprocs()) break;
      // advance heappos
      // leftbranch
      bool lefttraverseblock = false;
      while (1) {
        // can we continue going deaper down the left?
        size_t leftbranch = heappos * BARRIER_BRANCH_FACTOR + 1;
        if (lefttraverseblock == false && leftbranch < numprocs()) {
          heappos = leftbranch;
          break;
        }
        // ok. can't go down the left
        bool this_is_a_right_branch = (((heappos - 1) % BARRIER_BRANCH_FACTOR) == BARRIER_BRANCH_FACTOR - 1);
        // if we are a left branch, go to sibling
        if (this_is_a_right_branch == false) {
          size_t sibling = heappos + 1;
          if (sibling < numprocs()) {
            heappos = sibling;
            break;
          }
        }

        // we have finished this subtree, go back up to parent
        // and block the depth traversal on the next round
        // unless heappos is 0

        heappos = (heappos - 1) / BARRIER_BRANCH_FACTOR;
        lefttraverseblock = true;
        continue;
        // go to sibling
      }

    }
  }

  /// \copydoc distributed_control::all_reduce2()
  template <typename U, typename PlusEqual>
  void all_reduce2(U& data, PlusEqual plusequal, bool control = false) {
    if (numprocs() == 1) return;
    // get the string representation of the data
   /* charstream strm(128);
    oarchive oarc(strm);
    oarc << data;
    strm.flush();*/
    // upward message
    int ab_barrier_val = ab_barrier_sense;
    ab_barrier_mut.lock();
    // wait for all children to be done
    while(1) {
      if ((ab_barrier_sense == -1 && ab_child_barrier_counter.value == 0) ||
          (ab_barrier_sense == 1 && ab_child_barrier_counter.value == (int)(numchild))) {
        // flip the barrier sense
        ab_barrier_sense = -ab_barrier_sense;
        // call child to parent in parent
        ab_barrier_mut.unlock();
        if (procid() != 0) {
          // accumulate my children data
          for (procid_t i = 0;i < numchild; ++i) {
            std::stringstream istrm(ab_children_data[i]);
            iarchive iarc(istrm);
            U tmp;
            iarc >> tmp;
            plusequal(data, tmp);
          }
          // upward message
          charstream ostrm(128);
          oarchive oarc(ostrm);
          oarc << data;
          ostrm.flush();
          if (control) {
            this->RPC_CALL(internal_control_call,dc_dist_object<T>::__ab_child_to_parent_barrier_trigger)
                     (parent,
                            procid(),
                            std::string(ostrm->c_str(), ostrm->size()));
          }
          else {
            this->RPC_CALL(internal_call, dc_dist_object<T>::__ab_child_to_parent_barrier_trigger)
                     (parent,
                          procid(),
                          std::string(ostrm->c_str(), ostrm->size()));
          }
        }
        break;
      }
      ab_barrier_cond.wait(ab_barrier_mut);
    }


    logger(LOG_DEBUG, "AB barrier phase 1 complete\n");
    // I am root. send the barrier release downwards
    if (procid() == 0) {
      ab_barrier_release = ab_barrier_val;
      for (procid_t i = 0;i < numchild; ++i) {
        std::stringstream istrm(ab_children_data[i]);
        iarchive iarc(istrm);
        U tmp;
        iarc >> tmp;
        plusequal(data, tmp);
      }
      // build the downward data
      charstream ostrm(128);
      oarchive oarc(ostrm);
      oarc << data;
      ostrm.flush();
      ab_alldata = std::string(ostrm->c_str(), ostrm->size());
      for (procid_t i = 0;i < numchild; ++i) {
        this->RPC_CALL(internal_control_call,dc_dist_object<T>::__ab_parent_to_child_barrier_release)
                 ((procid_t)(childbase + i),
                             ab_barrier_val,
                             ab_alldata,
                             (int)control);

      }
    }
    // wait for the downward message releasing the barrier
    logger(LOG_DEBUG, "AB barrier waiting for %d\n", ab_barrier_val);
    ab_barrier_mut.lock();
    while(1) {
      if (ab_barrier_release == ab_barrier_val) break;
      ab_barrier_cond.wait(ab_barrier_mut);
    }

    if (procid() != 0) {
      // read the collected data and release the lock
      std::string local_ab_alldata = ab_alldata;
      ab_barrier_mut.unlock();

      logger(LOG_DEBUG, "barrier phase 2 complete\n");

      std::stringstream istrm(local_ab_alldata);
      iarchive iarc(istrm);
      iarc >> data;
    }
    else {
      ab_barrier_mut.unlock();
    }
  }


  template <typename U>
  struct default_plus_equal {
    void operator()(U& u, const U& v) {
      u += v;
    }
  };

  /// \copydoc distributed_control::all_reduce()
  template <typename U>
  void all_reduce(U& data, bool control = false) {
    all_reduce2(data, default_plus_equal<U>(), control);
  }

////////////////////////////////////////////////////////////////////////////


/*****************************************************************************
                      Implementation of All Scatter
 *****************************************************************************/

  template <typename U>
  void all_to_all(std::vector<U>& data, bool control = false) {
    ASSERT_EQ(data.size(), numprocs());
    for (size_t i = 0;i < data.size(); ++i) {
      if (i != procid()) {
        std::stringstream strm( std::ios::out | std::ios::binary );
        oarchive oarc(strm);
        oarc << data[i];
        strm.flush();
        if (control == false) {
          this->RPC_CALL(internal_call,dc_dist_object<T>::set_gather_receive)
                   (i,
                    procid(),
                    strm.str(),
                    gatherid.value);
        }
        else {
          this->RPC_CALL(internal_control_call, dc_dist_object<T>::set_gather_receive)
                   (i,
                    procid(),
                    strm.str(),
                    gatherid.value);
        }
      }
    }
    full_barrier();
    for (size_t i = 0; i < data.size(); ++i) {
      if (i != procid()) {
        std::stringstream strm(gather_receive[i],
                               std::ios::in | std::ios::binary);
        assert(strm.good());
        iarchive iarc(strm);
        iarc >> data[i];
      }
    }
    gatherid.inc();
    barrier();
  }


/*****************************************************************************
                      Implementation of Barrier
 *****************************************************************************/



 private:
  // ------- Sense reversing barrier data ----------
  /// The next value of the barrier. either +1 or -1
  int barrier_sense;
  /// When this flag == the current barrier value. The barrier is complete
  int barrier_release;
  /** when barrier sense is 1, barrier clears when
   * child_barrier_counter == numchild. When barrier sense is -1, barrier
   * clears when child_barrier_counter == 0;
   */
  atomic<int> child_barrier_counter;
  /// condition variable and mutex protecting the barrier variables
  fiber_conditional barrier_cond;
  mutex barrier_mut;
  procid_t parent;  /// parent node
  size_t childbase; /// id of my first child
  procid_t numchild;  /// number of children




  /**
    The child calls this function in the parent once the child enters the barrier
  */
  void __child_to_parent_barrier_trigger(procid_t source) {
    barrier_mut.lock();
    // assert childbase <= source <= childbase + BARRIER_BRANCH_FACTOR
    ASSERT_GE(source, childbase);
    ASSERT_LT(source, childbase + BARRIER_BRANCH_FACTOR);
    child_barrier_counter.inc(barrier_sense);
    barrier_cond.signal();
    barrier_mut.unlock();
  }

  /**
    This is on the downward pass of the barrier. The parent calls this function
    to release all the children's barriers
  */
  void __parent_to_child_barrier_release(int releaseval) {
    // send the release downwards
    // get my largest child
    logger(LOG_DEBUG, "Barrier Release %d\n", releaseval);
    for (procid_t i = 0;i < numchild; ++i) {
      this->RPC_CALL(internal_control_call,dc_dist_object<T>::__parent_to_child_barrier_release)
          ((procid_t)(childbase + i), releaseval);

    }
    barrier_mut.lock();
    barrier_release = releaseval;
    barrier_cond.signal();
    barrier_mut.unlock();
  }


 public:

  /// \copydoc distributed_control::barrier()
  void barrier() {
    // upward message
    int barrier_val = barrier_sense;
    barrier_mut.lock();
    // wait for all children to be done
    while(1) {
      if ((barrier_sense == -1 && child_barrier_counter.value == 0) ||
          (barrier_sense == 1 && child_barrier_counter.value == (int)(numchild))) {
        // flip the barrier sense
        barrier_sense = -barrier_sense;
        // call child to parent in parent
        barrier_mut.unlock();
        if (procid() != 0) {
          this->RPC_CALL(internal_control_call,dc_dist_object<T>::__child_to_parent_barrier_trigger)
                   (parent, procid());
        }
        break;
      }
      barrier_cond.wait(barrier_mut);
    }


    logger(LOG_DEBUG, "barrier phase 1 complete\n");
    // I am root. send the barrier release downwards
    if (procid() == 0) {
      barrier_release = barrier_val;

      for (procid_t i = 0;i < numchild; ++i) {
        this->RPC_CALL(internal_control_call, dc_dist_object<T>::__parent_to_child_barrier_release)
                 ((procid_t)(childbase + i), barrier_val);

      }
    }
    // wait for the downward message releasing the barrier
    logger(LOG_DEBUG, "barrier waiting for %d\n", barrier_val);
    barrier_mut.lock();
    while(1) {
      if (barrier_release == barrier_val) break;
      barrier_cond.wait(barrier_mut);
    }
    barrier_mut.unlock();

    logger(LOG_DEBUG, "barrier phase 2 complete\n");
  }


 /*****************************************************************************
                      Implementation of Full Barrier
*****************************************************************************/
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

 public:

  /// \copydoc distributed_control::full_barrier()
  void full_barrier() {
    // gather a sum of all the calls issued to machine 0
    std::vector<size_t> calls_sent_to_target(numprocs(), 0);
    for (size_t i = 0;i < numprocs(); ++i) {
      calls_sent_to_target[i] = callssent[i].value;
    }

    // tell node 0 how many calls there are
    std::vector<std::vector<size_t> > all_calls_sent(numprocs());
    all_calls_sent[procid()] = calls_sent_to_target;
    all_gather(all_calls_sent, true);

    // get the number of calls I am supposed to receive from each machine
    calls_to_receive.clear(); calls_to_receive.resize(numprocs(), 0);
    for (size_t i = 0;i < numprocs(); ++i) {
      calls_to_receive[i] += all_calls_sent[i][procid()];
    }
    // clear the counters
    num_proc_recvs_incomplete.value = numprocs();
    procs_complete.clear();
    // activate the full barrier
    full_barrier_in_effect = true;
    __asm("mfence");
    // begin one pass to set all which are already completed
    for (size_t i = 0;i < numprocs(); ++i) {
      if (callsreceived[i].value >= calls_to_receive[i]) {
        if (procs_complete.set_bit(i) == false) {
          num_proc_recvs_incomplete.dec();
        }
      } else {
        logstream(LOG_DEBUG) << "Expecting " << calls_to_receive[i] 
                             << " calls from " << i << " but only " 
                             << callsreceived[i].value << "received." << std::endl;
      }
    }

    full_barrier_lock.lock();
    while (num_proc_recvs_incomplete.value > 0) {
      logstream(LOG_DEBUG) << "Calls Incomplete. Waiting." << std::endl;
      full_barrier_cond.wait(full_barrier_lock);
    }
    full_barrier_lock.unlock();
    full_barrier_in_effect = false;
//     for (size_t i = 0; i < numprocs(); ++i) {
//       std::cout << "Received " << global_calls_received[i].value << " from " << i << std::endl;
//     }
    barrier();
  }

 /* --------------------  Implementation of Gather Statistics -----------------*/
 private:
  struct collected_statistics {
    size_t callssent;
    size_t bytessent;
    collected_statistics(): callssent(0), bytessent(0) { }
    void save(oarchive &oarc) const {
      oarc << callssent << bytessent;
    }
    void load(iarchive &iarc) {
      iarc >> callssent >> bytessent;
    }
  };
 public:
  /** Gather RPC statistics. All machines must call
   this function at the same time. However, only proc 0 will
   return values */
  std::map<std::string, size_t> gather_statistics() {
    std::map<std::string, size_t> ret;

    std::vector<collected_statistics> stats(numprocs());
    stats[procid()].callssent = calls_sent();
    stats[procid()].bytessent = bytes_sent();
    logstream(LOG_INFO) << procid() << ": calls_sent: ";
    for (size_t i = 0;i < numprocs(); ++i) {
      logstream(LOG_INFO) << callssent[i].value << ", ";
    }
    logstream(LOG_INFO) << std::endl;
    logstream(LOG_INFO) << procid() << ": calls_recv: ";
    for (size_t i = 0;i < numprocs(); ++i) {
      logstream(LOG_INFO) << callsreceived[i].value << ", ";
    }
    logstream(LOG_INFO) << std::endl;


    gather(stats, 0, true);
    if (procid() == 0) {
      collected_statistics cs;
      for (size_t i = 0;i < numprocs(); ++i) {
        cs.callssent += stats[i].callssent;
        cs.bytessent += stats[i].bytessent;
      }
      ret["total_calls_sent"] = cs.callssent;
      ret["total_bytes_sent"] = cs.bytessent;
    }
    return ret;
  }
};

#include <graphlab/macros_undef.hpp>
#include <rpc/mem_function_arg_types_undef.hpp>
#undef BARRIER_BRANCH_FACTOR
}// namespace graphlab
#endif

