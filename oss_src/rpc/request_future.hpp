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


#ifndef OBJECT_REQUEST_FUTURE_HPP
#define OBJECT_REQUEST_FUTURE_HPP
#include <memory>
#include <serialization/serialization_includes.hpp>
#include <rpc/dc_types.hpp>
#include <rpc/dc_internal_types.hpp>
#include <rpc/request_reply_handler.hpp>
#include <rpc/function_ret_type.hpp>

namespace graphlab {


  /**
   * \ingroup rpc
   * The result of a remote_request future call.
   * This class represents the outcome of a remote request sent to another
   * machine via the future-based remote_request_call. The future remote_request call
   * returns immediately with this object. Only when operator() is called on this
   * object, then it waits for a result from the remote machine. All remote_request
   * calls which return futures are linked below.
   *
   * example:
   * \code
   * // this function returns immediately
   * graphlab::request_future<int> res = 
   *   rmi.future_remote_request(SOME_OTHER_MACHINE, 
   *                             function_which_returns_an_integer, ...);
   *
   * ... we can do other stuff ... 
   * // read the result, or wait for the result if it is not done yet.
   * int actual_result = res();
   * \endcode
   *
   * \see graphlab::distributed_control::future_remote_request
   *      graphlab::dc_dist_object::future_remote_request
   *      graphlab::fiber_remote_request
   *      graphlab::object_fiber_remote_request
   *
   * The future object holds a copy of the result of the request, and the
   * operator() call returns a reference to this result (once it is available).
   */
template <typename T>
struct request_future {
  typedef typename dc_impl::function_ret_type<T>::type result_type;
#if defined(__cplusplus) && __cplusplus >= 201103L
  mutable std::shared_ptr<dc_impl::ireply_container> reply;
#else
  mutable std::auto_ptr<dc_impl::ireply_container> reply;
#endif
  result_type result;
  bool hasval;

  /// default constructor
  request_future(): 
      reply(new dc_impl::basic_reply_container),
      hasval(false) { }


  /** constructor which allows you to specify a custom target container
   * This class takes ownership of the container and will free it when done.
   */
  request_future(dc_impl::ireply_container* container): 
      reply(container),
      hasval(false) { }


  /** We can assign return values directly to the future in the
   * case where no remote calls are necessary. 
   * Thus allowing the following to be written easily:
   * \code
   * request_future<int> a_function(int arg) {
   *   if (arg == 0) return rmi.future_remote_request(... somewhere else ...) ;
   *   else return 10;
   * }
   * \endcode
   */
  request_future(const T& val): 
      reply(NULL),
      result(val), 
      hasval(true) { }

  /// copy constructor 
  request_future(const request_future<T>& val): 
      reply(val.reply),
      result(val.result), 
      hasval(val.hasval) { }

  /// operator=
  request_future& operator=(const request_future<T>& val) {
    reply = val.reply;
    result = val.result;
    hasval = val.hasval;
    return *this;
  }

  /**
   * \internal
   * Returns a handle to the underlying container
   */
  size_t get_handle() {
    return reinterpret_cast<size_t>(reply.get());
  }

  /**  
   * Waits for the request if it has not yet been received.
   */
  void wait() {
    if (!hasval) {
      reply->wait(); 
      dc_impl::blob& receiveddata = reply->get_blob();
      iarchive iarc(receiveddata.c, receiveddata.len); 
      iarc >> result;  
      receiveddata.free(); 
      hasval = true;
    }
  }

  /**
   * Returns true if the result is ready and \ref operator()
   * can be called without blocking.
   */
  bool is_ready() {
    return (hasval || reply->ready());
  }

  /**
   * Waits for the request if it has not yet been received.
   * When the result is ready, it returns a reference to the received value.
   */
  result_type& operator()() {
    if (!hasval) wait();
    return result;
  }
};


template <>
struct request_future<void> {
  typedef dc_impl::function_ret_type<void>::type result_type;

#if defined(__cplusplus) && __cplusplus >= 201103L
  mutable std::shared_ptr<dc_impl::ireply_container> reply;
#else
  mutable std::auto_ptr<dc_impl::ireply_container> reply;
#endif
  bool hasval;

  request_future(): 
      reply(new dc_impl::basic_reply_container),
      hasval(false) { }

  request_future(dc_impl::ireply_container* container): 
      reply(container),
      hasval(false) { }

  request_future(int val): 
      reply(NULL),
      hasval(true) { }
 
 
  request_future(const request_future<void>& val): 
      reply(val.reply),
      hasval(val.hasval) { }

  request_future& operator=(const request_future<void>& val) {
    reply = val.reply;
    hasval = val.hasval;
    return *this;
  }

  bool is_ready() {
    return (hasval || reply->ready());
  }


  size_t get_handle() {
    return reinterpret_cast<size_t>(reply.get());
  }

  void wait() {
    if (!hasval) {
      result_type result;
      reply->wait(); 
      dc_impl::blob& receiveddata = reply->get_blob();
      iarchive iarc(receiveddata.c, receiveddata.len); 
      iarc >> result;  
      receiveddata.free(); 
      hasval = true;
    }
  }

  result_type operator()() {
    if (!hasval) wait();
    return 0;
  }
};


}
#endif
