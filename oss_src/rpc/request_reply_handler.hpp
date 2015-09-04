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


#ifndef REPLY_INCREMENT_COUNTER_HPP
#define REPLY_INCREMENT_COUNTER_HPP
#include <string>
#include <parallel/atomic.hpp>
#include <parallel/pthread_tools.hpp>
#include <rpc/dc_internal_types.hpp>
namespace graphlab {

class distributed_control;

namespace dc_impl {
/**
\ingroup rpc
\internal
A wrapper around a char array. This structure 
is incapable of freeing itself and must be managed externally
*/
struct blob {
  /// Constructs a blob containing a pointer to a character array with length len
  inline blob(char* c, size_t len):c(c),len(len) { };
  inline blob():c(NULL), len(0){ };

  char *c;  ///< stored pointer 
  size_t len; ///< stored length


  /// serialize the char array
  inline void save(oarchive& oarc) const {
    oarc << len;
    if (len > 0) serialize(oarc, c, len);
  }

  /// deserializes a char array. If there is already a char array here, it will be freed
  inline void load(iarchive& iarc) {
    if (c) ::free(c);
    c = NULL;
    iarc >> len;
    if (len > 0) {
      c = (char*) malloc(len);
      deserialize(iarc, c, len);
    }
  }

  /// Free the stored char array.
  inline void free() {
    if (c) {
      ::free(c);
      c = NULL;
      len = 0;
    }
  }
};


/**
 *\internal
 * \ingroup rpc
 * Abstract class for where the result of a request go into.
 */
struct ireply_container {
  ireply_container() { }
  virtual ~ireply_container() { }
  virtual void wait() = 0;
  virtual void receive(procid_t source, blob b) = 0;
  virtual bool ready() const = 0;
  virtual blob& get_blob() = 0;
};


/**
\internal
\ingroup rpc
The most basic container for replies. Only waits for one reply,
and uses a mutex/condition variable pair to lock and wait on the reply value.
\see ireply_container 
*/
struct basic_reply_container: public ireply_container{
  blob val;
  mutex mut;
  conditional cond;
  bool valready;
  /**
   * Constructs a reply object which waits for 'retcount' replies.
   */
  basic_reply_container():valready(false) { }
  
  ~basic_reply_container() { 
    val.free();
  }

  void receive(procid_t source, blob b) {
    mut.lock();
    val = b;
    valready = true;
    cond.signal();
    mut.unlock();
  }
  /**
   * Waits for all replies to complete. It is up to the 
   * reply implementation to decrement the counter.
   */
  inline void wait() {
    mut.lock();
    while(!valready) cond.wait(mut);
    mut.unlock();
  }

  inline bool ready() const {
    return valready;
  }

  blob& get_blob() {
    return val;
  }
};


} // namespace dc_impl


/**
 * \internal
 * \ingroup rpc
 * The RPC call to handle the result of a request.
 *
 * The basic protocol of a request is as such:
 * On the sender side, a request_future is created which contains within it
 * an instance of an ireply_container. A message is then sent to the target
 * machine containing the address of the ireply_container.
 * Once the target machine finishes evaluating the function, it issues a
 * call to the request_reply_handler function, passing the original address
 * into the ptr argument. The request_reply_handler then reinterprets the ptr
 * argument as an ireply_container object and calls the receive() function 
 * on it.
 * \see ireply_container
 */
void request_reply_handler(distributed_control &dc, procid_t src, 
                             size_t ptr, dc_impl::blob ret);


} // namespace graphlab

#endif

