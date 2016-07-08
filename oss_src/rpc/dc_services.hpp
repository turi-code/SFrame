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


#include <rpc/dc_dist_object.hpp>
#ifndef GRAPHLAB_DC_SERVICES_HPP
#define GRAPHLAB_DC_SERVICES_HPP
#include <parallel/pthread_tools.hpp>



#include <graphlab/macros_def.hpp>
namespace graphlab {

  /**
    \internal
    \ingroup rpc
    Creates a new context for MPI-like global global operations.
    Where all machines create an instance of dc_services at the same time,
    operations performed by the new dc_services instance will not interfere
    and will run in parallel with other contexts.  i.e. If I have two
    distributed dc_services instances, one instance can 
    perform a barrier while another instance performs a broadcast() at the same 
    time.
  */
  class dc_services {
  private:
    dc_dist_object<dc_services> rmi;
  
  public:
    dc_services(distributed_control &dc):rmi(dc, this) {  
      // HACK: force instantation of the RPC_CALL structures for the request_reply_handler
      volatile int t = 0;
      if (t) {
        char c[1];
        dc.RPC_CALL(control_call, request_reply_handler)(0, 0, dc_impl::blob(c, 0));
      }
    }
    
    /// Returns the underlying dc_dist_object 
    dc_dist_object<dc_services>& rmi_instance() {
      return rmi;
    }

    /// Returns the underlying dc_dist_object 
    const dc_dist_object<dc_services>& rmi_instance() const {
      return rmi;
    }
    
    /**
      \copydoc distributed_control::send_to()
    */
    template <typename U>
    inline void send_to(procid_t target, U& t, bool control = false) {
      rmi.send_to(target, t, control);
    }
    
    /**
      \copydoc distributed_control::recv_from()
     */
    template <typename U>
    inline void recv_from(procid_t source, U& t, bool control = false) {
      rmi.recv_from(source, t, control);
    }

    /**
      \copydoc distributed_control::broadcast()
     */
    template <typename U>
    inline void broadcast(U& data, bool originator, bool control = false) { 
      rmi.broadcast(data, originator, control);
    }

    /**
      \copydoc distributed_control::gather()
     */
    template <typename U>
    inline void gather(std::vector<U>& data, procid_t sendto, bool control = false) {
      rmi.gather(data, sendto, control);
    }

    /**
      \copydoc distributed_control::all_gather()
     */
    template <typename U>
    inline void all_gather(std::vector<U>& data, bool control = false) {
      rmi.all_gather(data, control);
    }

    /**
      \copydoc distributed_control::all_reduce()
     */
    template <typename U>
    inline void all_reduce(U& data, bool control = false) {
      rmi.all_reduce(data, control);
    }

    /// \copydoc distributed_control::all_reduce2()
    template <typename U, typename PlusEqual>
    void all_reduce2(U& data, PlusEqual plusequal, bool control = false) {
      rmi.all_reduce2(data, plusequal, control);
    }

    /// \copydoc distributed_control::barrier()
    inline void barrier() {
      rmi.barrier();
    }
    
    
    /// \copydoc distributed_control::full_barrier()
    inline void full_barrier() {
      rmi.full_barrier();
    }
  
 

  };


} // end of namespace graphlab


#include <graphlab/macros_undef.hpp>
#endif

