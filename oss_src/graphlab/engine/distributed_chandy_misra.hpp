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


#ifndef GRAPHLAB_DISTRIBUTED_CHANDY_MISRA_HPP
#define GRAPHLAB_DISTRIBUTED_CHANDY_MISRA_HPP
#include <vector>
#include <rpc/dc_dist_object.hpp>
#include <rpc/distributed_event_log.hpp>
#include <logger/assertions.hpp>
#include <parallel/pthread_tools.hpp>
#include <graph/graph_basic_types.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

/**
  *
  * \internal
  */
template <typename GraphType>
class distributed_chandy_misra {
 public:
  typedef typename GraphType::local_vertex_type local_vertex_type;
  typedef typename GraphType::local_edge_type local_edge_type;
  
  typedef typename GraphType::vertex_id_type vertex_id_type;
  typedef typename GraphType::lvid_type lvid_type;

  typedef distributed_chandy_misra<GraphType> dcm_type;
  dc_dist_object<dcm_type> rmi;
  GraphType* graph;

  boost::function<void(lvid_type)> callback;
  boost::function<void(lvid_type)> hors_doeuvre_callback;
  /*
   * Each "fork" is one character.
   * bit 0: owner. if 0 is src. if 1 is target
   * bit 1: clean = 0, dirty = 1
   * bit 2: owner 0 request
   * bit 3: owner 1 request
   */
  std::vector<unsigned char> forkset;
  enum { OWNER_BIT = 1,
         DIRTY_BIT = 2,
         REQUEST_0 = 4,
         REQUEST_1 = 8 };
  enum {OWNER_SOURCE = 0, OWNER_TARGET = 1};
  inline unsigned char request_bit(bool owner) {
    return owner ? REQUEST_1 : REQUEST_0;
  }


  enum {
    COLLISIONS = 0,
    CANCELLATIONS = 1,
    ACCEPTED_CANCELLATIONS = 2
  };
  
  struct philosopher {
    vertex_id_type num_edges;
    vertex_id_type forks_acquired;
    simple_spinlock lock;
    unsigned char state;
    unsigned char counter;
    bool cancellation_sent;
    bool lockid;
  };
  std::vector<philosopher> philosopherset;
  atomic<size_t> clean_fork_count;
    
  /*
   * Possible values for the philosopher state
   */
  enum {
    THINKING = 0,
    HUNGRY = 1,
    HORS_DOEUVRE = 2, 
    EATING = 3
  };

  /** Places a request for the fork. Requires fork to be locked */
  inline void request_for_fork(size_t forkid, bool nextowner) {
    __sync_fetch_and_or(&forkset[forkid], request_bit(nextowner));
  }

  inline bool fork_owner(size_t forkid) {
    return forkset[forkid] & OWNER_BIT;
  }

  inline bool fork_dirty(size_t forkid) {
    return !!(forkset[forkid] & DIRTY_BIT);
  }

  inline void dirty_fork(size_t forkid) {
      if ((forkset[forkid] & DIRTY_BIT) == 0) clean_fork_count.dec();
    __sync_fetch_and_or(&forkset[forkid], DIRTY_BIT);
  }


  void compute_initial_fork_arrangement() {
    for (lvid_type i = 0;i < graph->num_local_vertices(); ++i) {
      local_vertex_type lvertex(graph->l_vertex(i));
      philosopherset[i].num_edges = lvertex.num_in_edges() +
                                    lvertex.num_out_edges();
      philosopherset[i].state = THINKING;
      philosopherset[i].forks_acquired = 0;
      philosopherset[i].counter = 0;
      philosopherset[i].cancellation_sent = false;
      philosopherset[i].lockid = false;
    }
    for (lvid_type i = 0;i < graph->num_local_vertices(); ++i) {
      local_vertex_type lvertex(graph->l_vertex(i));
      foreach(local_edge_type edge, lvertex.in_edges()) {
        if (edge.source().global_id() > edge.target().global_id()) {
          forkset[edge.id()] = DIRTY_BIT | OWNER_TARGET;
          philosopherset[edge.target().id()].forks_acquired++;
        }
        else {
          forkset[edge.id()] = DIRTY_BIT | OWNER_SOURCE;
          philosopherset[edge.source().id()].forks_acquired++;
        }
      }
    }
  }

  /**
   * We already have v1, we want to acquire v2.
   * When this function returns, both v1 and v2 locks are acquired
   */
  void try_acquire_edge_with_backoff(lvid_type v1,
                                     lvid_type v2) {
    if (v1 < v2) {
      philosopherset[v2].lock.lock();
    }
    else if (!philosopherset[v2].lock.try_lock()) {
        philosopherset[v1].lock.unlock();
        philosopherset[v2].lock.lock();
        philosopherset[v1].lock.lock();
    }
  }
  
/****************************************************************************
 * Tries to move a requested fork
 *
 * Pseudocode:
 *  If current owner is hungry and fork is clean
 *    Ignore
 *  ElseIf current owner is Thinking
 *    Relinquish fork immediately and clear the request flag
 *  ElseIf current owner is hors_doeuvre and fork is clean
 *    Ignore
 *  ElseIf current owner is hors_doeuvre and fork is dirty
 *    Send cancellation message
 *    Set cancelsent
 *  End
 * Return true if changes were made
 ***************************************************************************/
  inline bool advance_fork_state_on_lock(size_t forkid,
                                        lvid_type source,
                                        lvid_type target) {
    unsigned char currentowner = forkset[forkid] & OWNER_BIT;
    if (currentowner == OWNER_SOURCE) {
      // if the current owner is not eating, and the
      // fork is dirty and other side has placed a request
      if (philosopherset[source].state != EATING &&
          (forkset[forkid] & DIRTY_BIT) &&
          (forkset[forkid] & REQUEST_1)) {

        if (philosopherset[source].state != HORS_DOEUVRE) {
          //  change the owner and clean the fork)
          forkset[forkid] = OWNER_TARGET;
          clean_fork_count.inc();
          if (philosopherset[source].state == HUNGRY) {
            forkset[forkid] |= REQUEST_0;
          }
          philosopherset[source].forks_acquired--;
          philosopherset[target].forks_acquired++;
          return true;
        }
        else if (philosopherset[source].cancellation_sent == false) {
          //PERMANENT_ACCUMULATE_DIST_EVENT(eventlog, CANCELLATIONS, 1);
          philosopherset[source].cancellation_sent = true;
          bool lockid = philosopherset[source].lockid;
          philosopherset[source].lock.unlock();
          philosopherset[target].lock.unlock();
          issue_cancellation_request_unlocked(source, lockid);
          philosopherset[std::min(source, target)].lock.lock();
          philosopherset[std::max(source, target)].lock.lock();
        }
      }
    }
    else {
      // if the current owner is not eating, and the
      // fork is dirty and other side has placed a request
      if (philosopherset[target].state != EATING &&
          (forkset[forkid] & DIRTY_BIT) &&
          (forkset[forkid] & REQUEST_0)) {
        //  change the owner and clean the fork)
        if (philosopherset[target].state != HORS_DOEUVRE) {
          forkset[forkid] = OWNER_SOURCE;
          clean_fork_count.inc();
          if (philosopherset[target].state == HUNGRY) {
            forkset[forkid] |= REQUEST_1;
          }
          philosopherset[source].forks_acquired++;
          philosopherset[target].forks_acquired--;
          return true;
        }
        else if (philosopherset[target].cancellation_sent == false) {
          //PERMANENT_ACCUMULATE_DIST_EVENT(eventlog, CANCELLATIONS, 1);
          philosopherset[target].cancellation_sent = true;
          bool lockid = philosopherset[target].lockid;
          philosopherset[source].lock.unlock();
          philosopherset[target].lock.unlock();
          issue_cancellation_request_unlocked(target, lockid);
          philosopherset[std::min(source, target)].lock.lock();
          philosopherset[std::max(source, target)].lock.lock();
        }
      }
    }
    //PERMANENT_ACCUMULATE_DIST_EVENT(eventlog, COLLISIONS, 1);
    return false;
  }


/****************************************************************************
 * Performs a cancellation on a vertex.
 * 
 * If lockIds do not match, ignore
 * If counter == 0 ignore
 * Otherwise, counter++ and reply cancellation accept.
 * Unfortunately, I cannot perform a local call here even if I am the
 * owner since this may produce a lock cycle. Irregardless of whether
 * the owner is local or not, this must be performed by a remote call
 ***************************************************************************/

  void cancellation_request_unlocked(lvid_type lvid, procid_t requestor, bool lockid) {
    philosopherset[lvid].lock.lock();
    
    if (philosopherset[lvid].lockid == lockid) {
      if (philosopherset[lvid].counter > 0) {
        /*ASSERT_TRUE(philosopherset[lvid].state == HORS_DOEUVRE || 
                    philosopherset[lvid].state == HUNGRY);*/
        ++philosopherset[lvid].counter;
        bool lockid = philosopherset[lvid].lockid;
        //PERMANENT_ACCUMULATE_DIST_EVENT(eventlog, ACCEPTED_CANCELLATIONS, 1);
        vertex_id_type gvid = graph->global_vid(lvid);
        logstream(LOG_DEBUG) << rmi.procid() <<
            ": Cancellation accepted on " << gvid <<
            "(" << (int)philosopherset[lvid].counter << ")" << std::endl;
        philosopherset[lvid].lock.unlock();
        
        if (requestor != rmi.procid()) {
          unsigned char pkey = rmi.dc().set_sequentialization_key(gvid % 254 + 1);
          rmi.remote_call(requestor,
                          &dcm_type::rpc_cancellation_accept,
                          gvid,
                          lockid);
          rmi.dc().set_sequentialization_key(pkey);
        }
        else {
          cancellation_accept_unlocked(lvid, lockid);
        }
      }
      else {
        philosopherset[lvid].lock.unlock();
        logstream(LOG_DEBUG) << rmi.procid() <<
          ": Cancellation on " << graph->global_vid(lvid) <<
          " denied due to lock completion" << std::endl;
      }
    }
    else {
      philosopherset[lvid].lock.unlock();
      logstream(LOG_DEBUG) << rmi.procid() <<
        ": Cancellation on " << graph->global_vid(lvid) <<
        " denied to invalid lock ID" << std::endl;
    }
    
  }

  void rpc_cancellation_request(vertex_id_type gvid, procid_t requestor, bool lockid) {
    lvid_type lvid = graph->local_vid(gvid);
    cancellation_request_unlocked(lvid, requestor, lockid);
  }

  void issue_cancellation_request_unlocked(lvid_type lvid, bool lockid) {
    // signal the master
    logstream(LOG_DEBUG) << rmi.procid() <<
        ": Requesting cancellation on " << graph->global_vid(lvid) << std::endl;
    local_vertex_type lvertex(graph->l_vertex(lvid));
    
    if (lvertex.owner() == rmi.procid()) {
      cancellation_request_unlocked(lvid, rmi.procid(), lockid);
    }
    else {
      unsigned char pkey = rmi.dc().set_sequentialization_key(lvertex.global_id() % 254 + 1);
      rmi.remote_call(lvertex.owner(),
                    &dcm_type::rpc_cancellation_request,
                    lvertex.global_id(),
                    rmi.procid(), 
                    lockid);
      rmi.dc().set_sequentialization_key(pkey);

    }
  }


/****************************************************************************
 * Accepts a cancellation on a vertex.
 *
 * Pseudocode
 *  Change back to Hungry
 *  Releases all dirty forks
 ****************************************************************************/

  void rpc_cancellation_accept(vertex_id_type gvid, bool lockid) {
    lvid_type lvid = graph->local_vid(gvid);
    cancellation_accept_unlocked(lvid, lockid);
  }

  void cancellation_accept_unlocked(lvid_type p_id, bool lockid) {
    std::vector<lvid_type> retval;
    philosopherset[p_id].lock.lock();
    //philosopher is now hungry!
    /*ASSERT_EQ (lockid, philosopherset[p_id].lockid);
    ASSERT_EQ((int)philosopherset[p_id].state, (int)HORS_DOEUVRE); */
    philosopherset[p_id].state = HUNGRY;
    philosopherset[p_id].cancellation_sent = false;
    
    local_vertex_type lvertex(graph->l_vertex(p_id));
    logstream(LOG_DEBUG) << rmi.procid() <<
            ": Cancellation accept received on " << lvertex.global_id() << " " <<
            philosopherset[p_id].state << std::endl;

    // for each fork I own, try to give it away
    foreach(local_edge_type edge, lvertex.in_edges()) {
      try_acquire_edge_with_backoff(edge.target().id(), edge.source().id());
      if (philosopherset[p_id].state == HUNGRY) {
        //std::cerr << "\t" << graph->edge_id(edge) << ": " << edge.source() << "->" << edge.target() << std::endl;
        lvid_type other = edge.source().id();
        size_t edgeid = edge.id();
        if (fork_owner(edgeid) == OWNER_TARGET && fork_dirty(edgeid)) {
          
          if (advance_fork_state_on_lock(edgeid, edge.source().id(), edge.target().id()) &&
              philosopherset[other].state == HUNGRY &&
              philosopherset[other].forks_acquired == philosopherset[other].num_edges) {
            philosopherset[other].state = HORS_DOEUVRE;
            philosopherset[other].cancellation_sent = false;
            // signal eating on other
            retval.push_back(other);
          }
        }
        philosopherset[edge.source().id()].lock.unlock();
      }
      else {
        philosopherset[edge.source().id()].lock.unlock();
        break;
      }
      
    }
    //std::cerr << "out edges: " << std::endl;
    foreach(local_edge_type edge, lvertex.out_edges()) {
      //std::cerr << "\t" << graph->edge_id(edge) << ": " << edge.source() << "->" << edge.target() << std::endl;
      try_acquire_edge_with_backoff(edge.source().id(), edge.target().id());
      if (philosopherset[p_id].state == HUNGRY) {
        lvid_type other = edge.target().id();
        size_t edgeid = edge.id();
        if (fork_owner(edgeid) == OWNER_SOURCE && fork_dirty(edgeid)) {
          if (advance_fork_state_on_lock(edgeid, edge.source().id(), edge.target().id()) &&
              philosopherset[other].state == HUNGRY &&
              philosopherset[other].forks_acquired == philosopherset[other].num_edges) {
            philosopherset[other].state = HORS_DOEUVRE;
            philosopherset[other].cancellation_sent = false;
            // signal eating on other
            retval.push_back(other);
          }
        }
        philosopherset[edge.target().id()].lock.unlock();
      }
      else {        
        philosopherset[edge.target().id()].lock.unlock();
        break;
      }
    }
    
    if (philosopherset[p_id].state == HUNGRY &&
        philosopherset[p_id].forks_acquired == philosopherset[p_id].num_edges) {
      philosopherset[p_id].cancellation_sent = false;
      philosopherset[p_id].state = HORS_DOEUVRE;
      retval.push_back(p_id);
    }
      
    philosopherset[p_id].lock.unlock();
    foreach(lvid_type lvid, retval) {
      enter_hors_doeuvre_unlocked(lvid);
    }

  }
  
/****************************************************************************
 * Make Philosopher Hungry.
 *
 * Pseudocode:
 * Set Philosopher to Hungry
 * For all edges adjacent to v with forks it does not own:
 *   Send request for fork to neighboring vertex
 *
 * Conditions:
 *   Must be Thinking
 *   New lock ID must not be the same as the old lock ID
 *
 * Possible Immediate Transitions:
 *   Current vertex may enter HORS_DOEUVRE
 ***************************************************************************/
  void rpc_make_philosopher_hungry(vertex_id_type gvid, bool newlockid) {
    lvid_type lvid = graph->local_vid(gvid);
    logstream(LOG_DEBUG) << rmi.procid() <<
          ": Local HUNGRY Philosopher  " << gvid << std::endl;
    philosopherset[lvid].lock.lock();

    //ASSERT_EQ((int)philosopherset[lvid].state, (int)THINKING);
    philosopherset[lvid].state = HUNGRY;

//    ASSERT_NE(philosopherset[lvid].lockid, newlockid);
    philosopherset[lvid].lockid = newlockid;

    philosopherset[lvid].lock.unlock();

    local_philosopher_grabs_forks(lvid);
  }

  void local_philosopher_grabs_forks(lvid_type p_id) {
    philosopherset[p_id].lock.lock();
    local_vertex_type lvertex(graph->l_vertex(p_id));
    //philosopher is now hungry!
// now try to get all the forks. lock one edge at a time
    // using the backoff strategy
    //std::cerr << "vertex " << p_id << std::endl;
    //std::cerr << "in edges: " << std::endl;
    foreach(local_edge_type edge, lvertex.in_edges()) {
      try_acquire_edge_with_backoff(edge.target().id(), edge.source().id());
      if (philosopherset[p_id].state == HUNGRY) {

        //std::cerr << "\t" << graph->edge_id(edge) << ": " << edge.source() << "->" << edge.target() << std::endl;
        size_t edgeid = edge.id();
        // if fork is owned by other edge, try to take it
        if (fork_owner(edgeid) == OWNER_SOURCE) {
          request_for_fork(edgeid, OWNER_TARGET);
          advance_fork_state_on_lock(edgeid, edge.source().id(), edge.target().id());
        }
        philosopherset[edge.source().id()].lock.unlock();
      }
      else {
        philosopherset[edge.source().id()].lock.unlock();
        break;
      }
    }
    //std::cerr << "out edges: " << std::endl;
    foreach(local_edge_type edge, lvertex.out_edges()) {
      //std::cerr << "\t" << graph->edge_id(edge) << ": " << edge.source() << "->" << edge.target() << std::endl;
      try_acquire_edge_with_backoff(edge.source().id(), edge.target().id());
      if (philosopherset[p_id].state == HUNGRY) {
        size_t edgeid = edge.id();

        // if fork is owned by other edge, try to take it
        if (fork_owner(edgeid) == OWNER_TARGET) {
          request_for_fork(edgeid, OWNER_SOURCE);
          advance_fork_state_on_lock(edgeid, edge.source().id(), edge.target().id());
        }
        philosopherset[edge.target().id()].lock.unlock();
      }
      else {
        philosopherset[edge.target().id()].lock.unlock();
        break;
      }
    }

    bool enter_hors = false;
    if (philosopherset[p_id].state == HUNGRY &&
        philosopherset[p_id].forks_acquired == philosopherset[p_id].num_edges) {
      philosopherset[p_id].state = HORS_DOEUVRE;
      philosopherset[p_id].cancellation_sent = false;
      enter_hors = true;
    }
    philosopherset[p_id].lock.unlock();
    if (enter_hors) enter_hors_doeuvre_unlocked(p_id);
  }

/************************************************************************
 *
 * Called when a vertex may be ready to enter hors dourre
 * Locks must be maintained. HORS_DOEUVRE must be set prior
 * to entering this function .
 *
 ***********************************************************************/
  void enter_hors_doeuvre_unlocked(lvid_type p_id) {
     // if I got all forks I can eat
    logstream(LOG_DEBUG) << rmi.procid() <<
            ": Local HORS_DOEUVRE Philosopher  " << graph->global_vid(p_id) << std::endl;
    // signal the master
    local_vertex_type lvertex(graph->l_vertex(p_id));

    if (lvertex.owner() == rmi.procid()) {
      signal_ready_unlocked(p_id, philosopherset[p_id].lockid);
    }
    else {
      unsigned char pkey = rmi.dc().set_sequentialization_key(lvertex.global_id() % 254 + 1);
      if (hors_doeuvre_callback != NULL) hors_doeuvre_callback(p_id);
      rmi.remote_call(lvertex.owner(),
                      &dcm_type::rpc_signal_ready,
                      lvertex.global_id(), philosopherset[p_id].lockid);
      rmi.dc().set_sequentialization_key(pkey);
    }
  }

/************************************************************************
 *
 * Called when a vertex enters HORS_DOEUVRE. Locks must be maintained.
 * 
 * Conditions:
 *   vertex must be in HUNGRY or HORS_DOEUVRE
 *   lock IDs must match
 *
 * Possible Immediate Transitions
 *   If counter == 0, transit to EATING
 ***********************************************************************/

  void signal_ready_unlocked(lvid_type lvid, bool lockid) {
    philosopherset[lvid].lock.lock();
    if(!(philosopherset[lvid].state == (int)HUNGRY ||
      philosopherset[lvid].state == (int)HORS_DOEUVRE)) {
      logstream(LOG_ERROR) << rmi.procid() <<
              ": Bad signal ready state!!!! : " << (int)philosopherset[lvid].state << std::endl;
      logstream(LOG_ERROR) << rmi.procid() <<
              " Lock IDs : " << (int)philosopherset[lvid].lockid << " " << (int)lockid << std::endl;
      logstream(LOG_ERROR) << rmi.procid() <<
            ": BAD Global HORS_DOEUVRE " << graph->global_vid(lvid)
            << "(" << (int)philosopherset[lvid].counter << ")" << std::endl;
  
     /* ASSERT_TRUE(philosopherset[lvid].state == (int)HUNGRY ||
                  philosopherset[lvid].state == (int)HORS_DOEUVRE);*/
    }
    
//    ASSERT_EQ(philosopherset[lvid].lockid, lockid);
    philosopherset[lvid].counter--;

    logstream(LOG_DEBUG) << rmi.procid() <<
            ": Global HORS_DOEUVRE " << graph->global_vid(lvid)
            << "(" << (int)philosopherset[lvid].counter << ")" << " " << (int)(philosopherset[lvid].state) << std::endl;
  
    if(philosopherset[lvid].counter == 0) {
      philosopherset[lvid].lock.unlock();
      // broadcast EATING
      local_vertex_type lvertex(graph->l_vertex(lvid));
      unsigned char pkey = rmi.dc().set_sequentialization_key(lvertex.global_id() % 254 + 1);
      rmi.remote_call(lvertex.mirrors().begin(), lvertex.mirrors().end(),
                      &dcm_type::rpc_set_eating, lvertex.global_id(), lockid);
      set_eating(lvid, lockid);
      rmi.dc().set_sequentialization_key(pkey);
    }
    else {
      philosopherset[lvid].lock.unlock();
    }
  }


  void rpc_signal_ready(vertex_id_type gvid, bool lockid) {
    lvid_type lvid = graph->local_vid(gvid);
    signal_ready_unlocked(lvid, lockid);
  }

  void set_eating(lvid_type lvid, bool lockid) {
    philosopherset[lvid].lock.lock();
    
    logstream(LOG_DEBUG) << rmi.procid() <<
            ": EATING " << graph->global_vid(lvid)
            << "(" << (int)philosopherset[lvid].counter << ")" << std::endl;
  
//    ASSERT_EQ((int)philosopherset[lvid].state, (int)HORS_DOEUVRE);
//    ASSERT_EQ(philosopherset[lvid].lockid, lockid);
    philosopherset[lvid].state = EATING;
    philosopherset[lvid].cancellation_sent = false;
    philosopherset[lvid].lock.unlock();
    if (graph->l_vertex(lvid).owner() == rmi.procid()) {
    logstream(LOG_DEBUG) << rmi.procid() <<
            ": CALLBACK " << graph->global_vid(lvid) << std::endl;

      callback(lvid);
    }
  }

  void rpc_set_eating(vertex_id_type gvid, bool lockid) {

    logstream(LOG_DEBUG) << rmi.procid() <<
            ": Receive Set EATING " << gvid << std::endl;
  
    lvid_type lvid = graph->local_vid(gvid);
    set_eating(lvid, lockid);
  }
/************************************************************************
 *
 * Called when a vertex stops eating
 *
 ***********************************************************************/



  inline bool advance_fork_state_on_unlock(size_t forkid,
                                         lvid_type source,
                                         lvid_type target) {

    unsigned char currentowner = forkset[forkid] & OWNER_BIT;
    if (currentowner == OWNER_SOURCE) {
      // if the current owner is not eating, and the
      // fork is dirty and other side has placed a request
      if ((forkset[forkid] & DIRTY_BIT) &&
        (forkset[forkid] & REQUEST_1)) {
        //  change the owner and clean the fork)
        // keep my request bit if any
        clean_fork_count.inc();
        forkset[forkid] = OWNER_TARGET;
        philosopherset[source].forks_acquired--;
        philosopherset[target].forks_acquired++;
        return true;
      }
    }
    else {
      // if the current owner is not eating, and the
      // fork is dirty and other side has placed a request
      if ((forkset[forkid] & DIRTY_BIT) &&
        (forkset[forkid] & REQUEST_0)) {
        //  change the owner and clean the fork)
        // keep my request bit if any
        clean_fork_count.inc();
        forkset[forkid] = OWNER_SOURCE;
        philosopherset[source].forks_acquired++;
        philosopherset[target].forks_acquired--;
        return true;
      }
    }
    return false;
  }


  

  
  void local_philosopher_stops_eating(lvid_type p_id) {
    std::vector<lvid_type> retval;
    philosopherset[p_id].lock.lock();
    if (philosopherset[p_id].state != EATING) {
      std::cerr << rmi.procid() << ": " << p_id << "FAILED!! Cannot Stop Eating!" << std::endl;
//      ASSERT_EQ((int)philosopherset[p_id].state, (int)EATING);
    }
    
    local_vertex_type lvertex(graph->l_vertex(p_id));
    // now forks are dirty
    foreach(local_edge_type edge, lvertex.in_edges()) {
      dirty_fork(edge.id());
    }
    
    foreach(local_edge_type edge, lvertex.out_edges()) {
      dirty_fork(edge.id());
    }
    
    
    philosopherset[p_id].state = THINKING;
    philosopherset[p_id].counter = 0;

    // now forks are dirty
    foreach(local_edge_type edge, lvertex.in_edges()) {
      try_acquire_edge_with_backoff(edge.target().id(), edge.source().id());
      lvid_type other = edge.source().id();
      if (philosopherset[p_id].state == THINKING) {
        size_t edgeid = edge.id();
        advance_fork_state_on_unlock(edgeid, edge.source().id(), edge.target().id());
        if (philosopherset[other].state == HUNGRY &&
              philosopherset[other].forks_acquired ==
                  philosopherset[other].num_edges) {
          philosopherset[other].state = HORS_DOEUVRE;
          philosopherset[other].cancellation_sent = false;
          // signal eating on other
          retval.push_back(other);
        }
        philosopherset[other].lock.unlock();
      }
      else {
        philosopherset[other].lock.unlock();
        break;
      }
    }

    foreach(local_edge_type edge, lvertex.out_edges()) {
      try_acquire_edge_with_backoff(edge.source().id(), edge.target().id());
      lvid_type other = edge.target().id();
      if (philosopherset[p_id].state == THINKING) {
        size_t edgeid = edge.id();
        advance_fork_state_on_unlock(edgeid, edge.source().id(), edge.target().id());
        if (philosopherset[other].state == HUNGRY &&
              philosopherset[other].forks_acquired ==
                  philosopherset[other].num_edges) {
          philosopherset[other].state = HORS_DOEUVRE;
          philosopherset[other].cancellation_sent = false;
          // signal eating on other
          retval.push_back(other);
        }
        philosopherset[other].lock.unlock();
      }
      else {
        philosopherset[other].lock.unlock();
        break;
      }
    }

    philosopherset[p_id].lock.unlock();
    foreach(lvid_type lvid, retval) {
      enter_hors_doeuvre_unlocked(lvid);
    }
  }

  void rpc_philosopher_stops_eating(vertex_id_type gvid) {
    logstream(LOG_DEBUG) << rmi.procid() << ": Receive STOP eating on " << gvid << std::endl;
    local_philosopher_stops_eating(graph->local_vid(gvid));
  }

 public:
  inline distributed_chandy_misra(distributed_control &dc,
                                  GraphType &new_graph,
                                  boost::function<void(lvid_type)> callback,
                                  boost::function<void(lvid_type)> hors_doeuvre_callback = NULL
                                  ):
                          rmi(dc, this),
                          graph(&new_graph),
                          callback(callback),
                          hors_doeuvre_callback(hors_doeuvre_callback){
    forkset.resize(graph->num_local_edges(), 0);
    philosopherset.resize(graph->num_local_vertices());
    compute_initial_fork_arrangement();

    rmi.barrier();
  }


  inline distributed_chandy_misra(distributed_control &dc,
                                  boost::function<void(lvid_type)> callback,
                                  boost::function<void(lvid_type)> hors_doeuvre_callback = NULL
                                  ):
                          rmi(dc, this),
                          graph(NULL),
                          callback(callback),
                          hors_doeuvre_callback(hors_doeuvre_callback){
    rmi.barrier();
  }

  void init(GraphType& new_graph) {
    graph = &new_graph;
    forkset.resize(graph->num_local_edges(), 0);
    philosopherset.resize(graph->num_local_vertices());
    compute_initial_fork_arrangement();

    rmi.barrier();
  }

  size_t num_clean_forks() const {
    return clean_fork_count.value;
  }

  void initialize_master_philosopher_as_hungry_locked(lvid_type p_id,
                                                      bool lockid) {
    philosopherset[p_id].lockid = lockid;
    philosopherset[p_id].state = HUNGRY;
    philosopherset[p_id].counter = graph->l_vertex(p_id).num_mirrors() + 1;
  }
  
  void make_philosopher_hungry(lvid_type p_id) {
    local_vertex_type lvertex(graph->l_vertex(p_id));    
//    ASSERT_EQ(rec.get_owner(), rmi.procid());
    philosopherset[p_id].lock.lock();
//    ASSERT_EQ((int)philosopherset[p_id].state, (int)THINKING);
    bool newlockid = !philosopherset[p_id].lockid;
    initialize_master_philosopher_as_hungry_locked(p_id, newlockid);
    
    logstream(LOG_DEBUG) << rmi.procid() <<
            ": Global HUNGRY " << lvertex.global_id()
            << "(" << (int)philosopherset[p_id].counter << ")" << std::endl;
  
    philosopherset[p_id].lock.unlock();
    
    unsigned char pkey = rmi.dc().set_sequentialization_key(lvertex.global_id() % 254 + 1);
    rmi.remote_call(lvertex.mirrors().begin(), lvertex.mirrors().end(),
                    &dcm_type::rpc_make_philosopher_hungry, lvertex.global_id(), newlockid);
    rmi.dc().set_sequentialization_key(pkey);
    local_philosopher_grabs_forks(p_id);
  }
  
  
  
  void make_philosopher_hungry_per_replica(lvid_type p_id) {
    local_vertex_type lvertex(graph->l_vertex(p_id));    
    philosopherset[p_id].lock.lock();
//    ASSERT_EQ((int)philosopherset[p_id].state, (int)THINKING);

    if (lvertex.owner() == rmi.procid()) {
      bool newlockid = !philosopherset[p_id].lockid;
      initialize_master_philosopher_as_hungry_locked(p_id, newlockid);
      
      logstream(LOG_DEBUG) << rmi.procid() <<
            ": Global HUNGRY " << lvertex.global_id()
            << "(" << (int)philosopherset[p_id].counter << ")" << std::endl;
    }
    else {
      bool newlockid = !philosopherset[p_id].lockid;
      philosopherset[p_id].lockid = newlockid;
      philosopherset[p_id].state = HUNGRY;
    }
    philosopherset[p_id].lock.unlock();
    local_philosopher_grabs_forks(p_id);
  }
  
  
  void philosopher_stops_eating(lvid_type p_id) {
    local_vertex_type lvertex(graph->l_vertex(p_id));    

    logstream(LOG_DEBUG) << rmi.procid() <<
            ": Global STOP Eating " << lvertex.global_id() << std::endl;

    philosopherset[p_id].lock.lock();
//    ASSERT_EQ(philosopherset[p_id].state, (int)EATING);
    philosopherset[p_id].counter = 0;
    philosopherset[p_id].lock.unlock();
    unsigned char pkey = rmi.dc().set_sequentialization_key(lvertex.global_id() % 254 + 1);
    rmi.remote_call(lvertex.mirrors().begin(), lvertex.mirrors().end(),
                    &dcm_type::rpc_philosopher_stops_eating, lvertex.global_id());
    rmi.dc().set_sequentialization_key(pkey);
    local_philosopher_stops_eating(p_id);
  }

  void philosopher_stops_eating_per_replica(lvid_type p_id) {
    logstream(LOG_DEBUG) << rmi.procid() <<
            ": Global STOP Eating " << graph->global_vid(p_id) << std::endl;

//    ASSERT_EQ(philosopherset[p_id].state, (int)EATING);
    
    local_philosopher_stops_eating(p_id);
  }


  void no_locks_consistency_check() {
    // make sure all forks are dirty
    for (size_t i = 0;i < forkset.size(); ++i) ASSERT_TRUE(fork_dirty(i));
    // all philosophers are THINKING
    for (size_t i = 0;i < philosopherset.size(); ++i) ASSERT_TRUE(philosopherset[i].state == THINKING);
  }

  void print_out() {
  
  boost::unordered_set<size_t> eidset1;
  boost::unordered_set<size_t> eidset2;
  for (lvid_type v = 0; v < graph->num_local_vertices(); ++v) {
    local_vertex_type lvertex(graph->l_vertex(v));
    foreach(local_edge_type edge, lvertex.in_edges()) {
      size_t edgeid = edge.id();
      ASSERT_TRUE(eidset1.find(edgeid) == eidset1.end());
      eidset1.insert(edgeid);
    }
    foreach(local_edge_type edge, lvertex.out_edges()) {
      size_t edgeid = edge.id();
      ASSERT_TRUE(eidset2.find(edgeid) == eidset2.end());
      eidset2.insert(edgeid);
    }
  }
  ASSERT_EQ(eidset1.size(), eidset2.size());
  eidset1.clear(); eidset2.clear();
  complete_consistency_check();
  
    std::cerr << "Philosophers\n";
    std::cerr << "------------\n";
    for (lvid_type v = 0; v < graph->num_local_vertices(); ++v) {
      local_vertex_type lvertex(graph->l_vertex(v));
      std::cerr << graph->global_vid(v) << ": " << (int)philosopherset[v].state << " " <<
                      philosopherset[v].forks_acquired << " " << philosopherset[v].num_edges << " ";
      if (philosopherset[v].forks_acquired == philosopherset[v].num_edges) std::cerr << "---------------!";
      std::cerr << "\n";
      std::cerr << "\tin: ";
      foreach(local_edge_type edge, lvertex.in_edges()) {
        size_t edgeid = edge.id();
        if (fork_dirty(forkset[edgeid])) std::cerr << edgeid << ":" << (int)forkset[edgeid] << " ";
      }
      std::cerr << "\n\tout: ";
      foreach(local_edge_type edge, lvertex.out_edges()) {
        size_t edgeid = edge.id();
        if (fork_dirty(forkset[edgeid]))  std::cerr << edgeid << ":" << (int)forkset[edgeid] << " ";
      }
      std::cerr << "\n";
    }
  }
  
  void complete_consistency_check() {
    for (lvid_type v = 0; v < graph->num_local_vertices(); ++v) {
      local_vertex_type lvertex(graph->l_vertex(v));
      // count the number of forks I own
      size_t numowned = 0;
      size_t numowned_clean = 0;
      foreach(local_edge_type edge, lvertex.in_edges()) {
        size_t edgeid = edge.id();
        if (fork_owner(edgeid) == OWNER_TARGET) {
          numowned++;
          if (!fork_dirty(edgeid)) numowned_clean++;
        }
      }
      foreach(local_edge_type edge, lvertex.out_edges()) {
        size_t edgeid = edge.id();
        if (fork_owner(edgeid) == OWNER_SOURCE) {
          numowned++;
          if (!fork_dirty(edgeid)) numowned_clean++;
        }
      }

      ASSERT_EQ(philosopherset[v].forks_acquired, numowned);
      if (philosopherset[v].state == THINKING) {
        ASSERT_EQ(numowned_clean, 0);
      }
      else if (philosopherset[v].state == HUNGRY) {
        ASSERT_NE(philosopherset[v].num_edges, philosopherset[v].forks_acquired);
        // any fork I am unable to acquire. Must be clean, and the other person
        // must be eating or hungry
        foreach(local_edge_type edge, lvertex.in_edges()) {
          size_t edgeid = edge.id();
          // not owned
          if (fork_owner(edgeid) == OWNER_SOURCE) {
            if (philosopherset[edge.source().id()].state != EATING) {
              if (fork_dirty(edgeid)) {
                std::cerr << (int)(forkset[edgeid]) << " "
                          << (int)philosopherset[edge.source().id()].state
                          << "->" << (int)philosopherset[edge.target().id()].state
                          << std::endl;
                ASSERT_FALSE(fork_dirty(edgeid));
              }
            }
            ASSERT_NE(philosopherset[edge.source().id()].state, (int)THINKING);
          }
        }
        foreach(local_edge_type edge, lvertex.out_edges()) {
          size_t edgeid = edge.id();
          if (fork_owner(edgeid) == OWNER_TARGET) {
            if (philosopherset[edge.target().id()].state != EATING) {
              if (fork_dirty(edgeid)) {
                std::cerr << (int)(forkset[edgeid]) << " "
                          << (int)philosopherset[edge.source().id()].state
                          << "->"
                          << (int)philosopherset[edge.target().id()].state
                          << std::endl;
                ASSERT_FALSE(fork_dirty(edgeid));
              }
            }
            ASSERT_NE(philosopherset[edge.target().id()].state, (int)THINKING);
          }
        }

      }
      else if (philosopherset[v].state == EATING) {
        ASSERT_EQ(philosopherset[v].forks_acquired, philosopherset[v].num_edges);
      }
    }
  }
};

}

#include <graphlab/macros_undef.hpp>

#endif
