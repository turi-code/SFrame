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


#ifndef GRAPHLAB_LOCAL_CHANDY_MISRA_HPP
#define GRAPHLAB_LOCAL_CHANDY_MISRA_HPP
#include <vector>
#include <logger/assertions.hpp>
#include <parallel/pthread_tools.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

template <typename GraphType>
class chandy_misra {
 public:
  GraphType &graph;
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

  struct philosopher {
    vertex_id_type num_edges;
    vertex_id_type forks_acquired;
    simple_spinlock lock;
    unsigned char state;
  };
  std::vector<philosopher> philosopherset;
  /*
   * Possible values for the philosopher state
   */
  enum {
    THINKING = 0,
    HUNGRY = 1,
    EATING = 2
  };

  /** Places a request for the fork. Requires fork to be locked */
  inline void request_for_fork(size_t forkid, bool nextowner) {
    forkset[forkid] |= request_bit(nextowner);
  }

  inline bool fork_owner(size_t forkid) {
    return forkset[forkid] & OWNER_BIT;
  }

  inline bool fork_dirty(size_t forkid) {
    return !!(forkset[forkid] & DIRTY_BIT);
  }

  inline void dirty_fork(size_t forkid) {
    forkset[forkid] |= DIRTY_BIT;
  }
  
  /** changes the fork owner if it is dirty, and the other side
   *  has requested for it. Fork must be locked.
   * Returns true if fork moved. false otherwise.
   */
  inline void advance_fork_state_on_lock(size_t forkid,
                                        vertex_id_type source,
                                        vertex_id_type target) {
    
    unsigned char currentowner = forkset[forkid] & OWNER_BIT;
    if (currentowner == OWNER_SOURCE) {
      // if the current owner is not eating, and the
      // fork is dirty and other side has placed a request
      if (philosopherset[source].state != EATING &&
          (forkset[forkid] & DIRTY_BIT) &&
          (forkset[forkid] & REQUEST_1)) {
        //  change the owner and clean the fork)
        
        forkset[forkid] = OWNER_TARGET;
        if (philosopherset[source].state == HUNGRY) {
          forkset[forkid] |= REQUEST_0;
        }
        philosopherset[source].forks_acquired--;
        philosopherset[target].forks_acquired++;        
      }
    }
    else {
      // if the current owner is not eating, and the
      // fork is dirty and other side has placed a request
      if (philosopherset[target].state != EATING &&
          (forkset[forkid] & DIRTY_BIT) &&
          (forkset[forkid] & REQUEST_0)) {
        //  change the owner and clean the fork)
        
        forkset[forkid] = OWNER_SOURCE;
        if (philosopherset[target].state == HUNGRY) {
          forkset[forkid] |= REQUEST_1;
        }
        philosopherset[source].forks_acquired++;
        philosopherset[target].forks_acquired--;
      }
    }
  }
  
  
  inline bool advance_fork_state_on_unlock(size_t forkid,
                                         vertex_id_type source,
                                         vertex_id_type target) {
    
    unsigned char currentowner = forkset[forkid] & OWNER_BIT;
    if (currentowner == OWNER_SOURCE) {
      // if the current owner is not eating, and the
      // fork is dirty and other side has placed a request
      if ((forkset[forkid] & DIRTY_BIT) &&
        (forkset[forkid] & REQUEST_1)) {
        //  change the owner and clean the fork)
        // keep my request bit if any
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
        forkset[forkid] = OWNER_SOURCE;
        philosopherset[source].forks_acquired++;
        philosopherset[target].forks_acquired--;
        return true; 
      }
    }
    return false;
  }
  
  void compute_initial_fork_arrangement() {
    for (vertex_id_type i = 0;i < graph.num_vertices(); ++i) {
      philosopherset[i].num_edges = graph.num_in_edges(i) +
                                    graph.num_out_edges(i);
      philosopherset[i].state = THINKING;
      philosopherset[i].forks_acquired = 0;
    }
    for (vertex_id_type i = 0;i < graph.num_vertices(); ++i) {
      foreach(typename GraphType::edge_type edge, graph.in_edges(i)) {
        if (edge.source() > edge.target()) {
          forkset[graph.edge_id(edge)] = DIRTY_BIT | OWNER_TARGET;
          philosopherset[edge.target()].forks_acquired++;
        }
        else {
          forkset[graph.edge_id(edge)] = DIRTY_BIT | OWNER_SOURCE;
          philosopherset[edge.source()].forks_acquired++;
        }
      }
    }
  }

  void compute_initial_fork_arrangement(const std::vector<vertex_id_type> &altvids) {
    for (vertex_id_type i = 0;i < graph.num_vertices(); ++i) {
      philosopherset[i].num_edges = graph.num_in_edges(i) +
                                    graph.num_out_edges(i);
      philosopherset[i].state = THINKING;
      philosopherset[i].forks_acquired = 0;
    }
    for (vertex_id_type i = 0;i < graph.num_vertices(); ++i) {
      foreach(typename GraphType::edge_type edge, graph.in_edges(i)) {
        if (altvids[edge.source()] > altvids[edge.target()]) {
          forkset[graph.edge_id(edge)] = DIRTY_BIT | OWNER_TARGET;
          philosopherset[edge.target()].forks_acquired++;
        }
        else {
          forkset[graph.edge_id(edge)] = DIRTY_BIT | OWNER_SOURCE;
          philosopherset[edge.source()].forks_acquired++;
        }
      }
    }
  }


  /**
   * We already have v1, we want to acquire v2.
   * When this function returns, both v1 and v2 locks are acquired
   */
  void try_acquire_edge_with_backoff(vertex_id_type v1,
                                     vertex_id_type v2) {
    if (v1 < v2) {
      philosopherset[v2].lock.lock();
    }
    else if (!philosopherset[v2].lock.try_lock()) {
        philosopherset[v1].lock.unlock();
        philosopherset[v2].lock.lock();
        philosopherset[v1].lock.lock();
    }
  }

  
 public:
  inline chandy_misra(GraphType &graph):graph(graph) {
    forkset.resize(graph.num_edges(), 0);
    philosopherset.resize(graph.num_vertices());
    compute_initial_fork_arrangement();
  }

  inline chandy_misra(GraphType &graph, 
                      const std::vector<vertex_id_type> &altvids):graph(graph) {
    forkset.resize(graph.num_edges(), 0);
    philosopherset.resize(graph.num_vertices());
    compute_initial_fork_arrangement(altvids);
  }

  inline const vertex_id_type invalid_vid() const {
    return (vertex_id_type)(-1);
  }

  inline vertex_id_type make_philosopher_hungry(vertex_id_type p_id) {
    vertex_id_type retval = vertex_id_type(-1);
    philosopherset[p_id].lock.lock();
    //philosopher is now hungry!
    ASSERT_EQ((int)philosopherset[p_id].state, (int)THINKING);
    philosopherset[p_id].state = HUNGRY;

    // now try to get all the forks. lock one edge at a time
    // using the backoff strategy
    //std::cerr << "vertex " << p_id << std::endl;
    //std::cerr << "in edges: " << std::endl;
    foreach(typename GraphType::edge_type edge, graph.in_edges(p_id)) {
      try_acquire_edge_with_backoff(edge.target(), edge.source());
      //std::cerr << "\t" << graph.edge_id(edge) << ": " << edge.source() << "->" << edge.target() << std::endl;
      size_t edgeid = graph.edge_id(edge);
      // if fork is owned by other edge, try to take it
      if (fork_owner(edgeid) == OWNER_SOURCE) {
        request_for_fork(edgeid, OWNER_TARGET);        
        advance_fork_state_on_lock(edgeid, edge.source(), edge.target());
      }
      philosopherset[edge.source()].lock.unlock();
    }
    //std::cerr << "out edges: " << std::endl;
    foreach(typename GraphType::edge_type edge, graph.out_edges(p_id)) {
      //std::cerr << "\t" << graph.edge_id(edge) << ": " << edge.source() << "->" << edge.target() << std::endl;
      try_acquire_edge_with_backoff(edge.source(), edge.target());
      size_t edgeid = graph.edge_id(edge);
 
      // if fork is owned by other edge, try to take it
      if (fork_owner(edgeid) == OWNER_TARGET) {
        request_for_fork(edgeid, OWNER_SOURCE);
        advance_fork_state_on_lock(edgeid, edge.source(), edge.target());
      }
      philosopherset[edge.target()].lock.unlock();
    }

    // if I got all forks I can eat
    if (philosopherset[p_id].forks_acquired ==
                  philosopherset[p_id].num_edges) {
      philosopherset[p_id].state = EATING;
      // signal eating
      retval = p_id;
    }
    philosopherset[p_id].lock.unlock();
    return retval;
  }

  inline std::vector<vertex_id_type> philosopher_stops_eating(size_t p_id) {
    std::vector<vertex_id_type> retval;
    philosopherset[p_id].lock.lock();
    //philosopher is now hungry!
    ASSERT_EQ((int)philosopherset[p_id].state, (int)EATING);
    philosopherset[p_id].state = THINKING;

    // now forks are dirty
    foreach(typename GraphType::edge_type edge, graph.in_edges(p_id)) {
      try_acquire_edge_with_backoff(edge.target(), edge.source());
      size_t edgeid = graph.edge_id(edge);
      vertex_id_type other = edge.source();
      dirty_fork(edgeid);
      advance_fork_state_on_unlock(edgeid, edge.source(), edge.target());
      if (philosopherset[other].state == HUNGRY && 
            philosopherset[other].forks_acquired ==
                philosopherset[other].num_edges) {
        philosopherset[other].state = EATING;
        // signal eating on other
        retval.push_back(other);
      }
      philosopherset[other].lock.unlock();
    }

    foreach(typename GraphType::edge_type edge, graph.out_edges(p_id)) {
      try_acquire_edge_with_backoff(edge.source(), edge.target());
      size_t edgeid = graph.edge_id(edge);
      vertex_id_type other = edge.target();
      dirty_fork(edgeid);
      advance_fork_state_on_unlock(edgeid, edge.source(), edge.target());
      if (philosopherset[other].state == HUNGRY && 
            philosopherset[other].forks_acquired ==
                philosopherset[other].num_edges) {
        philosopherset[other].state = EATING;
        // signal eating on other
        retval.push_back(other);
      }
      philosopherset[other].lock.unlock();
    }
    
    philosopherset[p_id].lock.unlock();
    return retval;
  }

  inline std::vector<vertex_id_type> cancel_eating_philosopher(vertex_id_type p_id) {
    std::vector<vertex_id_type> retval;
    philosopherset[p_id].lock.lock();
    //philosopher is now hungry!
    if(philosopherset[p_id].state != EATING) {
      philosopherset[p_id].lock.unlock();
      return retval;
    }
    philosopherset[p_id].state = HUNGRY;

    // now forks are dirty
    foreach(typename GraphType::edge_type edge, graph.in_edges(p_id)) {
      try_acquire_edge_with_backoff(edge.target(), edge.source());
      size_t edgeid = graph.edge_id(edge);
      vertex_id_type other = edge.source();
      if (fork_dirty(edgeid)) {
        advance_fork_state_on_unlock(edgeid, edge.source(), edge.target());
        if (philosopherset[other].state == HUNGRY && 
              philosopherset[other].forks_acquired ==
                  philosopherset[other].num_edges) {
          philosopherset[other].state = EATING;
          // signal eating on other
          retval.push_back(other);
        }
      }
      philosopherset[other].lock.unlock();
    }

    foreach(typename GraphType::edge_type edge, graph.out_edges(p_id)) {
      try_acquire_edge_with_backoff(edge.source(), edge.target());
      size_t edgeid = graph.edge_id(edge);
      vertex_id_type other = edge.target();
      if (fork_dirty(edgeid)) {
        advance_fork_state_on_unlock(edgeid, edge.source(), edge.target());
        if (philosopherset[other].state == HUNGRY && 
              philosopherset[other].forks_acquired ==
                  philosopherset[other].num_edges) {
          philosopherset[other].state = EATING;
          // signal eating on other
          retval.push_back(other);
        }
      }
      philosopherset[other].lock.unlock();    
    }
        // if I got all forks I can eat
    if (philosopherset[p_id].forks_acquired ==
                  philosopherset[p_id].num_edges) {
      philosopherset[p_id].state = EATING;
      // signal eating
      retval.push_back(p_id);
    }
     philosopherset[p_id].lock.unlock();    
     return retval;
  }


  void no_locks_consistency_check() {
    // make sure all forks are dirty
    for (size_t i = 0;i < forkset.size(); ++i) ASSERT_TRUE(fork_dirty(i));
    // all philosophers are THINKING
    for (size_t i = 0;i < philosopherset.size(); ++i) ASSERT_TRUE(philosopherset[i].state == THINKING);
  }

  void complete_consistency_check() {
    for (vertex_id_type v = 0; v < graph.num_vertices(); ++v) {
      // count the number of forks I own
      size_t numowned = 0;
      size_t numowned_clean = 0;
      foreach(typename GraphType::edge_type edge, graph.in_edges(v)) {
        size_t edgeid = graph.edge_id(edge);
        if (fork_owner(edgeid) == OWNER_TARGET) {
          numowned++;
          if (!fork_dirty(edgeid)) numowned_clean++;
        }
      }
      foreach(typename GraphType::edge_type edge, graph.out_edges(v)) {
        size_t edgeid = graph.edge_id(edge);
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
        foreach(typename GraphType::edge_type edge, graph.in_edges(v)) {
          size_t edgeid = graph.edge_id(edge);
          // not owned
          if (fork_owner(edgeid) == OWNER_SOURCE) {
            if (philosopherset[edge.source()].state != EATING) {
              if (fork_dirty(edgeid)) {
                std::cerr << (int)(forkset[edgeid]) << " " 
                          << (int)philosopherset[edge.source()].state 
                          << "->" << (int)philosopherset[edge.target()].state 
                          << std::endl;
                ASSERT_FALSE(fork_dirty(edgeid));
              }
            }
            ASSERT_NE(philosopherset[edge.source()].state, (int)THINKING);
          }
        }
        foreach(typename GraphType::edge_type edge, graph.out_edges(v)) {
          size_t edgeid = graph.edge_id(edge);
          if (fork_owner(edgeid) == OWNER_TARGET) {
            if (philosopherset[edge.target()].state != EATING) {
              if (fork_dirty(edgeid)) {
                std::cerr << (int)(forkset[edgeid]) << " " 
                          << (int)philosopherset[edge.source()].state 
                          << "->" 
                          << (int)philosopherset[edge.target()].state 
                          << std::endl;
                ASSERT_FALSE(fork_dirty(edgeid));
              }
            }
            ASSERT_NE(philosopherset[edge.target()].state, (int)THINKING);
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
