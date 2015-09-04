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



#ifndef GRAPHLAB_GRAPH_VERTEX_SET_HPP
#define GRAPHLAB_GRAPH_VERTEX_SET_HPP

#include <util/dense_bitset.hpp>
#include <graph/graph_basic_types.hpp>
#include <rpc/buffered_exchange.hpp>
#include <graphlab/macros_def.hpp>

namespace graphlab {

/**
 * \brief Describes a set of vertices
 *
 * The vertex_set describes a set of vertices upon which
 * union / intersection / difference can be performed.
 * These sets can then be passed into graph aggregate operations
 * such as distributed_graph::map_reduce_vertices to perform aggregates
 * over \b subsets of vertices or edges. Engines also permit signalling of
 * sets of vertices through
 * \ref graphlab::iengine::signal_all(const vertex_set& vset, const message_type& message, const std::string& order) "signal_all()".
 *
 * \ref distributed_graph::complete_set() and \ref distributed_graph::empty_set()
 * provide two convenient functions to obtain a full or an empty set of
 * vertices.
 * \code
 * vertex_set all = graph.complete_set();
 * vertex_set empty = graph.empty_set();
 * \endcode
 *
 * \ref distributed_graph::select() can be used to obtain a restriction of the
 * set of vertices. For instance if vertices contain an integer, the following
 * code will construct a set of vertices containing only vertices with data
 * which are a multiple of 2.
 *
 * \code
 * bool is_multiple_of_2(const graph_type::vertex_type& vertex) {
 *   return vertex.data() % 2 == 0;
 * }
 * vertex_set even_vertices = graph.select(is_multiple_of_2);
 * \endcode
 * For more details see \ref distributed_graph::select()
 *
 * The size of the vertex set can only be queried through the graph using
 * \ref distributed_graph::vertex_set_size();
 *
 */
class vertex_set {
  public:
    /**
     * Used only if \ref lazy is false.
     * If \ref lazy is false, this must be the same size as the graph's
     * graphlab::distributed_graph::num_local_vertices().
     * The invariant is that the bit value of each mirror vertex must be the
     * same value as the bit value on their corresponding master vertices.
     */
    mutable dense_bitset localvset;

    /**
     * Used only if \ref lazy is set.
     * If is_complete_set is true, this set describes the set of all vertices.
     * If is_complete set is false, this set describes the empty set.
     */
    bool is_complete_set;

    /**
     * If set, the localvset is empty and not used.
     * instead, \ref is_complete_set will define the set of vertices.
     */
    mutable bool lazy;


    /**
     * \internal
     * \brief Returns a const reference to the underlying bitset.
     */
    template <typename DGraphType>
    const dense_bitset& get_lvid_bitset(const DGraphType& dgraph) const {
      if (lazy) make_explicit(dgraph);
      return localvset;
    }


    /**
     * \internal
     * Sets a bit in the bitset without local threading
     * synchronization. vertex set must be made explicit. This call does not
     * perform remote synchronization and addititional distributed
     * synchronization calls must be made to restore datastructure invariants.
     */
    inline void set_lvid_unsync(lvid_type lvid) {
      ASSERT_FALSE(lazy);
      localvset.set_bit_unsync(lvid);
    }


    /**
     * \internal
     * Sets a bit in the bitset with local threading
     * synchronization. vertex set must be made explicit. This call does not
     * perform remote synchronization and addititional distributed
     * synchronization calls must be made to restore datastructure invariants.
     */
    inline void set_lvid(lvid_type lvid) {
      ASSERT_FALSE(lazy);
      localvset.set_bit(lvid);
    }

    /**
     * \internal
     * Makes the internal representation explicit by clearing the lazy flag
     * and filling the bitset.
     */
    template <typename DGraphType>
    void make_explicit(const DGraphType& dgraph) const {
      if (lazy) {
        localvset.resize(dgraph.num_local_vertices());
        if (is_complete_set) {
          localvset.fill();
        }
        else {
          localvset.clear();
        }
        lazy = false;
      }
    }

    /**
     * \internal
     * Copies the master state to each mirror.
     * Restores the datastructure invariants.
     */
    template <typename DGraphType>
    void synchronize_master_to_mirrors(DGraphType& dgraph,
                               buffered_exchange<vertex_id_type>& exchange) {
      if (lazy) {
        make_explicit(dgraph);
        return;
      }
      foreach(size_t lvid, localvset) {
        typename DGraphType::local_vertex_type lvtx = dgraph.l_vertex(lvid);
        if (lvtx.owned()) {
          // send to mirrors
          vertex_id_type gvid = lvtx.global_id();
          foreach(size_t proc, lvtx.mirrors()) {
            exchange.send(proc, gvid);
          }
        }
        else {
          localvset.clear_bit_unsync(lvid);
        }
      }
      exchange.flush();

      typename buffered_exchange<vertex_id_type>::buffer_type recv_buffer;
      procid_t sending_proc;

      while(exchange.recv(sending_proc, recv_buffer)) {
        foreach(vertex_id_type gvid, recv_buffer) {
          localvset.set_bit_unsync(dgraph.vertex(gvid).local_id());
        }
        recv_buffer.clear();
      }
      exchange.barrier();
    }


    /**
     * \internal
     * Let the master state be the logical OR of the mirror states.
     */
    template <typename DGraphType>
    void synchronize_mirrors_to_master_or(DGraphType& dgraph,
                               buffered_exchange<vertex_id_type>& exchange) {
      if (lazy) {
        make_explicit(dgraph);
        return;
      }
      foreach(size_t lvid, localvset) {
        typename DGraphType::local_vertex_type lvtx = dgraph.l_vertex(lvid);
        if (!lvtx.owned()) {
          // send to master
          vertex_id_type gvid = lvtx.global_id();
          exchange.send(lvtx.owner(), gvid);
        }
      }
      exchange.flush();

      typename buffered_exchange<vertex_id_type>::buffer_type recv_buffer;
      procid_t sending_proc;

      while(exchange.recv(sending_proc, recv_buffer)) {
        foreach(vertex_id_type gvid, recv_buffer) {
          localvset.set_bit_unsync(dgraph.vertex(gvid).local_id());
        }
        recv_buffer.clear();
      }
      exchange.barrier();
    }

    template <typename VertexType, typename EdgeType>
    friend class distributed_graph;

  public:
    /// default constructor which constructs an empty set.
    vertex_set():is_complete_set(false), lazy(true){}


    /** Constructs a completely empty, or a completely full vertex set
     * \param complete If set to true, creates a set of all vertices.
     *                 If set to false, creates an empty set.
     */
    explicit vertex_set(bool complete):is_complete_set(complete),lazy(true){}

    template <typename DGraphType, typename FunctionType>
        explicit vertex_set(DGraphType& g,
                            FunctionType select_functor,
                            const vertex_set& vset = vertex_set(true)) {
          typedef typename DGraphType::vertex_type vertex_type;
          lazy = true;
          is_complete_set = false;
          make_explicit(g);
#ifdef _OPENMP
              #pragma omp for
#endif
        for (int i = 0; i < (int)g.num_local_vertices(); ++i) {
          auto lvertex = g.l_vertex(i);
          if (lvertex.owned() && vset.l_contains(lvid_type(i))
              && select_functor(vertex_type(lvertex))) {
            set_lvid(i);
          }
        }
        buffered_exchange<typename DGraphType::vertex_id_type> vset_exchange(g.dc());
        synchronize_master_to_mirrors(g, vset_exchange);
    }

    /// copy constructor
    inline vertex_set(const vertex_set& other):
        localvset(other.localvset),
        is_complete_set(other.is_complete_set),
        lazy(other.lazy) {}

    /// copyable
    inline vertex_set& operator=(const vertex_set& other) {
      localvset = other.localvset;
      is_complete_set = other.is_complete_set;
      lazy = other.lazy;
      return *this;
    }

    /**
     * \internal
     * Queries if a local vertex ID is contained within the vertex set
     */
    inline bool l_contains(lvid_type lvid) const {
      if (lazy) return is_complete_set;
      if (lvid < localvset.size()) {
        return localvset.get(lvid);
      }
      else {
        return false;
      }
    }

    /**
     * \brief Takes the set intersection of two vertex sets.
     *
     * \code
     *   vertex_set intersection_result = a & b;
     * \endcode
     * A vertex is in \c intersection_result if and only if the vertex is in
     * \b both set \c a and set \c b.
     */
    inline vertex_set operator&(const vertex_set& other) const {
      vertex_set ret = (*this);
      ret &= other;
      return ret;
    }

    /**
     * \brief Takes the set union of two vertex sets.
     *
     * \code
     *   vertex_set union_result = a | b;
     * \endcode
     * A vertex is in \c union_result if and only if the vertex is in
     * \b either of set \c a and set \c b.
     *
     */
    inline vertex_set operator|(const vertex_set& other) const {
      vertex_set ret = (*this);
      ret |= other;
      return ret;
    }


    /**
     * \brief Takes the set difference of two vertex sets.
     *
     * \code
     *   vertex_set difference_result = a - b;
     * \endcode
     * A vertex is in \c difference_result if and only if the vertex is in
     * set a and not in set b.
     *
     * Equivalent to:
     *
     * \code
     *  vertex_set inv_b = ~b;
     *  vertex_set difference_result = a & inv_b;
     * \endcode
     */
    inline vertex_set operator-(const vertex_set& other) const {
      vertex_set ret = (*this);
      ret -= other;
      return ret;
    }


    /**
     * \brief Takes the set intersection of the current vertex set with another
     * vertex set.
     *
     * \code
     *   a &= b;
     * \endcode
     * A vertex is in the resultant \c a if and only if the vertex was in
     * \b both set \c a and set \c b.
     */
    inline vertex_set& operator&=(const vertex_set& other) {
      if (lazy) {
        if (is_complete_set) (*this) = other;
        else (*this) = vertex_set(false);
      }
      else if (other.lazy) {
        if (other.is_complete_set) /* no op */;
        else (*this) = vertex_set(false);
      }
      else {
        localvset &= other.localvset;
      }
      return *this;
    }

    /**
     * \brief Takes the set union of the current vertex set with another
     * vertex set.
     *
     * \code
     *   a |= b;
     * \endcode
     * A vertex is in the resultant \c a if and only if the vertex was in
     * \b either set \c a and set \c b.
     */
    inline vertex_set& operator|=(const vertex_set& other) {
      if (lazy) {
        if (is_complete_set) (*this) = vertex_set(true);
        else (*this) = other;
      }
      else if (other.lazy) {
        if (other.is_complete_set) (*this) = vertex_set(true);
        else /* no op */;
      }
      else {
        localvset |= other.localvset;
      }
      return *this;
    }

    /**
     * \brief Takes the set difference of the current vertex set with another
     * vertex set.
     *
     * \code
     *   a -= b;
     * \endcode
     * A vertex is in the resultant \c a if and only if the vertex was in
     * set \c a but not in set \c b.
     *
     * Conceptually equivalent to
     * \code
     *   a &= ~b;
     * \endcode
     */
    inline vertex_set& operator-=(const vertex_set& other) {
      if (lazy) {
        if (is_complete_set) (*this) = ~other;
        else (*this) = vertex_set(false);
      }
      else if (other.lazy) {
        if (other.is_complete_set) (*this) = vertex_set(false);
        else /* no op */;
      }
      else {
        localvset -= other.localvset;
      }
      return *this;
    }

    /**
     * \brief Returns the inverse of the current set.
     *
     * \code
     *  vertex_set inv_b = ~b;
     * \endcode
     * A vertex is in \c inv_b if and only if it is not in \c b
     */
    inline vertex_set operator~() const {
      vertex_set ret(*this);
      ret.invert();
      return ret;
    }

    /**
     * \brief Inverts the current set in-place.
     *
     * \code
     * b.invert();
     * \endcode
     * A vertex is in the result \c b if and only if it is not in \c b
     */
    inline void invert() {
      if (lazy) {
        is_complete_set = !is_complete_set;
      }
      else {
        localvset.invert();
      }
    }


};


} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
