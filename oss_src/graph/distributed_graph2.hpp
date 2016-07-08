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

#ifndef GRAPHLAB_DISTRIBUTED_GRAPH2_HPP
#define GRAPHLAB_DISTRIBUTED_GRAPH2_HPP

#ifndef __NO_OPENMP__
#include <omp.h>
#endif

#include <cmath>

#include <string>
#include <list>
#include <vector>
#include <set>
#include <map>
#include <util/dense_bitset.hpp>


#include <queue>
#include <algorithm>
#include <functional>
#include <fstream>
#include <iostream>
#include <sstream>

#include <boost/functional.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/filesystem.hpp>
#include <boost/concept/requires.hpp>


#include <logger/logger.hpp>
#include <logger/assertions.hpp>

#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <rpc/buffered_exchange.hpp>
#include <random/random.hpp>
#include <util/branch_hints.hpp>
#include <graphlab/util/generics/conditional_addition_wrapper.hpp>

#include <graphlab/options/graphlab_options.hpp>
#include <serialization/serialization_includes.hpp>
#include <graphlab/vertex_program/op_plus_eq_concept.hpp>

#include <graph/builtin_parsers.hpp>
#include <graph/dynamic_local_graph.hpp>
#include <graph/graph_gather_apply.hpp>
#include <graph/ingress/distributed_ingress_base.hpp>
#include <graph/ingress/distributed_random_ingress.hpp>
#include <graph/ingress/distributed_oblivious_ingress.hpp>
#include <graph/ingress/distributed_constrained_random_ingress.hpp>
#include <graph/ingress/sharding_constraint.hpp>
#include <graph/graph_hash.hpp>

#include <graphlab/util/hopscotch_map.hpp>

#include <graphlab/util/fs_util.hpp>
#include <fileio/hdfs.hpp>

#include <graphlab/macros_def.hpp>
namespace tests {
class distributed_graph_test;
}
namespace graphlab {


  template<typename VertexData, typename EdgeData>
  class distributed_graph2 {
  public:

    /// The type of the vertex data stored in the graph
    typedef VertexData vertex_data_type;

    /**
     * \cond GRAPHLAB_INTERNAL
     * \brief GraphLab Requires that vertex data be default
     * constructible.  That is you must have a public member:
     *
     * \code
     * class vertex_data {
     * public:
     *   vertex_data() { }
     * }; //
     * \endcode
     */
    BOOST_CONCEPT_ASSERT((boost::DefaultConstructible<VertexData>));
    /// \endcond

    /**
     * \cond GRAPHLAB_INTERNAL
     * \brief GraphLab Requires that vertex data be default
     * Serializable.
     */
    BOOST_CONCEPT_ASSERT((graphlab::Serializable<VertexData>));
    /// \endcond


    /// The type of the edge data stored in the graph
    typedef EdgeData   edge_data_type;


    typedef graphlab::vertex_id_type vertex_id_type;
    typedef graphlab::lvid_type lvid_type;
    typedef graphlab::edge_id_type edge_id_type;

    struct vertex_type;
    typedef bool edge_list_type;
    class edge_type;

    struct local_vertex_type;
    struct local_edge_list_type;
    class local_edge_type;

    /**
     * \cond GRAPHLAB_INTERNAL
     * \brief GraphLab Requires that edge data be default
     * constructible.  That is you must have a public member:
     *
     * \code
     * class edge_data {
     * public:
     *   edge_data() { }
     * }; //
     * \endcode
     */
    BOOST_CONCEPT_ASSERT((boost::DefaultConstructible<EdgeData>));
    /// \endcond

    /**
     * \cond GRAPHLAB_INTERNAL
     * \brief GraphLab Requires that edge data be default
     * Serializable.
     */
    BOOST_CONCEPT_ASSERT((graphlab::Serializable<EdgeData>));
    /// \endcond


    /**
       The line parse is any function (or functor) that has the form:

       <code>
        bool line_parser(distributed_graph& graph, const std::string& filename,
                         const std::string& textline);
       </code>

       the line parser returns true if the line is parsed successfully and
       calls graph.add_vertex(...) or graph.add_edge(...)

       See \ref graphlab::distributed_graph::load(std::string path, line_parser_type line_parser) "load()"
       for details.
     */
    typedef boost::function<bool(distributed_graph2&, const std::string&,
                                 const std::string&)> line_parser_type;


    typedef fixed_dense_bitset<RPC_MAX_N_PROCS> mirror_type;

    /// The type of the local graph used to store the graph data
    typedef graphlab::dynamic_local_graph<VertexData, EdgeData> local_graph_type;

    // boost::unordered_map<vertex_id_type, lvid_type> vid2lvid;
    /** The map from global vertex ids back to local vertex ids */
    typedef hopscotch_map<vertex_id_type, lvid_type> hopscotch_map_type;
    typedef hopscotch_map_type vid2lvid_map_type;

    /// Storing the graph statistics
    class graph_stats;

    /// Storing the distributed vertex info
    class vertex_record;

    /// The type of the local graph structure and data containers 
    typedef typename local_graph_type::data_container_t l_data_container_t;
    typedef typename local_graph_type::structure_container_t l_structure_container_t;

    /// The type of the distributed graph structure and data containers 
    typedef struct {
      vid2lvid_map_type vid2lvid;
      std::vector<vertex_record> lvid2record;
      graph_stats stats;
      l_structure_container_t l_structure;
    } structure_container_t;

    typedef struct {
      std::vector<VertexData>* get_vertex_data() {
        return &(l_data.first);
      }
      std::vector<EdgeData>* get_edge_data() {
        return &(l_data.second);
      }
      l_data_container_t l_data;
    } data_container_t;


    typedef graphlab::distributed_graph2<VertexData, EdgeData> graph_type;

    typedef std::vector<simple_spinlock> lock_manager_type;

    // Make friends with graph ingress classes 
    friend class distributed_ingress_base<graph_type>;
    friend class distributed_random_ingress<graph_type>;
    friend class distributed_oblivious_ingress<graph_type>;
    friend class distributed_constrained_random_ingress<graph_type>;

    // Make friends with graph operation classes 
    template <typename Graph, typename GatherType>
    friend class graph_gather_apply;



    /**
     * \brief Vertex object which provides access to the vertex data
     * and information about the vertex.
     *
     * The vertex_type object may be copied and has very little internal
     * state. It behaves as a reference to location of the vertex
     * in the internal graph representation. While vertex_type may be copied
     * it must not outlive the underlying graph.
     */
    struct vertex_type {
      typedef distributed_graph2 graph_type;
      distributed_graph2& graph_ref;
      lvid_type lvid;

      /// \cond GRAPHLAB_INTERNAL
      /** \brief Constructs a vertex_type object with local vid
       * lvid. This function should not be used directly. Use
       * distributed_graph::vertex() or distributed_graph::l_vertex()
       *
       * \param graph_ref A reference to the parent graph object
       * \param lvid The local VID of the vertex to be accessed
       */
      vertex_type(distributed_graph2& graph_ref, lvid_type lvid):
            graph_ref(graph_ref), lvid(lvid) { }
      /// \endcond

      /// \brief Compares two vertex_type's for equality. Returns true
      //  if they reference the same vertex and false otherwise.
      bool operator==(vertex_type& v) const {
        return lvid == v.lvid;
      }

      /// \brief Returns a constant reference to the data on the vertex
      const vertex_data_type& data() const {
        return graph_ref.get_local_graph().vertex_data(lvid);
      }

      /// \brief Returns a mutable reference to the data on the vertex
      vertex_data_type& data() {
        return graph_ref.get_local_graph().vertex_data(lvid);
      }

      /// \brief Returns the number of in edges of the vertex
      size_t num_in_edges() const {
        return graph_ref.l_get_vertex_record(lvid).num_in_edges;
      }

      /// \brief Returns the number of out edges of the vertex
      size_t num_out_edges() const {
        return graph_ref.l_get_vertex_record(lvid).num_out_edges;
      }

      /// \brief Returns the vertex ID of the vertex
      vertex_id_type id() const {
        return graph_ref.global_vid(lvid);
      }

      /// \cond GRAPHLAB_INTERNAL

      /// \brief Returns a list of in edges (not implemented)
      edge_list_type in_edges() __attribute__ ((noreturn)) {
        ASSERT_TRUE(false);
      }

      /// \brief Returns a list of out edges (not implemented)
      edge_list_type out_edges() __attribute__ ((noreturn)) {
        ASSERT_TRUE(false);
      }
      /// \endcond

      /**
       *  \brief Returns the local ID of the vertex
       */
      lvid_type local_id() const {
        return lvid;
      }
    };


    /**
     * \brief The edge represents an edge in the graph and provide
     * access to the data associated with that edge as well as the
     * source and target distributed::vertex_type objects.
     *
     * An edge object may be copied and has very little internal state
     * and essentially only a reference to the location of the edge
     * information in the underlying graph.  Therefore edge objects
     * can be copied but must not outlive the underlying graph.
     */
    class edge_type {
     private:
       /** \brief An internal reference to the underlying graph */
       distributed_graph2& graph_ref;
       /** \brief The edge in the local graph */
       typename local_graph_type::edge_type edge;

       /**
        * \internal
        * \brief Constructs a edge_type object from a edge_type
        * object of the graphlab::local_graph.
        * lvid. This function should not be used directly.
        *
        * \param graph_ref A reference to the parent graph object
        * \param edge The local graph's edge_type to access
        */
       edge_type(distributed_graph2& graph_ref,
                 typename local_graph_type::edge_type edge):
           graph_ref(graph_ref), edge(edge) { }
       friend class distributed_graph2;
     public:

       /** 
        * \internal
        * Unimplemented default constructor to help with
        * various type inference needs
        */
       edge_type();

       /**
        * \brief Returns the source vertex of the edge.
        * This function returns a vertex_object by value and as a
        * consequence it is possible to use the resulting vertex object
        * to access and *modify* the associated vertex data.
        *
        * Modification of vertex data obtained through an edge object
        * is *usually not safe* and can lead to data corruption.
        *
        * \return The vertex object representing the source vertex.
        */
       vertex_type source() const {
         return vertex_type(graph_ref, edge.source().id());
       }

       /**
        * \brief Returns the target vertex of the edge.
        *
        * This function returns a vertex_object by value and as a
        * consequence it is possible to use the resulting vertex object
        * to access and *modify* the associated vertex data.
        *
        * Modification of vertex data obtained through an edge object
        * is *usually not safe* and can lead to data corruption.
        *
        * \return The vertex object representing the target vertex.
        */
       vertex_type target() const {
         return vertex_type(graph_ref, edge.target().id());
       }

       /**
        * \brief Returns a constant reference to the data on the edge
        */
       const edge_data_type& data() const { return edge.data(); }

       /**
        * \brief Returns a mutable reference to the data on the edge
        */
       edge_data_type& data() { return edge.data(); }
    }; // end of edge_type


    // Storing distributed graph statistics 
    class graph_stats {
     public:
       size_t nverts, nedges, local_own_nverts, nreplicas;

       graph_stats(): nverts(0), nedges(0), local_own_nverts(0), nreplicas(0) {}

       void clear() {
         nverts = nedges = local_own_nverts = nreplicas = 0;
       }

       void save(oarchive& oarc) const {
         oarc << nverts << nedges << local_own_nverts << nreplicas;
       }
       void load(iarchive& iarc) {
         iarc >> nverts >> nedges >> local_own_nverts >> nreplicas;
       }
    };


    // CONSTRUCTORS ==========================================================>
    distributed_graph2(distributed_control& dc,
                       data_container_t& g_data,
                       structure_container_t& g_structure, 
                       const graphlab_options& opts = graphlab_options()) :
        rpc(dc, this), 
        local_graph(g_data.l_data, g_structure.l_structure),
        lvid2record(g_structure.lvid2record),
        vid2lvid(g_structure.vid2lvid),
        stats(g_structure.stats),
        ingress_ptr(NULL),
#ifdef _OPENMP
        vertex_exchange(dc, omp_get_max_threads())
#else
            vertex_exchange(dc) 
#endif
            {
              rpc.barrier();
              set_options(opts);
            }

    distributed_graph2(distributed_control& dc,
                       const graphlab_options& opts = graphlab_options()) :
        rpc(dc, this), 
        local_graph(),
        _lvid2record_ptr(new std::vector<vertex_record>()),
        _vid2lvid_ptr(new vid2lvid_map_type()),
        _graph_stats_ptr(new graph_stats()),
        lvid2record(*_lvid2record_ptr),
        vid2lvid(*_vid2lvid_ptr),
        stats(*_graph_stats_ptr),
        ingress_ptr(NULL),
#ifdef _OPENMP
        vertex_exchange(dc, omp_get_max_threads())
#else
            vertex_exchange(dc) 
#endif
            {
              rpc.barrier();
              set_options(opts);
            }

    /*
     * Disable Default Copy Constructor and copy assignment
     */
    distributed_graph2(const distributed_graph2& other) = delete;

    distributed_graph2& operator=(const distributed_graph2& other) = delete;

    ~distributed_graph2() {
      delete ingress_ptr; ingress_ptr = NULL;
    }

    lock_manager_type& get_lock_manager() {
      return lock_manager;
    }
  private:
    void set_options(const graphlab_options& opts) {
      std::string ingress_method = "";
      std::vector<std::string> keys = opts.get_graph_args().get_option_keys();
      foreach(std::string opt, keys) {
        if (opt == "ingress") {
          opts.get_graph_args().get_option("ingress", ingress_method);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: ingress = "
                                << ingress_method << std::endl;
        } else {
          logstream(LOG_ERROR) << "Unexpected Graph Option: " << opt << std::endl;
        }
      }
      set_ingress_method(ingress_method);
    }

  public:



    // METHODS ===============================================================>
    bool is_dynamic() const {
      ASSERT_TRUE(local_graph.is_dynamic());
      return true;
    }

    /**
     * \brief Commits the graph structure. Once a graph is finalized it may
     * no longer be modified. Must be called on all machines simultaneously.
     *
     * Finalize is used to complete graph ingress by resolving vertex
     * ownship and completing local data structures. Once a graph is finalized
     * its structure may not be modified. Repeated calls to finalize() do
     * nothing.
     */
    void finalize() {
      ASSERT_NE(ingress_ptr, NULL);
      logstream(LOG_INFO) << "Distributed graph: enter finalize" << std::endl;
      ingress_ptr->finalize();
      lock_manager.resize(num_local_vertices());
      rpc.barrier(); 
    }

    /// \brief Returns true if the graph is finalized.
    bool is_finalized() const {
      return true;
    }

    /** \brief Get the number of vertices */
    size_t num_vertices() const { return stats.nverts; }

    /** \brief Get the number of edges */
    size_t num_edges() const { return stats.nedges; }

    /** \brief converts a vertex ID to a vertex object. This function should
     *   not be used without a deep understanding of the distributed graph
     *   representation.
     *
     * This functions converts a global vertex ID to a vertex_type object.
     * The global vertex ID must exist on this machine or assertion failures
     * will be produced.
     */
    vertex_type vertex(vertex_id_type vid) {
      return vertex_type(*this, local_vid(vid));
    }

    /// \cond GRAPHLAB_INTERNAL
    /** \brief Get a list of all in edges of a given vertex ID. Not Implemented */
    edge_list_type in_edges(const vertex_id_type vid) const
        __attribute__((noreturn)) {
          // Not implemented.
          logstream(LOG_WARNING) << "in_edges not implemented. " << std::endl;
          ASSERT_TRUE(false);
        }

    /** Get a list of all out edges of a given vertex ID. Not Implemented */
    edge_list_type out_edges(const vertex_id_type vid) const
        __attribute__((noreturn)) {
          // Not implemented.
          logstream(LOG_WARNING) << "in_edges not implemented. " << std::endl;
          ASSERT_TRUE(false);
        }
    /// \endcond



    /**
     * \brief Returns the number of in edges of a given global vertex ID. This
     * function should not be used without a deep understanding of the
     * distributed graph representation.
     *
     * Returns the number of in edges of a given vertex ID.  Equivalent to
     * vertex(vid).num_in_edges(). The global vertex ID must exist on this
     * machine or assertion failures will be produced.
     */
    size_t num_in_edges(const vertex_id_type vid) const {
      return get_vertex_record(vid).num_in_edges;
    }


    /**
     * \brief Returns the number of out edges of a given global vertex ID. This
     * function should not be used without a deep understanding of the
     * distributed graph representation.
     *
     * Returns the number of out edges of a given vertex ID.  Equivalent to
     * vertex(vid).num_out_edges(). The global vertex ID must exist on this
     * machine or assertion failures will be produced.
     */
    size_t num_out_edges(const vertex_id_type vid) const {
      return get_vertex_record(vid).num_out_edges;
    }


    /**
     * Defines the strategy to use when duplicate vertices are inserted.
     * The default behavior is that an arbitrary vertex data is picked.
     * This allows you to define a combining strategy.
     */
    void set_duplicate_vertex_strategy(boost::function<void(vertex_data_type&,
                                                            const vertex_data_type&)>
                                       combine_strategy) {
      ingress_ptr->set_duplicate_vertex_strategy(combine_strategy);
    }

    /**
     * \brief Creates a vertex containing the vertex data.
     *
     * Creates a vertex with a particular vertex ID and containing a
     * particular vertex data. Vertex IDs need not be sequential, and
     * may arbitrarily span the unsigned integer range of vertex_id_type
     * with the exception of (vertex_id_type)(-1), or corresponding to
     * 0xFFFFFFFF on 32-bit vertex IDs.
     *
     * This function is parallel and distributed. i.e. It does not matter which
     * machine, or which thread on which machines calls add_vertex() for a
     * particular ID.
     *
     * However, each vertex may only be added exactly once.
     *
     * Returns true if successful, returns false if a vertex with id (-1) 
     * was added.
     */
    bool add_vertex(const vertex_id_type& vid,
                    const VertexData& vdata = VertexData(),
#ifdef _OPENMP
                    size_t thread_id = omp_get_thread_num()
#else
                    size_t thread_id = 0
#endif
                   ) {
      if(vid == vertex_id_type(-1)) {
        logstream(LOG_ERROR)
            << "\n\tAdding a vertex with id -1 is not allowed."
            << "\n\tThe -1 vertex id is reserved for internal use."
            << std::endl;
        return false;
      }
      ASSERT_NE(ingress_ptr, NULL);
      ingress_ptr->add_vertex(vid, vdata, thread_id);
      return true;
    }


    /**
     * \brief Creates an edge connecting vertex source, and vertex target().
     *
     * Creates a edge connecting two vertex IDs.
     *
     * This function is parallel and distributed. i.e. It does not matter which
     * machine, or which thread on which machines calls add_edge() for a
     * particular ID.
     *
     * However, each edge direction may only be added exactly once. i.e.
     * if edge 5->6 is added already, no other calls to add edge 5->6 should be
     * made.
     *
     * Returns true on success. Returns false if it is a self-edge, or if
     * we are trying to create a vertex with ID (vertex_id_type)(-1).
     */
    bool add_edge(vertex_id_type source, vertex_id_type target,
                  const EdgeData& edata = EdgeData(),
#ifdef _OPENMP
                          size_t thread_id = omp_get_thread_num()
#else
                          size_t thread_id = 0
#endif
                  ) {
      if(source == vertex_id_type(-1)) {
        logstream(LOG_ERROR)
            << "\n\tThe source vertex with id vertex_id_type(-1)\n"
            << "\tor unsigned value " << vertex_id_type(-1) << " in edge \n"
            << "\t(" << source << "->" << target << ") is not allowed.\n"
            << "\tThe -1 vertex id is reserved for internal use."
            << std::endl;
        return false;
      }
      if(target == vertex_id_type(-1)) {
        logstream(LOG_ERROR)
            << "\n\tThe target vertex with id vertex_id_type(-1)\n"
            << "\tor unsigned value " << vertex_id_type(-1) << " in edge \n"
            << "\t(" << source << "->" << target << ") is not allowed.\n"
            << "\tThe -1 vertex id is reserved for internal use."
            << std::endl;
        return false;
      }
      if(source == target) {
        logstream(LOG_ERROR)
            << "\n\tTrying to add self edge (" << source << "->" << target << ")."
            << "\n\tSelf edges are not allowed."
            << std::endl;
        return false;
      }
      ASSERT_NE(ingress_ptr, NULL);
      ingress_ptr->add_edge(source, target, edata, thread_id);
      return true;
    }




    /** \brief Load the graph from an archive */
    void load(iarchive& arc) {
      // read the vertices
      arc >> vid2lvid
          >> lvid2record
          >> local_graph
          >> stats;
      // check the graph condition
    } // end of load


    /** \brief Save the graph to an archive */
    void save(oarchive& arc) const {
      // Write the number of edges and vertices
      arc << vid2lvid
          << lvid2record
          << local_graph
          << stats;
    } // end of save

    /// \endcond

    /// \brief Clears and resets the graph, releasing all memory used.
    void clear () {
      foreach (vertex_record& vrec, lvid2record)
        vrec.clear();
      lvid2record.clear();
      vid2lvid.clear();
      local_graph.clear();
      stats.clear();
    }


/****************************************************************************
 *                       Internal Functions                                 *
 *                     ----------------------                               *
 * These functions functions and types provide internal access to the       *
 * underlying graph representation. They should not be used unless you      *
 * *really* know what you are doing.                                        *
 ****************************************************************************/


    /**
     * \internal
     * The vertex record stores information associated with each
     * vertex on this proc
     */
    class vertex_record {
     public:
      /// The official owning processor for this vertex
      procid_t owner;
      /// The local vid of this vertex on this proc
      vertex_id_type gvid;
      /// The number of in edges
      vertex_id_type num_in_edges, num_out_edges;
      /** The set of proc that mirror this vertex.  The owner should
          NOT be in this set.*/
      mirror_type _mirrors;
      vertex_record() :
        owner(-1), gvid(-1), num_in_edges(0), num_out_edges(0) { }
      vertex_record(const vertex_id_type& vid) :
        owner(-1), gvid(vid), num_in_edges(0), num_out_edges(0) { }
      procid_t get_owner () const { return owner; }
      const mirror_type& mirrors() const { return _mirrors; }
      size_t num_mirrors() const { return _mirrors.popcount(); }

      void clear() {
        _mirrors.clear();
      }

      void load(iarchive& arc) {
        clear();
        arc >> owner
            >> gvid
            >> num_in_edges
            >> num_out_edges
            >> _mirrors;
      }

      void save(oarchive& arc) const {
        arc << owner
            << gvid
            << num_in_edges
            << num_out_edges
            << _mirrors;
      } // end of save

      bool operator==(const vertex_record& other) const {
        return (
            (owner == other.owner) &&
            (gvid == other.gvid)  &&
            (num_in_edges == other.num_in_edges) &&
            (num_out_edges == other.num_out_edges) && 
            (_mirrors == other._mirrors)
            );
      }
    }; // end of vertex_record


/****************************************************************************
 *                     Vertex Set Functions                                 *
 *                     ----------------------                               *
 * Manages operations involving sets of vertices                            *
 ****************************************************************************/

   /**
    *  \brief Retuns an empty set of vertices
    */
   static vertex_set empty_set() {
     return vertex_set(false);
   }

   /**
    *  \brief Retuns a full set of vertices
    */
   static vertex_set complete_set() {
     return vertex_set(true);
   }

    /** \internal
     * This function synchronizes the master vertex data with all the mirrors.
     * This function must be called simultaneously by all machines
     */
    void synchronize(const vertex_set& vset = complete_set()) {
      typedef std::pair<vertex_id_type, vertex_data_type> pair_type;

      procid_t sending_proc;
      // Loop over all the local vertex records


#ifdef _OPENMP
#pragma omp parallel for
#endif
      for(lvid_type lvid = 0; lvid < lvid2record.size(); ++lvid) {
        typename buffered_exchange<pair_type>::buffer_type recv_buffer;
        const vertex_record& record = lvid2record[lvid];
        // if this machine is the owner of a record then send the
        // vertex data to all mirrors
        if(record.owner == rpc.procid() && vset.l_contains(lvid)) {
          foreach(size_t proc, record.mirrors()) {
            const pair_type pair(record.gvid, local_graph.vertex_data(lvid));
#ifdef _OPENMP
            vertex_exchange.send(proc, pair, omp_get_thread_num());
#else
            vertex_exchange.send(proc, pair);
#endif
          }
        }
        // Receive any vertex data and update local mirrors
        while(vertex_exchange.recv(sending_proc, recv_buffer, true)) {
          foreach(const pair_type& pair, recv_buffer)  {
            vertex(pair.first).data() = pair.second;
          }
          recv_buffer.clear();
        }
      }


      typename buffered_exchange<pair_type>::buffer_type recv_buffer;
      vertex_exchange.flush();
      while(vertex_exchange.recv(sending_proc, recv_buffer)) {
        foreach(const pair_type& pair, recv_buffer) {
          vertex(pair.first).data() = pair.second;
        }
        recv_buffer.clear();
      }
      ASSERT_TRUE(vertex_exchange.empty());
    } // end of synchronize


    /** \internal
     * \brief converts a local vertex ID to a local vertex object
     */
    local_vertex_type l_vertex(lvid_type vid) {
      return local_vertex_type(*this, vid);
    }

    /**
     * \internal
     * \brief Returns the edge data on the edge with ID eid
     */
    edge_data_type& l_edge_data(edge_id_type eid) {
      return local_graph.edge_data(eid);
    }

    /**
     * \internal
     * \brief Returns the edge data on the edge with ID eid
     */
    const edge_data_type& l_edge_data(edge_id_type eid) const {
      return local_graph.edge_data(eid);
    }

    /** \internal
     *\brief Get the Total number of vertex replicas in the graph */
    size_t num_replicas() const { return stats.nreplicas; }

    /** \internal
     *\brief Get the number of vertices local to this proc */
    size_t num_local_vertices() const { return local_graph.num_vertices(); }

    /** \internal
     *\brief Get the number of edges local to this proc */
    size_t num_local_edges() const { return local_graph.num_edges(); }

    /** \internal
     *\brief Get the number of vertices owned by this proc */
    size_t num_local_own_vertices() const { return stats.local_own_nverts; }

    /** \internal
     *\brief Convert a global vid to a local vid */
    lvid_type local_vid (const vertex_id_type vid) const {
      // typename boost::unordered_map<vertex_id_type, lvid_type>::
      //   const_iterator iter = vid2lvid.find(vid);
      typename hopscotch_map_type::const_iterator iter = vid2lvid.find(vid);
      return iter->second;
    } // end of local_vertex_id

    /** \internal
     *\brief Convert a local vid to a global vid */
    vertex_id_type global_vid(const lvid_type lvid) const {
      ASSERT_LT(lvid, lvid2record.size());
      return lvid2record[lvid].gvid;
    } // end of global_vertex_id


    /** \internal
     * \brief Returns true if the local graph as an instance of (master or mirror)
     * of the vertex ID.
     */
    bool contains_vertex(const vertex_id_type vid) const {
      return vid2lvid.find(vid) != vid2lvid.end();
    }
    /**
     * \internal
     * \brief Returns an edge list of all in edges of a local vertex ID
     *        on the local graph
     *
     * Equivalent to l_vertex(lvid).in_edges()
     */
    local_edge_list_type l_in_edges(const lvid_type lvid) {
      return local_edge_list_type(*this, local_graph.in_edges(lvid));
    }

    /**
     * \internal
     * \brief Returns the number of in edges of a local vertex ID
     *        on the local graph
     *
     * Equivalent to l_vertex(lvid).num in_edges()
     */
    size_t l_num_in_edges(const lvid_type lvid) const {
      return local_graph.num_in_edges(lvid);
    }

    /**
     * \internal
     * \brief Returns an edge list of all out edges of a local vertex ID
     *        on the local graph
     *
     * Equivalent to l_vertex(lvid).out_edges()
     */
    local_edge_list_type l_out_edges(const lvid_type lvid) {
      return local_edge_list_type(*this, local_graph.out_edges(lvid));
    }

    /**
     * \internal
     * \brief Returns the number of out edges of a local vertex ID
     *        on the local graph
     *
     * Equivalent to l_vertex(lvid).num out_edges()
     */
    size_t l_num_out_edges(const lvid_type lvid) const {
      return local_graph.num_out_edges(lvid);
    }

    procid_t procid() const {
      return rpc.procid();
    }


    procid_t numprocs() const {
      return rpc.numprocs();
    }

    distributed_control& dc() const {
      return rpc.dc();
    }



    /** \internal
     * \brief Returns the internal vertex record of a given global vertex ID
     */
    const vertex_record& get_vertex_record(vertex_id_type vid) const {
      // typename boost::unordered_map<vertex_id_type, lvid_type>::
      //   const_iterator iter = vid2lvid.find(vid);
      typename hopscotch_map_type::const_iterator iter = vid2lvid.find(vid);
      ASSERT_TRUE(iter != vid2lvid.end());
      return lvid2record[iter->second];
    }

    /** \internal
     * \brief Returns the internal vertex record of a given local vertex ID
     */
    vertex_record& l_get_vertex_record(lvid_type lvid) {
      ASSERT_LT(lvid, lvid2record.size());
      return lvid2record[lvid];
    }

    /** \internal
     * \brief Returns the internal vertex record of a given local vertex ID
     */
    const vertex_record& l_get_vertex_record(lvid_type lvid) const {
      ASSERT_LT(lvid, lvid2record.size());
      return lvid2record[lvid];
    }

    /** \internal
     * \brief Returns true if the provided global vertex ID is a
     *        master vertex on this machine and false otherwise.
     */
    bool is_master(vertex_id_type vid) const {
      const procid_t owning_proc = graph_hash::hash_vertex(vid) % rpc.numprocs();
      return (owning_proc == rpc.procid());
    }


    procid_t master(vertex_id_type vid) const {
      const procid_t owning_proc = graph_hash::hash_vertex(vid) % rpc.numprocs();
      return owning_proc;
    }

    /** \internal
     * \brief Returns true if the provided local vertex ID is a master vertex.
     *        Returns false otherwise.
     */
    bool l_is_master(lvid_type lvid) const {
      ASSERT_LT(lvid, lvid2record.size());
      return lvid2record[lvid].owner == rpc.procid();
    }

    /** \internal
     * \brief Returns the master procid for vertex lvid.
     */
    procid_t l_master(lvid_type lvid) const {
      ASSERT_LT(lvid, lvid2record.size());
      return lvid2record[lvid].owner;
    }


    /** \internal
     *  \brief Returns a reference to the internal graph representation
     */
    local_graph_type& get_local_graph() {
      return local_graph;
    }

    /** \internal
     *  \brief Returns a const reference to the internal graph representation
     */
    const local_graph_type& get_local_graph() const {
      return local_graph;
    }

    /** \internal
     *  vertex type while provides access to local graph vertices.
     */
    struct local_vertex_type {
      distributed_graph2& graph_ref;
      lvid_type lvid;

      local_vertex_type(distributed_graph2& graph_ref, lvid_type lvid):
            graph_ref(graph_ref), lvid(lvid) { }

      /// \brief Can be casted from local_vertex_type using an explicit cast
      explicit local_vertex_type(vertex_type v) :graph_ref(v.graph_ref),lvid(v.lvid) { }
      /// \brief Can be casted to vertex_type using an explicit cast
      operator vertex_type() const {
        return vertex_type(graph_ref, lvid);
      }

      bool operator==(local_vertex_type& v) const {
        return lvid == v.lvid;
      }

      /// \brief Returns a reference to the data on the local vertex
      const vertex_data_type& data() const {
        return graph_ref.get_local_graph().vertex_data(lvid);
      }

      /// \brief Returns a reference to the data on the local vertex
      vertex_data_type& data() {
        return graph_ref.get_local_graph().vertex_data(lvid);
      }

      /** \brief Returns the number of in edges on the
       *         local graph of this local vertex
       */
      size_t num_in_edges() const {
        return graph_ref.get_local_graph().num_in_edges(lvid);
      }

      /** \brief Returns the number of in edges on the
       *         local graph of this local vertex
       */
      size_t num_out_edges() const {
        return graph_ref.get_local_graph().num_out_edges(lvid);
      }

      /// \brief Returns the local ID of this local vertex
      lvid_type id() const {
        return lvid;
      }

      /// \brief Returns the global ID of this local vertex
      vertex_id_type global_id() const {
        return graph_ref.global_vid(lvid);
      }

      /** \brief Returns a list of all in edges on the
       *         local graph of this local vertex
       */
      local_edge_list_type in_edges() {
        return graph_ref.l_in_edges(lvid);
      }

      /** \brief Returns a list of all out edges on the
       *         local graph of this local vertex
       */
      local_edge_list_type out_edges() {
        return graph_ref.l_out_edges(lvid);
      }

      /** \brief Returns the owner of this local vertex
       */
      procid_t owner() const {
        return graph_ref.l_get_vertex_record(lvid).owner;
      }

      /** \brief Returns the owner of this local vertex
       */
      bool owned() const {
        return graph_ref.l_get_vertex_record(lvid).owner == graph_ref.procid();
      }

      /** \brief Returns the number of in_edges of this vertex
       *         on the global graph
       */
      size_t global_num_in_edges() const {
        return graph_ref.l_get_vertex_record(lvid).num_in_edges;
      }


      /** \brief Returns the number of out_edges of this vertex
       *         on the global graph
       */
      size_t global_num_out_edges() const {
        return graph_ref.l_get_vertex_record(lvid).num_out_edges;
      }


      /** \brief Returns the set of mirrors of this vertex
       */
      const mirror_type& mirrors() const {
        return graph_ref.l_get_vertex_record(lvid)._mirrors;
      }

      size_t num_mirrors() const {
        return graph_ref.l_get_vertex_record(lvid).num_mirrors();
      }

      /** \brief Returns the vertex record of this
       *         this local vertex
       */
      vertex_record& get_vertex_record() {
        return graph_ref.l_get_vertex_record(lvid);
      }
    };

    /** \internal
     *  edge type which provides access to local graph edges */
    class local_edge_type {
    private:
      distributed_graph2& graph_ref;
      typename local_graph_type::edge_type e;
    public:
      local_edge_type(distributed_graph2& graph_ref,
                      typename local_graph_type::edge_type e):
                                                graph_ref(graph_ref), e(e) { }

      /// \brief Can be converted from edge_type via an explicit cast
      explicit local_edge_type(edge_type ge) :graph_ref(ge.graph_ref),e(ge.e) { }

      /// \brief Can be casted to edge_type using an explicit cast
      operator edge_type() const {
        return edge_type(graph_ref, e);
      }

      /// \brief Returns the source local vertex of the edge
      local_vertex_type source() const { return local_vertex_type(graph_ref, e.source().id()); }

      /// \brief Returns the target local vertex of the edge
      local_vertex_type target() const { return local_vertex_type(graph_ref, e.target().id()); }



      /// \brief Returns a constant reference to the data on the vertex
      const edge_data_type& data() const { return e.data(); }

      /// \brief Returns a reference to the data on the vertex
      edge_data_type& data() { return e.data(); }

      /// \brief Returns the internal ID of this edge
      edge_id_type id() const { return e.id(); }
    };

    /** \internal
     * \brief A functor which converts local_graph_type::edge_type to
     *        local_edge_type
     */
    struct make_local_edge_type_functor {
      typedef typename local_graph_type::edge_type argument_type;
      typedef local_edge_type result_type;
      distributed_graph2& graph_ref;
      make_local_edge_type_functor(distributed_graph2& graph_ref):
                                                      graph_ref(graph_ref) { }
      result_type operator() (const argument_type et) const {
        return local_edge_type(graph_ref, et);
      }
    };


    /** \internal
     * \brief A list of edges. Used by l_in_edges() and l_out_edges()
     */
    struct local_edge_list_type {
      make_local_edge_type_functor me_functor;
      typename local_graph_type::edge_list_type elist;

      typedef boost::transform_iterator<make_local_edge_type_functor,
                                      typename local_graph_type::edge_list_type::iterator> iterator;
      typedef iterator const_iterator;

      local_edge_list_type(distributed_graph2& graph_ref,
                           typename local_graph_type::edge_list_type elist) :
                          me_functor(graph_ref), elist(elist) { }
      /// \brief Returns the number of edges in the list
      size_t size() const { return elist.size(); }

      /// \brief Random access to the list elements
      local_edge_type operator[](size_t i) const { return me_functor(elist[i]); }

      /** \brief Returns an iterator to the beginning of the list.
       *
       * Returns an iterator to the beginning of the list. \see end()
       * The iterator_type is local_edge_list_type::iterator.
       *
       * \code
       * local_edge_list_type::iterator iter = elist.begin();
       * while(iter != elist.end()) {
       *   ... [do stuff] ...
       *   ++iter;
       * }
       * \endcode
       *
      */
      iterator begin() const { return
          boost::make_transform_iterator(elist.begin(), me_functor); }

      /** \brief Returns an iterator to the end of the list.
       *
       * Returns an iterator to the end of the list. \see begin()
       * The iterator_type is local_edge_list_type::iterator.
       *
       * \code
       * local_edge_list_type::iterator iter = elist.begin();
       * while(iter != elist.end()) {
       *   ... [do stuff] ...
       *   ++iter;
       * }
       * \endcode
       *
      */
      iterator end() const { return
          boost::make_transform_iterator(elist.end(), me_functor); }

      /// \brief Returns true if the list is empty
      bool empty() const { return elist.empty(); }
    };

  private:

    // PRIVATE DATA MEMBERS ===================================================>
    /** The rpc interface for this class */
    mutable dc_dist_object<distributed_graph2> rpc;


  public:

    // For the warp engine to find the remote instances of this class
    size_t get_rpc_obj_id() {
      return rpc.get_obj_id();
    }

  private:
    /** The local graph data */
    local_graph_type local_graph;

    /**
     * Internal data holding pointers for default constructor.
     */
    std::shared_ptr< std::vector<vertex_record> > _lvid2record_ptr;
    std::shared_ptr< vid2lvid_map_type > _vid2lvid_ptr;
    std::shared_ptr< graph_stats > _graph_stats_ptr;

    /** The map from global vertex ids to vertex records */
    std::vector<vertex_record>&  lvid2record;

    /** The map from global vertex ids to local vertex ids*/
    vid2lvid_map_type& vid2lvid;

    /**
     * Statistics of the graph: num_edges, num_vertices, num_replicas... 
     */
    graph_stats& stats;

    /** pointer to the distributed ingress object*/
    distributed_ingress_base<graph_type>* ingress_ptr;

    /** Buffered Exchange used by synchronize() */
    buffered_exchange<std::pair<vertex_id_type, vertex_data_type> > vertex_exchange;

    lock_manager_type lock_manager;

    void set_ingress_method(const std::string& method) {
      if(ingress_ptr != NULL) { delete ingress_ptr; ingress_ptr = NULL; }
      if  (method == "random") {
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use random ingress" << std::endl;
        ingress_ptr = new distributed_random_ingress<graph_type>(rpc.dc(), *this); 
      } else if (method == "grid") {
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use grid ingress" << std::endl;
        ingress_ptr = new distributed_constrained_random_ingress<graph_type>(rpc.dc(), *this, "grid");
      } else if (method == "pds") {
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Use pds ingress" << std::endl;
        ingress_ptr = new distributed_constrained_random_ingress<graph_type>(rpc.dc(), *this, "pds");
      } else {
        // use default ingress method if none is specified
        std::string ingress_auto="";
        size_t num_shards = rpc.numprocs();
        int nrow, ncol, p;
        if (sharding_constraint::is_pds_compatible(num_shards, p)) {
          ingress_auto="pds";
          ingress_ptr = new distributed_constrained_random_ingress<graph_type>(rpc.dc(), *this, "pds");
        } else if (sharding_constraint::is_grid_compatible(num_shards, nrow, ncol)) {
          ingress_auto="grid";
          ingress_ptr = new distributed_constrained_random_ingress<graph_type>(rpc.dc(), *this, "grid");
        } else {
          ingress_auto="oblivious";
          ingress_ptr = new distributed_oblivious_ingress<graph_type>(rpc.dc(), *this);
        }
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Automatically determine ingress method: " << ingress_auto << std::endl;
      }
    } // end of set ingress method

    friend class tests::distributed_graph_test;
  }; // End of graph
} // end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif
