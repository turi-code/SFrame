/**
 * Copyright (C) 2015 Dato, Inc.
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

#ifndef GRAPHLAB_DISTRIBUTED_GRAPH_HPP
#define GRAPHLAB_DISTRIBUTED_GRAPH_HPP

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

#include <parallel/lambda_omp.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <rpc/buffered_exchange.hpp>
#include <random/random.hpp>
#include <util/branch_hints.hpp>
#include <graphlab/util/generics/conditional_addition_wrapper.hpp>

#include <graphlab/options/graphlab_options.hpp>
#include <serialization/serialization_includes.hpp>
#include <graphlab/vertex_program/op_plus_eq_concept.hpp>

#include <graph/local_graph.hpp>
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


#include <graph/builtin_parsers.hpp>
#include <graph/vertex_set.hpp>

#include <graphlab/macros_def.hpp>
namespace tests {
class distributed_graph_test;
}
namespace graphlab {

  /** \brief A directed graph datastructure which is distributed across
   * multiple machines.
   *
   * This class implements a distributed directed graph datastructure where
   * vertices and edges may contain arbitrary user-defined datatypes as
   * templatized by the VertexData and EdgeData template parameters.
   *
   * ### Initialization
   *
   * To declare a distributed graph you write:
   * \code typedef
   * graphlab::distributed_graph<vdata, edata> graph_type;
   * graph_type graph(dc, clopts);
   * \endcode
   * where <code>vdata</code> is the type of data to be
   * stored on vertices, and <code>edata</code> is the type of data to be
   * stored on edges. The constructor must be called simultaneously on all
   * machines. <code>dc</code> is a graphlab::distributed_control object that
   * must be constructed at the start of the program, and clopts is a
   * graphlab::graphlab_options object that is used to pass graph
   * construction runtime options to the graph. See the code examples for
   * further details.
   *
   * Each vertex is uniquely identified by an unsigned  numeric ID of the type
   * graphlab::vertex_id_type. Vertex IDs need not be sequential. However, the
   * ID corresponding to <code>(vertex_id_type)(-1)</code> is reserved.  (This
   * is the largest possible ID, corresponding to 0xFFFFFFFF when using 32-bit
   * IDs).
   *
   * Edges are not numbered, but are uniquely identified by its source->target
   * pair. In other words, there can only be two edges between any pair of
   * vertices, the edge going in the forward direction, and the edge going in
   * the backward direction.
   *
   * ### Construction
   *
   * The distributed graph can be constructed in two different ways.  The
   * first, and the preferred method, is to construct the graph from files
   * located on a shared filesystem (NFS mounts for instance) , or from files
   * on HDFS (HDFS support must be compiled).
   *
   * To construct from files, the load_format() function provides built-in
   * parsers to construct the graph structure from various graph file formats
   * on disk or HDFS. Alternatively, the
   * \ref load(const std::string& path, line_parser_type line_parser) "load()"
   * function provides generalized parsing capabilities
   * allowing you to construct from your own defined file format.
   * Alternatively, load_binary() may be used to perform an extremely rapid
   * load of a graph previously saved with save_binary(). The caveat being that
   * the number of machines used to save the graph must match the number of
   * machines used to load the graph.
   *
   * The second construction strategy is to call the add_vertex() and
   * add_edge() functions directly. These functions are parallel reentrant, and
   * are also distributed. Each vertex and each edge should be added no more
   * than once across all machines.
   *
   * add_vertex() calls are not strictly required since add_edge(i, j) will
   * implicitly construct vertices i and j. The data on these vertices
   * will be default constructed.
   *
   * ### Finalization
   * After all vertices and edges are inserted into the graph
   * via either load from file functions or direct calls to add_vertex() and
   * add_edge(), for the graph to the useable, it must be finalized.
   *
   * This is performed by calling \code graph.finalize(); \endcode on all
   * machines simultaneously. None of the load* functions perform finalization
   * so multiple load operations could be performed (reading from different
   * file groups) before finalization.
   *
   * The finalize() operation partitions the graph and synchronizes all
   * internal graph datastructures. After this point, all graph computation
   * operations such as engine, map_reduce and transform operations will
   * function.
   *
   * ### Partitioning Strategies
   *
   * The graph is partitioned across the machines using a "vertex separator"
   * strategy where edges are assigned to machines, while vertices may span
   * multiple machines. There are three partitioning strategies implemented.
   * These can be selected by setting --graph_opts="ingress=[partition_method]"
   * on the command line.
   * \li \c "random" The most naive and the fastest partitioner. Random places
   *                 edges on machines.
   * \li \c "oblivious" Runs at roughly half the speed of random. Machines
   *                    indepedently partitions the segment of the graph it
   *                    read. Improves partitioning quality and will reduce
   *                    runtime memory consumption.
   *
   * \li \c "grid" Runs at rouphly the same speed of random. Randomly places
   *                edges on machines with a grid constraint.
   *                This obtains quality partition, close to oblivious,
   *                but currently only works with perfect square number of machines.
   *
   * \li \c "pds"  Runs at roughly the speed of random. Randomly places
   *                edges on machines with a sparser constraint generated by
   *                perfect difference set.  This obtains the highest quality partition,
   *                reducing runtime memory consumption significantly, without load-time penalty.
   *                Currently only works with p^2+p+1 number of machines (p prime).
   *
   * ### Referencing Vertices / Edges Many GraphLab operations will pass around
   * vertex_type and edge_type objects. These objects are light-weight copyable
   * opaque references to vertices and edges in the distributed graph.  The
   * vertex_type object provides capabilities such as:
   * \li \c vertex_type::id() Returns the ID of the vertex
   * \li \c vertex_type::num_in_edges() Returns the number of in edges
   * \li \c vertex_type::num_out_edges() Returns the number of out edges
   * \li \c vertex_type::data() Returns a <b>reference</b> to the data on
   *                            the vertex
   *
   * No traversal operations are currently provided and there there is no
   * single method to return a list of adjacent edges to the vertex.
   *
   * The edge_type object has similar capabilities:
   * \li \c edge_type::data() Returns a <b>reference</b> to the data on the edge
   * \li \c edge_type::source() Returns a \ref vertex_type of the source vertex
   * \li \c edge_type::target() Returns a \ref vertex_type of the target vertex
   *
   * This permits the use of <code>edge.source().data()</code> for instance, to
   * obtain the vertex data on the source vertex.
   *
   * See the documentation for \ref vertex_type and \ref edge_type for further
   * details.
   *
   * Due to the distributed nature of the graph, There is at the moment, no way
   * to obtain a reference to arbitrary vertices or edges. The only way to
   * obtain a reference to vertices or edges, is if one is passed to you via a
   * callback (for instance in map_reduce_vertices() / map_reduce_edges() or in
   * an update function). To manipulate the graph at a more fine-grained level
   * will require a more intimate understanding of the underlying distributed
   * graph representation.
   *
   * ### Saving the graph
   * After computation is complete, the graph structure can be saved
   * via save_format() which provides built-in writers to write various
   * graph formats to disk or HDFS. Alternatively,
   * \ref save(const std::string& prefix, writer writer, bool gzip, bool save_vertex, bool save_edge, size_t files_per_machine) "save()"
   * provides generalized writing capabilities allowing you to write
   * your own graph output to disk or HDFS.
   *
   * ### Distributed Representation
   * The graph is partitioned over machines using vertex separators.
   * In other words, each edge is assigned to a unique machine while
   * vertices are allowed to span multiple machines.
   *
   * The image below demonstrates the procedure. The example graph
   * on the left is to be separated among 4 machines where the cuts
   * are denoted by the dotted red lines. After partitioning,
   * (the image on the right), each vertex along the cut
   * is now separated among multiple machines. For instance, the
   * central vertex spans 4 different machines.
   *
   * \image html partition_fig.gif
   *
   * Each vertex which span multiple machines, has a <b>master</b>
   * machine (a black vertex), and all other instances of the vertex
   * are called <b>mirrors</b>. For instance, we observe that the central
   * vertex spans 4 machines, where machine 3 holds the <b>master</b>
   * copy, while all remaining machines hold <b>mirrored</b> copies.
   *
   * This concept of vertex separators allow us to easily manage large
   * power-law graphs where vertices may have extremely high degrees,
   * since the adjacency information for even the high degree vertices
   * can be separated across multiple machines.
   *
   * ### Internal Representation
   * \warning This is only useful if you plan to make use of the graph
   * in ways which exceed the provided abstractions.
   *
   * Each machine maintains its local section of the graph in a
   * graphlab::local_graph object. The local_graph object assigns
   * each vertex a sequential vertex ID called the local vertex ID.
   * A hash table is used to provide a mapping between the local vertex IDs
   * and their corresponding global vertex IDs. Additionally, each local
   * vertex is associated with a \ref vertex_record which provides information
   * about global ID of the vertex, the machine which holds the master instance
   * of the vertex, as well as a list of all machines holding a mirror
   * of the vertex.
   *
   * To support traversal of the local graph, two additional types, the
   * \ref local_vertex_type and the \ref local_edge_type is provided which
   * provide references to vertices and edges on the local graph. These behave
   * similarly to the \ref vertex_type and \ref edge_type types and have
   * similar functionality. However, since these reference the local graph,
   * there is substantially more flexility. In particular, the function
   * l_vertex() may be used to obtain a reference to a local vertex from a
   * local vertex ID. Also unlike the \ref vertex_type , the \ref
   * local_vertex_type support traversal operations such as returning a list of
   * all in_edges (local_vertex_type::in_edges()).  However, the list only
   * contains the edges which are local to the current machine. See
   * \ref local_vertex_type and \ref local_edge_type for more details.
   *
   *
   * \tparam VertexData Type of data stored on vertices. Must be
   *                    Copyable, Default Constructable, Copy
   *                    Constructable and \ref sec_serializable.
   * \tparam EdgeData Type of data stored on edges. Must be
   *                  Copyable, Default Constructable, Copy
   *                  Constructable and \ref sec_serializable.
   */
  template<typename VertexData, typename EdgeData>
  class distributed_graph {
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
    typedef boost::function<bool(distributed_graph&, const std::string&,
                                 const std::string&)> line_parser_type;


    typedef fixed_dense_bitset<RPC_MAX_N_PROCS> mirror_type;

    // boost::unordered_map<vertex_id_type, lvid_type> vid2lvid;
    /** The map from global vertex ids back to local vertex ids */
    typedef hopscotch_map<vertex_id_type, lvid_type> hopscotch_map_type;
    typedef hopscotch_map_type vid2lvid_map_type;

    /// Storing the graph statistics
    class graph_stats;

    /// Storing the distributed vertex info
    class vertex_record;


    /// The type of the local graph used to store the graph data
#ifdef USE_DYNAMIC_LOCAL_GRAPH
    typedef graphlab::dynamic_local_graph<VertexData, EdgeData> local_graph_type;
#else 
    typedef graphlab::local_graph<VertexData, EdgeData> local_graph_type;
#endif
    typedef graphlab::distributed_graph<VertexData, EdgeData> graph_type;

    typedef std::vector<simple_spinlock> lock_manager_type;

    friend class distributed_ingress_base<graph_type>;


    // Make friends with graph operation classes 
    template <typename Graph, typename GatherType>
    friend class graph_gather_apply;


    // Make friends with Ingress classes
    friend class distributed_random_ingress<graph_type>;
    friend class distributed_oblivious_ingress<graph_type>;
    friend class distributed_constrained_random_ingress<graph_type>;

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
      typedef distributed_graph graph_type;
      distributed_graph& graph_ref;
      lvid_type lvid;

      /// \cond GRAPHLAB_INTERNAL
      /** \brief Constructs a vertex_type object with local vid
       * lvid. This function should not be used directly. Use
       * distributed_graph::vertex() or distributed_graph::l_vertex()
       *
       * \param graph_ref A reference to the parent graph object
       * \param lvid The local VID of the vertex to be accessed
       */
      vertex_type(distributed_graph& graph_ref, lvid_type lvid):
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
      distributed_graph& graph_ref;
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
      edge_type(distributed_graph& graph_ref,
                typename local_graph_type::edge_type edge):
        graph_ref(graph_ref), edge(edge) { }
      friend class distributed_graph;
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
    class graph_stats: public IS_POD_TYPE {
     public:
      size_t nverts, nedges, local_own_nverts, nreplicas;

      graph_stats(): nverts(0), nedges(0), local_own_nverts(0), nreplicas(0) {}

      void clear() {
        nverts = nedges = local_own_nverts = nreplicas = 0;
      }
    };

    // CONSTRUCTORS ==========================================================>

    /**
     * Constructs a distributed graph. All machines must call this constructor
     * simultaneously.
     *
     * Value graph options are:
     * \li \c ingress The graph partitioning method to use. May be "random"
     *                "grid" or "pds". The methods have roughly the same runtime 
     *                complexity, but the increasing partition qaulity. "grid" 
     *                requires number of machine P be able to layout as a n*m = P 
     *                grid with ( |m-n| <= 2). "pds" uses requires P = p^2+p+1 where 
     *                p is a prime number.
     *
     * \li \c userecent An optimization that can decrease memory utilization
     *                of oblivious and batch quite significantly (especially
     *                when there are a large number of machines) at a small
     *                partitioning penalty. Defaults to 0. Set to 1 to
     *                enable.
     * \li \c bufsize The batch size used by the batch ingress method.
     *                Defaults to 50,000. Increasing this number will
     *                decrease partitioning time with a penalty to partitioning
     *                quality.
     *
     * \param [in] dc Distributed controller to associate with
     * \param [in] opts A graphlab::graphlab_options object specifying engine
     *                  parameters.  This is typically constructed using
     *                  \ref graphlab::command_line_options.
     */
    distributed_graph(distributed_control& dc,
                      const graphlab_options& opts = graphlab_options()) :
      rpc(dc, this), finalized(false), vid2lvid(), stats(), ingress_ptr(NULL), 
#ifdef _OPENMP
      vertex_exchange(dc, omp_get_max_threads()), 
#else
      vertex_exchange(dc), 
#endif
      vset_exchange(dc), parallel_ingress(true) {
      rpc.barrier();
      set_options(opts);
    }

    ~distributed_graph() {
      delete ingress_ptr; ingress_ptr = NULL;
    }


    lock_manager_type& get_lock_manager() {
      return lock_manager;
    }
  private:
    void set_options(const graphlab_options& opts) {
      size_t bufsize = 50000;
      bool usehash = false;
      bool userecent = false;
      std::string ingress_method = "";
      std::vector<std::string> keys = opts.get_graph_args().get_option_keys();
      foreach(std::string opt, keys) {
        if (opt == "ingress") {
          opts.get_graph_args().get_option("ingress", ingress_method);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: ingress = "
              << ingress_method << std::endl;
        } else if (opt == "parallel_ingress") {
         opts.get_graph_args().get_option("parallel_ingress", parallel_ingress);
          if (!parallel_ingress && rpc.procid() == 0)
            logstream(LOG_EMPH) << "Disable parallel ingress. Graph will be streamed through one node."
              << std::endl;
        }
        /**
         * These options below are deprecated.
         */
        else if (opt == "bufsize") {
          opts.get_graph_args().get_option("bufsize", bufsize);
           if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: bufsize = "
              << bufsize << std::endl;
       } else if (opt == "usehash") {
          opts.get_graph_args().get_option("usehash", usehash);
          if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: usehash = "
              << usehash << std::endl;
        } else if (opt == "userecent") {
          opts.get_graph_args().get_option("userecent", userecent);
           if (rpc.procid() == 0)
            logstream(LOG_EMPH) << "Graph Option: userecent = "
              << userecent << std::endl;
        }  else {
          logstream(LOG_ERROR) << "Unexpected Graph Option: " << opt << std::endl;
        }
    }
      set_ingress_method(ingress_method, bufsize, usehash, userecent);
    }

  public:



    // METHODS ===============================================================>
    bool is_dynamic() const {
      return local_graph.is_dynamic();
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
#ifndef USE_DYNAMIC_LOCAL_GRAPH
      if (finalized) return;
#endif
      ASSERT_NE(ingress_ptr, NULL);
      logstream(LOG_INFO) << "Distributed graph: enter finalize" << std::endl;
      ingress_ptr->finalize();
      lock_manager.resize(num_local_vertices());
      rpc.barrier(); 

      finalized = true;
    }

    /// \brief Returns true if the graph is finalized.
    bool is_finalized() {
      return finalized;
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
      thread_id = thread_id % MAX_BUFFER_LOCKS;
#ifndef USE_DYNAMIC_LOCAL_GRAPH
      if(finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to add a vertex to a finalized graph."
          << "\n\tVertices cannot be added to a graph after finalization."
          << std::endl;
      }
#else
      finalized = false;
#endif
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
      thread_id = thread_id % MAX_BUFFER_LOCKS;
#ifndef USE_DYNAMIC_LOCAL_GRAPH
      if(finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to add an edge to a finalized graph."
          << "\n\tEdges cannot be added to a graph after finalization."
          << std::endl;
      }
#else 
      finalized = false;
#endif
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


   /**
    * \brief Performs a map-reduce operation on each vertex in the
    * graph returning the result.
    *
    * Given a map function, map_reduce_vertices() call the map function on all
    * vertices in the graph. The return values are then summed together and the
    * final result returned. The map function should only read the vertex data
    * and should not make any modifications. map_reduce_vertices() must be
    * called on all machines simultaneously.
    *
    * ### Basic Usage
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    *
    * To compute an absolute sum over all the vertex data, we would write
    * a function which reads in each a vertex, and returns the absolute
    * value of the data on the vertex.
    * \code
    * float absolute_vertex_data(const graph_type::vertex_type& vertex) {
    *   return std::fabs(vertex.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = graph.map_reduce_vertices<float>(absolute_vertex_data);
    * \endcode
    * will call the <code>absolute_vertex_data()</code> function
    * on each vertex in the graph. <code>absolute_vertex_data()</code>
    * reads the value of the vertex and returns the absolute result.
    * This return values are then summed together and returned.
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the mapfunction.
    *
    * The optional argument vset can be used to restrict he set of vertices
    * map-reduced over.
    *
    * ### Relations
    * This function is similar to
    * graphlab::iengine::map_reduce_vertices()
    * with the difference that this does not take a context
    * and thus cannot influence engine signalling.
    * transform_vertices() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam VertexMapperType The type of the map function.
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param mapfunction The map function to use. Must take
    *                   a \ref vertex_type, or a reference to a
    *                   \ref vertex_type as its only argument.
    *                   Returns a ReductionType which must be summable
    *                   and \ref sec_serializable .
    * \param vset The set of vertices to map reduce over. Optional. Defaults to
    *             complete_set()
    */
    template <typename ReductionType, typename MapFunctionType>
    ReductionType map_reduce_vertices(MapFunctionType mapfunction,
                                      const vertex_set& vset = complete_set()) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      if(!finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to run graph.map_reduce_vertices(...) "
          << "\n\tbefore calling graph.finalize()."
          << std::endl;
      }

      rpc.barrier();
      bool global_result_set = false;
      ReductionType global_result = ReductionType();
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        bool result_set = false;
        ReductionType result = ReductionType();
#ifdef _OPENMP
        #pragma omp for
#endif
        for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
          if (lvid2record[i].owner == rpc.procid() &&
              vset.l_contains((lvid_type)i)) {
            if (!result_set) {
              const vertex_type vtx(l_vertex(i));
              result = mapfunction(vtx);
              result_set = true;
            }
            else if (result_set){
              const vertex_type vtx(l_vertex(i));
              const ReductionType tmp = mapfunction(vtx);
              result += tmp;
            }
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          if (result_set) {
            if (!global_result_set) {
              global_result = result;
              global_result_set = true;
            }
            else {
              global_result += result;
            }
          }
        }
      }
      conditional_addition_wrapper<ReductionType>
        wrapper(global_result, global_result_set);
      rpc.all_reduce(wrapper);
      return wrapper.value;
    } // end of map_reduce_vertices

   /**
    * \brief Performs a map-reduce operation on each edge in the
    * graph returning the result.
    *
    * Given a map function, map_reduce_edges() call the map function on all
    * edges in the graph. The return values are then summed together and the
    * final result returned. The map function should only read data
    * and should not make any modifications. map_reduce_edges() must be
    * called on all machines simultaneously.
    *
    * ### Basic Usage
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    *
    * To compute an absolute sum over all the edge data, we would write
    * a function which reads in each a edge, and returns the absolute
    * value of the data on the edge.
    * \code
    * float absolute_edge_datac(const graph_type::edge_type& edge) {
    *   return std::fabs(edge.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = graph.map_reduce_edges<float>(absolute_edge_data);
    * \endcode
    * will call the <code>absolute_edge_data()</code> function
    * on each edge in the graph. <code>absolute_edge_data()</code>
    * reads the value of the edge and returns the absolute result.
    * This return values are then summed together and returned.
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the mapfunction.
    *
    * The two optional arguments vset and edir can be used to restrict the
    * set of edges which are map-reduced over.
    *
    * ### Relations
    * This function similar to
    * graphlab::distributed_graph::map_reduce_edges()
    * with the difference that this does not take a context
    * and thus cannot influence engine signalling.
    * Finally transform_edges() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam EdgeMapperType The type of the map function.
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param mapfunction The map function to use. Must take
    *                   a \ref edge_type, or a reference to a
    *                   \ref edge_type as its only argument.
    *                   Returns a ReductionType which must be summable
    *                   and \ref sec_serializable .
    * \param vset A set of vertices. Combines with
    *             edir to identify the set of edges. For instance, if
    *             edir == IN_EDGES, map_reduce_edges will map over all in edges
    *             of the vertices in vset. Optional. Defaults to complete_set().
    * \param edir An edge direction. Combines with vset to identify the set
    *             of edges to map over. For instance, if
    *             edir == IN_EDGES, map_reduce_edges will map over all in edges
    *             of the vertices in vset. Optional. Defaults to IN_EDGES.
    */
    template <typename ReductionType, typename MapFunctionType>
    ReductionType map_reduce_edges(MapFunctionType mapfunction,
                                   const vertex_set& vset = complete_set(),
                                   edge_dir_type edir = IN_EDGES) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      if(!finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to run graph.map_reduce_vertices(...)"
          << "\n\tbefore calling graph.finalize()."
          << std::endl;
      }

      rpc.barrier();
      bool global_result_set = false;
      ReductionType global_result = ReductionType();
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        bool result_set = false;
        ReductionType result = ReductionType();
#ifdef _OPENMP
        #pragma omp for
#endif
        for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
          if (vset.l_contains((lvid_type)i)) {
            if (edir == IN_EDGES || edir == ALL_EDGES) {
              foreach(const local_edge_type& e, l_vertex(i).in_edges()) {
                if (!result_set) {
                  edge_type edge(e);
                  result = mapfunction(edge);
                  result_set = true;
                }
                else if (result_set){
                  edge_type edge(e);
                  const ReductionType tmp = mapfunction(edge);
                  result += tmp;
                }
              }
            }
            if (edir == OUT_EDGES || edir == ALL_EDGES) {
              foreach(const local_edge_type& e, l_vertex(i).out_edges()) {
                if (!result_set) {
                  edge_type edge(e);
                  result = mapfunction(edge);
                  result_set = true;
                }
                else if (result_set){
                  edge_type edge(e);
                  const ReductionType tmp = mapfunction(edge);
                  result += tmp;
                }
              }
            }
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          if (result_set) {
            if (!global_result_set) {
              global_result = result;
              global_result_set = true;
            }
            else {
              global_result += result;
            }
          }
        }
      }

      conditional_addition_wrapper<ReductionType>
        wrapper(global_result, global_result_set);
      rpc.all_reduce(wrapper);
      return wrapper.value;
   } // end of map_reduce_edges



   /**
    * \brief Performs a fold operation on each vertex in the
    * graph returning the result.
    *
    * Given a fold function, fold_vertices() call the fold function on all
    * vertices in the graph, passing around a aggregator variable.
    * The return values are then summed together across machines using the
    * combiner function and the final result returned. The fold function should
    * only read the vertex data and should not make any modifications.
    * fold_vertices() must be called on all machines simultaneously.
    *
    * ### Basic Usage
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    *
    * To compute an absolute sum over all the vertex data, we would write
    * a function which reads in each a vertex, and returns the absolute
    * value of the data on the vertex.
    * \code
    * void absolute_vertex_data(const graph_type::vertex_type& vertex, float& total) {
    *   total += std::fabs(vertex.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = graph.fold_vertices<float>(absolute_vertex_data);
    * \endcode
    * will call the <code>absolute_vertex_data()</code> function
    * on each vertex in the graph. <code>absolute_vertex_data()</code>
    * reads the value of the vertex and returns the absolute result.
    * This return values are then summed together and returned.
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the fold function.
    *
    * The optional argument vset can be used to restrict he set of vertices
    * map-reduced over.
    *
    * Unlike map_reduce_vertices, this function exposes to a certain extent,
    * the internals of the parallelism structure since the fold is used within
    * a thread, but across threads/machines operator+= is used. The behavior
    * of the foldfunction, or the behavior of the return type should not make
    * assumptions on the undocumented behavior of this function (such as 
    * when the fold is used, and when += is used).
    *
    * ### Relations
    * This function is similar to
    * map_reduce_vertices()
    * with the difference that this uses a fold and is hence more efficient
    * for large aggregation objects.
    * transform_vertices() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam VertexFoldType The type of the fold function.
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param foldfunction The fold function to use. Must take
    *                   a \ref vertex_type, or a reference to a
    *                   \ref vertex_type as its first argument, and a 
    *                   reference to a ReductionType in its second argument.
    * \param vset The set of vertices to fold reduce over. Optional. Defaults to
    *             complete_set()
    */
    template <typename ReductionType, typename VertexFoldType>
    ReductionType fold_vertices(VertexFoldType foldfunction,
                                      const vertex_set& vset = complete_set()) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      if(!finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to run graph.map_reduce_vertices(...) "
          << "\n\tbefore calling graph.finalize()."
          << std::endl;
      }

      rpc.barrier();
      bool global_result_set = false;
      ReductionType global_result = ReductionType();
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        ReductionType result = ReductionType();
#ifdef _OPENMP
        #pragma omp for
#endif
        for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
          if (lvid2record[i].owner == rpc.procid() &&
              vset.l_contains((lvid_type)i)) {
            const vertex_type vtx(l_vertex(i));
            foldfunction(vtx, result);
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          if (!global_result_set) {
            global_result = result;
            global_result_set = true;
          }
          else {
            global_result += result;
          }
        }
      }
      conditional_addition_wrapper<ReductionType>
        wrapper(global_result, global_result_set);
      rpc.all_reduce(wrapper);
      return wrapper.value;
    } 



   /**
    * \brief Performs a fold operation on each edge in the
    * graph returning the result.
    *
    * Given a fold function, fold_edges() call the fold function on all
    * edges in the graph passing an aggregator. 
    * The return values are then summed together across machines and 
    * final result returned. The fold function should only read data
    * and should not make any modifications. fold_edges() must be
    * called on all machines simultaneously.
    *
    * ### Basic Usage
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    *
    * To compute an absolute sum over all the edge data, we would write
    * a function which reads in each a edge, and returns the absolute
    * value of the data on the edge.
    * \code
    * void absolute_edge_data(const graph_type::edge_type& edge, float& acc) {
    *   acc += std::fabs(edge.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = graph.fold_edges<float>(absolute_edge_data);
    * \endcode
    * will call the <code>absolute_edge_data()</code> function
    * on each edge in the graph. <code>absolute_edge_data()</code>
    * reads the value of the edge and returns the absolute result.
    * This return values are then summed together and returned.
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the foldfunction.
    *
    * The two optional arguments vset and edir can be used to restrict the
    * set of edges which are map-reduced over.
    *
    * ### Relations
    * This function similar to
    * graphlab::distributed_graph::map_reduce_edges()
    * with the difference that this uses a fold and is hence more efficient
    * for large aggregation objects.
    * Finally transform_edges() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam EdgeFoldType The type of the Fold function.
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param fold function The map function to use. Must take
    *                   a \ref edge_type, or a reference to a
    *                   \ref edge_type as its first argument, and
    *                   a reference to a ReductionType in its second argument.
    * \param vset A set of vertices. Combines with
    *             edir to identify the set of edges. For instance, if
    *             edir == IN_EDGES, map_reduce_edges will map over all in edges
    *             of the vertices in vset. Optional. Defaults to complete_set().
    * \param edir An edge direction. Combines with vset to identify the set
    *             of edges to map over. For instance, if
    *             edir == IN_EDGES, map_reduce_edges will map over all in edges
    *             of the vertices in vset. Optional. Defaults to IN_EDGES.
    */
    template <typename ReductionType, typename FoldFunctionType>
    ReductionType fold_edges(FoldFunctionType foldfunction,
                                   const vertex_set& vset = complete_set(),
                                   edge_dir_type edir = IN_EDGES) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      if(!finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to run graph.map_reduce_vertices(...)"
          << "\n\tbefore calling graph.finalize()."
          << std::endl;
      }

      rpc.barrier();
      bool global_result_set = false;
      ReductionType global_result = ReductionType();
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        ReductionType result = ReductionType();
#ifdef _OPENMP
        #pragma omp for
#endif
        for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
          if (vset.l_contains((lvid_type)i)) {
            if (edir == IN_EDGES || edir == ALL_EDGES) {
              foreach(const local_edge_type& e, l_vertex(i).in_edges()) {
                  edge_type edge(e);
                  foldfunction(edge, result);
              }
            }
            if (edir == OUT_EDGES || edir == ALL_EDGES) {
              foreach(const local_edge_type& e, l_vertex(i).out_edges()) {
                edge_type edge(e);
                foldfunction(edge, result);
              }
            }
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          if (!global_result_set) {
            global_result = result;
            global_result_set = true;
          }
          else {
            global_result += result;
          }
        }
      }

      conditional_addition_wrapper<ReductionType>
        wrapper(global_result, global_result_set);
      rpc.all_reduce(wrapper);
      return wrapper.value;
   } // end of map_reduce_edges



    /**
     * \brief Performs a transformation operation on each vertex in the graph.
     *
     * Given a mapfunction, transform_vertices() calls mapfunction on
     * every vertex in graph. The map function may make modifications
     * to the data on the vertex. transform_vertices() must be called by all
     * machines simultaneously.
     *
     * The optional vset argument may be used to restrict the set of vertices
     * operated upon.
     *
     * ### Basic Usage
     * For instance, if the graph has integer vertex data, and integer edge
     * data:
     * \code
     *   typedef graphlab::distributed_graph<size_t, size_t> graph_type;
     * \endcode
     *
     * To set each vertex value to be the number of out-going edges,
     * we may write the following function:
     * \code
     * void set_vertex_value(graph_type::vertex_type& vertex)i {
     *   vertex.data() = vertex.num_out_edges();
     * }
     * \endcode
     *
     * Calling transform_vertices():
     * \code
     *   graph.transform_vertices(set_vertex_value);
     * \endcode
     * will run the <code>set_vertex_value()</code> function
     * on each vertex in the graph, setting its new value.
     *
     * ### Relations
     * map_reduce_vertices() provide similar signalling functionality,
     * but should not make modifications to graph data.
     * graphlab::iengine::transform_vertices() provide
     * the same graph modification capabilities, but with a context
     * and thus can perform signalling.
     *
     * \tparam VertexMapperType The type of the map function.
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \param mapfunction The map function to use. Must take an
     *                   \ref icontext_type& as its first argument, and
     *                   a \ref vertex_type, or a reference to a
     *                   \ref vertex_type as its second argument.
     *                   Returns void.
     * \param vset The set of vertices to transform. Optional. Defaults to
     *             complete_set()
     */
    template <typename TransformType>
    void transform_vertices(TransformType transform_functor,
                            const vertex_set vset = complete_set()) {
      if(!finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to call graph.transform_vertices(...)"
          << "\n\tbefore finalizing the graph."
          << std::endl;
      }

      rpc.barrier();
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
        if (lvid2record[i].owner == rpc.procid() &&
            vset.l_contains((lvid_type)i)) {
          vertex_type vtx(l_vertex(i));
          transform_functor(vtx);
        }
      }
      rpc.barrier();
      synchronize();
    }

    /**
     * \brief Performs a transformation operation on each edge in the graph.
     *
     * Given a mapfunction, transform_edges() calls mapfunction on
     * every edge in graph. The map function may make modifications
     * to the data on the edge. transform_edges() must be called on
     * all machines simultaneously.
     *
     * ### Basic Usage
     * For instance, if the graph has integer vertex data, and integer edge
     * data:
     * \code
     *   typedef graphlab::distributed_graph<size_t, size_t> graph_type;
     * \endcode
     *
     * To set each edge value to be the number of out-going edges
     * of the target vertex, we may write the following:
     * \code
     * void set_edge_value(graph_type::edge_type& edge) {
     *   edge.data() = edge.target().num_out_edges();
     * }
     * \endcode
     *
     * Calling transform_edges():
     * \code
     *   graph.transform_edges(set_edge_value);
     * \endcode
     * will run the <code>set_edge_value()</code> function
     * on each edge in the graph, setting its new value.
     *
     * The two optional arguments vset and edir may be used to restrict
     * the set of edges operated upon.
     *
     * ### Relations
     * map_reduce_edges() provide similar signalling functionality,
     * but should not make modifications to graph data.
     * graphlab::iengine::transform_edges() provide
     * the same graph modification capabilities, but with a context
     * and thus can perform signalling.
     *
     * \tparam EdgeMapperType The type of the map function.
     *                          Not generally needed.
     *                          Can be inferred by the compiler.
     * \param mapfunction The map function to use. Must take an
     *                   \ref icontext_type& as its first argument, and
     *                   a \ref edge_type, or a reference to a
     *                   \ref edge_type as its second argument.
     *                   Returns void.
     * \param vset A set of vertices. Combines with
     *             edir to identify the set of edges. For instance, if
     *             edir == IN_EDGES, map_reduce_edges will map over all in edges
     *             of the vertices in vset. Optional. Defaults to complete_set().
     * \param edir An edge direction. Combines with vset to identify the set
     *             of edges to map over. For instance, if
     *             edir == IN_EDGES, map_reduce_edges will map over all in edges
     *             of the vertices in vset. Optional. Defaults to IN_EDGES.
     */
    template <typename TransformType>
    void transform_edges(TransformType transform_functor,
                         const vertex_set& vset = complete_set(),
                         edge_dir_type edir = IN_EDGES) {
      if(!finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to call graph.transform_edges(...)"
          << "\n\tbefore finalizing the graph."
          << std::endl;
      }
      rpc.barrier();
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
        if (vset.l_contains((lvid_type)i)) {
          if (edir == IN_EDGES || edir == ALL_EDGES) {
            foreach(const local_edge_type& e, l_vertex(i).in_edges()) {
              edge_type edge(e);
              transform_functor(edge);
            }
          }
          if (edir == OUT_EDGES || edir == ALL_EDGES) {
            foreach(const local_edge_type& e, l_vertex(i).out_edges()) {
              edge_type edge(e);
              transform_functor(edge);
            }
          }
        }
      }
      rpc.barrier();
    }

    // disable documentation for parallel_for stuff. These are difficult
    // to use properly by the user
    /// \cond GRAPHLAB_INTERNAL
    /**
     * \internal
     * parallel_for_vertices will partition the set of vertices among the
     * vector of accfunctions. Each accfunction is then executed sequentially
     * on the set of vertices it was assigned.
     *
      * \param accfunction must be a void function which takes a single
      * vertex_type argument. It may be a functor and contain state.
      * The function need not be reentrant as it is only called sequentially
     */
    template <typename VertexFunctorType>
    void parallel_for_vertices(std::vector<VertexFunctorType>& accfunction) {
      ASSERT_TRUE(finalized);
      rpc.barrier();
      int numaccfunctions = (int)accfunction.size();
      ASSERT_GE(numaccfunctions, 1);
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)accfunction.size(); ++i) {
        for (int j = i;j < (int)local_graph.num_vertices(); j+=numaccfunctions) {
          if (lvid2record[j].owner == rpc.procid()) {
            accfunction[i](vertex_type(l_vertex(j)));
          }
        }
      }
      rpc.barrier();
    }


    /**
     * \internal
     * parallel_for_edges will partition the set of edges among the
     * vector of accfunctions. Each accfunction is then executed sequentially
     * on the set of edges it was assigned.
     *
     * \param accfunction must be a void function which takes a single
     * edge_type argument. It may be a functor and contain state.
     * The function need not be reentrant as it is only called sequentially
     */
    template <typename EdgeFunctorType>
    void parallel_for_edges(std::vector<EdgeFunctorType>& accfunction) {
      ASSERT_TRUE(finalized);
      rpc.barrier();
      int numaccfunctions = (int)accfunction.size();
      ASSERT_GE(numaccfunctions, 1);
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)accfunction.size(); ++i) {
        for (int j = i;j < (int)local_graph.num_vertices(); j+=numaccfunctions) {
          foreach(const local_edge_type& e, l_vertex(j).in_edges()) {
            accfunction[i](edge_type(e));
          }
        }
      }
      rpc.barrier();
    }



    /** \brief Load the graph from an archive */
    void load(iarchive& arc) {
      // read the vertices
      arc >> stats 
          >> vid2lvid
          >> lvid2record
          >> local_graph;
      finalized = true;
      // check the graph condition
    } // end of load


    distributed_graph& operator=(const distributed_graph& other) {
      ASSERT_TRUE(other.finalized);
      clear();
      stats = other.stats;
      vid2lvid = other.vid2lvid;
      lvid2record = other.lvid2record;
      local_graph = other.local_graph;
      finalized = true;
      return *this;
    }

    /** \brief Save the graph to an archive */
    void save(oarchive& arc) const {
      if(!finalized) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to save a graph before calling graph.finalize()."
          << std::endl;
      }
      // Write the number of edges and vertices
      arc << stats 
          << vid2lvid
          << lvid2record
          << local_graph;
    } // end of save

    /// \endcond

    /// \brief Clears and resets the graph, releasing all memory used.
    void clear () {
      foreach (vertex_record& vrec, lvid2record)
        vrec.clear();
      lvid2record.clear();
      vid2lvid.clear();
      local_graph.clear();
      finalized=false;
      stats.clear();
    }


    /** \brief Load a distributed graph from a native binary format
     * previously saved with save_binary(). This function must be called
     *  simultaneously on all machines.
     *
     * This function loads a sequence of files numbered
     * \li [prefix].0.gz
     * \li [prefix].1.gz
     * \li [prefix].2.gz
     * \li etc.
     *
     * These files must be previously saved using save_binary(), and
     * must be saved <b>using the same number of machines</b>.
     * This function uses the graphlab serialization system, so
     * the user must ensure that the vertex data and edge data
     * serialization formats have not changed since the graph was saved.
     *
     * A graph loaded using load_binary() is already finalized and
     * structure modifications are not permitted after loading.
     *
     * Return true on success and false on failure if the file cannot be loaded.
     */
    bool load_binary(const std::string& prefix) {
      rpc.full_barrier();
      std::string fname = prefix + tostr(rpc.procid()) + ".bin";

      logstream(LOG_INFO) << "Load graph from " << fname << std::endl;
      if(boost::starts_with(fname, "hdfs://")) {
        graphlab::hdfs hdfs;
        graphlab::hdfs::fstream in_file(hdfs, fname);
        boost::iostreams::filtering_stream<boost::iostreams::input> fin;
        fin.push(boost::iostreams::gzip_decompressor());
        fin.push(in_file);

        if(!fin.good()) {
          logstream(LOG_ERROR) << "\n\tError opening file: " << fname << std::endl;
          return false;
        }
        iarchive iarc(fin);
        iarc >> *this;
        fin.pop();
        fin.pop();
        in_file.close();
      } else {
        std::ifstream in_file(fname.c_str(),
                              std::ios_base::in | std::ios_base::binary);
        if(!in_file.good()) {
          logstream(LOG_ERROR) << "\n\tError opening file: " << fname << std::endl;
          return false;
        }
        boost::iostreams::filtering_stream<boost::iostreams::input> fin;
        fin.push(boost::iostreams::gzip_decompressor());
        fin.push(in_file);
        iarchive iarc(fin);
        iarc >> *this;
        fin.pop();
        fin.pop();
        in_file.close();
      }
      logstream(LOG_INFO) << "Finish loading graph from " << fname << std::endl;
      rpc.full_barrier();
      return true;
    } // end of load


    /** \brief Saves a distributed graph to a native binary format
     * which can be loaded with load_binary(). This function must be called
     *  simultaneously on all machines.
     *
     * This function saves a sequence of files numbered
     * \li [prefix].0.gz
     * \li [prefix].1.gz
     * \li [prefix].2.gz
     * \li etc.
     *
     * This files can be loaded with load_binary() using the <b> same number
     * of machines</b>.
     * This function uses the graphlab serialization system, so
     * the vertex data and edge data serialization formats must not
     * change between the use of save_binary() and load_binary().
     *
     * If the graph is not alreasy finalized before save_binary() is called,
     * this function will finalize the graph.
     *
     * Returns true on success, and false if the graph cannot be loaded from
     * the specified file.
     */
    bool save_binary(const std::string& prefix) {
      rpc.full_barrier();
      finalize();
      timer savetime;  savetime.start();
      std::string fname = prefix + tostr(rpc.procid()) + ".bin";
      logstream(LOG_INFO) << "Save graph to " << fname << std::endl;
      if(boost::starts_with(fname, "hdfs://")) {
        graphlab::hdfs hdfs;
        graphlab::hdfs::fstream out_file(hdfs, fname, true);
        boost::iostreams::filtering_stream<boost::iostreams::output> fout;
        fout.push(boost::iostreams::gzip_compressor());
        fout.push(out_file);
        if (!fout.good()) {
          logstream(LOG_ERROR) << "\n\tError opening file: " << fname << std::endl;
          return false;
        }
        oarchive oarc(fout);
        oarc << *this;
        fout.pop();
        fout.pop();
        out_file.close();
      } else {
        std::ofstream out_file(fname.c_str(),
                               std::ios_base::out | std::ios_base::binary);
        if (!out_file.good()) {
          logstream(LOG_ERROR) << "\n\tError opening file: " << fname << std::endl;
          return false;
        }
        boost::iostreams::filtering_stream<boost::iostreams::output> fout;
        fout.push(boost::iostreams::gzip_compressor());
        fout.push(out_file);
        oarchive oarc(fout);
        oarc << *this;
        fout.pop();
        fout.pop();
        out_file.close();
      }
      logstream(LOG_INFO) << "Finish saving graph to " << fname << std::endl
                          << "Finished saving binary graph: "
                          << savetime.current_time() << std::endl;
      rpc.full_barrier();
      return true;
    } // end of save


    /**
     * \brief Saves the graph to the filesystem using a provided Writer object.
     * Like \ref save(const std::string& prefix, writer writer, bool gzip, bool save_vertex, bool save_edge, size_t files_per_machine) "save()"
     * but only saves to local filesystem.
     */
    template<typename Writer>
    void save_to_posixfs(const std::string& prefix, Writer writer,
                         bool gzip = true,
                         bool save_vertex = true,
                         bool save_edge = true,
                         size_t files_per_machine = 4) {
      typedef boost::function<void(vertex_type)> vertex_function_type;
      typedef boost::function<void(edge_type)> edge_function_type;
      typedef std::ofstream base_fstream_type;
      typedef boost::iostreams::filtering_stream<boost::iostreams::output>
        boost_fstream_type;
      rpc.full_barrier();
      finalize();
      // figure out the filenames
      std::vector<std::string> graph_files;
      std::vector<base_fstream_type*> outstreams;
      std::vector<boost_fstream_type*> booststreams;
      graph_files.resize(files_per_machine);
      for(size_t i = 0; i < files_per_machine; ++i) {
        graph_files[i] = prefix + "_" + tostr(1 + i + rpc.procid() * files_per_machine)
          + "_of_" + tostr(rpc.numprocs() * files_per_machine);
        if (gzip) graph_files[i] += ".gz";
      }

      // create the vector of callbacks
      std::vector<vertex_function_type> vertex_callbacks(graph_files.size());
      std::vector<edge_function_type> edge_callbacks(graph_files.size());

      for(size_t i = 0; i < graph_files.size(); ++i) {
        logstream(LOG_INFO) << "Saving to file: " << graph_files[i] << std::endl;
        // open the stream
        base_fstream_type* out_file =
          new base_fstream_type(graph_files[i].c_str(),
                                std::ios_base::out | std::ios_base::binary);
        // attach gzip if the file is gzip
        boost_fstream_type* fout = new boost_fstream_type;
        // Using gzip filter
        if (gzip) fout->push(boost::iostreams::gzip_compressor());
        fout->push(*out_file);

        outstreams.push_back(out_file);
        booststreams.push_back(fout);
        // construct the callback for the parallel for
        typedef distributed_graph<vertex_data_type, edge_data_type> graph_type;
        vertex_callbacks[i] =
          boost::bind(&graph_type::template save_vertex_to_stream<boost_fstream_type, Writer>,
                      this, _1, boost::ref(*fout), boost::ref(writer));
        edge_callbacks[i] =
          boost::bind(&graph_type::template save_edge_to_stream<boost_fstream_type, Writer>,
                      this, _1, boost::ref(*fout), boost::ref(writer));
      }

      if (save_vertex) parallel_for_vertices(vertex_callbacks);
      if (save_edge) parallel_for_edges(edge_callbacks);

      // cleanup
      for(size_t i = 0; i < graph_files.size(); ++i) {
        booststreams[i]->pop();
        if (gzip) booststreams[i]->pop();
        delete booststreams[i];
        delete outstreams[i];
      }
      vertex_callbacks.clear();
      edge_callbacks.clear();
      outstreams.clear();
      booststreams.clear();
      rpc.full_barrier();
    } // end of save to posixfs



    /**
     * \brief Saves the graph to HDFS using a provided Writer object.
     * Like \ref save(const std::string& prefix, writer writer, bool gzip, bool save_vertex, bool save_edge, size_t files_per_machine) "save()"
     * but only saves to HDFS.
     */
    template<typename Writer>
    void save_to_hdfs(const std::string& prefix, Writer writer,
                      bool gzip = true,
                      bool save_vertex = true,
                      bool save_edge = true,
                      size_t files_per_machine = 4) {
      typedef boost::function<void(vertex_type)> vertex_function_type;
      typedef boost::function<void(edge_type)> edge_function_type;
      typedef graphlab::hdfs::fstream base_fstream_type;
      typedef boost::iostreams::filtering_stream<boost::iostreams::output>
        boost_fstream_type;
      rpc.full_barrier();
      finalize();
      // figure out the filenames
      std::vector<std::string> graph_files;
      std::vector<base_fstream_type*> outstreams;
      std::vector<boost_fstream_type*> booststreams;
      graph_files.resize(files_per_machine);
      for(size_t i = 0; i < files_per_machine; ++i) {
        graph_files[i] = prefix + "_" + tostr(1 + i + rpc.procid() * files_per_machine)
          + "_of_" + tostr(rpc.numprocs() * files_per_machine);
        if (gzip) graph_files[i] += ".gz";
      }

      if(!hdfs::has_hadoop()) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to save a graph to HDFS but GraphLab"
          << "\n\twas built without HDFS."
          << std::endl;
      }
      hdfs& hdfs = hdfs::get_hdfs();

      // create the vector of callbacks

      std::vector<vertex_function_type> vertex_callbacks(graph_files.size());
      std::vector<edge_function_type> edge_callbacks(graph_files.size());

      for(size_t i = 0; i < graph_files.size(); ++i) {
        logstream(LOG_INFO) << "Saving to file: " << graph_files[i] << std::endl;
        // open the stream
        base_fstream_type* out_file = new base_fstream_type(hdfs,
                                                            graph_files[i],
                                                            true);
        // attach gzip if the file is gzip
        boost_fstream_type* fout = new boost_fstream_type;
        // Using gzip filter
        if (gzip) fout->push(boost::iostreams::gzip_compressor());
        fout->push(*out_file);

        outstreams.push_back(out_file);
        booststreams.push_back(fout);
        // construct the callback for the parallel for
        typedef distributed_graph<vertex_data_type, edge_data_type> graph_type;
        vertex_callbacks[i] =
          boost::bind(&graph_type::template save_vertex_to_stream<boost_fstream_type, Writer>,
                      this, _1, boost::ref(*fout), writer);
        edge_callbacks[i] =
          boost::bind(&graph_type::template save_edge_to_stream<boost_fstream_type, Writer>,
                      this, _1, boost::ref(*fout), writer);
      }

      if (save_vertex) parallel_for_vertices(vertex_callbacks);
      if (save_edge) parallel_for_edges(edge_callbacks);

      // cleanup
      for(size_t i = 0; i < graph_files.size(); ++i) {
        booststreams[i]->pop();
        if (gzip) booststreams[i]->pop();
        delete booststreams[i];
        delete outstreams[i];
      }
      vertex_callbacks.clear();
      edge_callbacks.clear();
      outstreams.clear();
      booststreams.clear();
      rpc.full_barrier();
    } // end of save to hdfs



    /**
     * \brief Saves the graph to the filesystem or to HDFS using
     *  a user provided Writer object. This function should be called on
     *  all machines simultaneously.
     *
     * This function saves the current graph to disk using a user provided
     * Writer object. The writer object must implement two functions:
     * \code
     * std::string Writer::save_vertex(graph_type::vertex_type v);
     * std::string Writer::save_edge(graph_type::edge_type e);
     * \endcode
     *
     * The <code>save_vertex()</code> function will be called on each vertex
     * on the graph, and the output of the function is written to file.
     * Similarly, the <code>save_edge()</code> function is called on each edge
     * in the graph and the output written to file.
     *
     * For instance, a simple Writer object which saves a file containing
     * a list of edges will be:
     * \code
     * struct edge_list_writer {
     *   std::string save_vertex(vertex_type) { return ""; }
     *   std::string save_edge(edge_type e) {
     *     char c[128];
     *     sprintf(c, "%u\t%u\n", e.source().id(), e.target().id());
     *     return c;
     *   }
     * };
     * \endcode
     * The save_edge() function is called on each edge in the graph. It then
     * constructs a string containing "[source] \\t [target] \\n" and returns
     * the string.
     *
     * This can also be used to data in human readable format. For instance,
     * if the vertex data type is a floating point number (say a PageRank
     * value), to save a list of vertices and their corresponding PageRanks,
     * the following writer could be implemented:
     * \code
     * struct pagerank_writer {
     *   std::string save_vertex(vertex_type v) {
     *     char c[128];
     *     sprintf(c, "%u\t%f\n", v.id(), v.data());
     *     return c;
     *   }
     *   std::string save_edge(edge_type) {}
     * };
     * \endcode
     * \note Note that these is not an example a reliable parser since sprintf
     *  may break if the size of vertex_id_type changes
     *
     * The output files will be written in
     * \li [prefix]_1_of_16.gz
     * \li [prefix]_2_of_16.gz
     * \li [prefix].3_of_16.gz
     * \li etc.
     *
     * To accelerate the saving process, multiple files are be written
     * per machine in parallel. If the gzip option is not set, the ".gz" suffix
     * is not added.
     *
     * For instance, if there are 4 machines, running:
     * \code
     *   save("test_graph", pagerank_writer);
     * \endcode
     * Will create the files
     * \li test_graph_1_of_16.gz
     * \li test_graph_2_of_16.gz
     * \li ...
     * \li test_graph_16_of_16.gz
     *
     * If HDFS support is compiled in, this function can save to HDFS by
     * adding "hdfs://" to the prefix.
     *
     * For instance, if there are 4 machines, running:
     * \code
     *   save("hdfs:///hdfs_server/data/test_graph", pagerank_writer);
     * \endcode
     * Will create on the HDFS server, the files
     * \li /data/test_graph_1_of_16.gz
     * \li /data/test_graph_2_of_16.gz
     * \li ...
     * \li /data/test_graph_16_of_16.gz
     *
     * \tparam Writer The writer object type. This is generally inferred by the
     *                compiler and need not be specified.
     *
     * \param prefix The file prefix to save the output graph files. The output
     *               files will be numbered [prefix].0 , [prefix].1 , etc.
     *               If prefix begins with "hdfs://", the output is written to
     *               HDFS
     * \param writer The writer object to use.
     * \param gzip If gzip compression should be used. If set, all files will be
     *             appended with the .gz suffix. Defaults to true.
     * \param save_vertex If vertices should be saved. Defaults to true.
     * \param save_edges If edges should be saved. Defaults to true.
     * \param files_per_machine Number of files to write simultaneously in
     *                          parallel per machine. Defaults to 4.
     */
    template<typename Writer>
    void save(const std::string& prefix, Writer writer,
              bool gzip = true, bool save_vertex = true, bool save_edge = true,
              size_t files_per_machine = 4) {
      if(boost::starts_with(prefix, "hdfs://")) {
        save_to_hdfs(prefix, writer, gzip, save_vertex, save_edge, files_per_machine);
      } else {
        save_to_posixfs(prefix, writer, gzip, save_vertex, save_edge, files_per_machine);
      }
    } // end of save



    /**
     * \brief Saves the graph in the specified format. This function should be
     * called on all machines simultaneously.
     *
     * The output files will be written in
     * \li [prefix].0.gz
     * \li [prefix].1.gz
     * \li [prefix].2.gz
     * \li etc.
     *
     * To accelerate the saving process, multiple files are be written
     * per machine in parallel. If the gzip option is not set, the ".gz" suffix
     * is not added.
     *
     * For instance, if there are 4 machines, running:
     * \code
     *   save_format("test_graph", "tsv");
     * \endcode
     * Will create the files
     * \li test_graph_0.gz
     * \li test_graph_1.gz
     * \li ...
     * \li test_graph_15.gz
     *
     * The supported formats are described in \ref graph_formats.
     *
     * \param prefix The file prefix to save the output graph files. The output
     *               files will be numbered [prefix].0 , [prefix].1 , etc.
     *               If prefix begins with "hdfs://", the output is written to
     *               HDFS.
     * \param format The file format to save in.
     *               Either "tsv", "snap", "graphjrl" or "bin".
     * \param gzip If gzip compression should be used. If set, all files will be
     *             appended with the .gz suffix. Defaults to true. Ignored
     *             if format == "bin".
     * \param files_per_machine Number of files to write simultaneously in
     *                          parallel per machine. Defaults to 4. Ignored if
     *                          format == "bin".
     */
    void save_format(const std::string& prefix, const std::string& format,
                        bool gzip = true, size_t files_per_machine = 4) {
      if (format == "snap" || format == "tsv") {
        save(prefix, builtin_parsers::tsv_writer<distributed_graph>(),
             gzip, false, true, files_per_machine);
      } else if (format == "graphjrl") {
         save(prefix, builtin_parsers::graphjrl_writer<distributed_graph>(),
             gzip, true, true, files_per_machine);
      } else if (format == "bin") {
         save_binary(prefix);
      } else if (format == "bintsv4") {
         save_direct(prefix, gzip, &graph_type::save_bintsv4_to_stream);
      } else {
        logstream(LOG_FATAL)
          << "Unrecognized Format \"" << format << "\"!" << std::endl;
        return;
      }
    } // end of save structure




    /**
     *  \brief Load a graph from a collection of files in stored on
     *  the filesystem using the user defined line parser. Like
     *  \ref load(const std::string& path, line_parser_type line_parser)
     *  but only loads from the filesystem.
     */
    void load_from_posixfs(std::string prefix,
                           line_parser_type line_parser) {
      std::string directory_name; std::string original_path(prefix);
      boost::filesystem::path path(prefix);
      std::string search_prefix;
      if (boost::filesystem::is_directory(path)) {
        // if this is a directory
        // force a "/" at the end of the path
        // make sure to check that the path is non-empty. (you do not
        // want to make the empty path "" the root path "/" )
        directory_name = path.native();
      }
      else {
        directory_name = path.parent_path().native();
        search_prefix = path.filename().native();
        directory_name = (directory_name.empty() ? "." : directory_name);
      }
      std::vector<std::string> graph_files;
      fs_util::list_files_with_prefix(directory_name, search_prefix, graph_files);
      if (graph_files.size() == 0) {
        logstream(LOG_WARNING) << "No files found matching " << original_path << std::endl;
      }

#ifdef _OPENMP
#pragma omp parallel for
#endif
      for(size_t i = 0; i < graph_files.size(); ++i) {
        if ((parallel_ingress && (i % rpc.numprocs() == rpc.procid()))
            || (!parallel_ingress && (rpc.procid() == 0))) {
          logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
          // is it a gzip file ?
          const bool gzip = boost::ends_with(graph_files[i], ".gz");
          // open the stream
          std::ifstream in_file(graph_files[i].c_str(),
                                std::ios_base::in | std::ios_base::binary);
          // attach gzip if the file is gzip
          boost::iostreams::filtering_stream<boost::iostreams::input> fin;
          // Using gzip filter
          if (gzip) fin.push(boost::iostreams::gzip_decompressor());
          fin.push(in_file);
          const bool success = load_from_stream(graph_files[i], fin, line_parser);
          if(!success) {
            logstream(LOG_FATAL)
              << "\n\tError parsing file: " << graph_files[i] << std::endl;
          }
          fin.pop();
          if (gzip) fin.pop();
        }
      }
      rpc.full_barrier();
    } // end of load from posixfs

    /**
     *  \brief Load a graph from a collection of files in stored on
     *  the HDFS using the user defined line parser. Like
     *  \ref load(const std::string& path, line_parser_type line_parser)
     *  but only loads from HDFS.
     */
    void load_from_hdfs(std::string prefix, line_parser_type line_parser) {
      // force a "/" at the end of the path
      // make sure to check that the path is non-empty. (you do not
      // want to make the empty path "" the root path "/" )
      std::string path = prefix;
      if (path.length() > 0 && path[path.length() - 1] != '/') path = path + "/";
      if(!hdfs::has_hadoop()) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to load a graph from HDFS but GraphLab"
          << "\n\twas built without HDFS."
          << std::endl;
      }
      hdfs& hdfs = hdfs::get_hdfs();
      std::vector<std::string> graph_files;
      graph_files = hdfs.list_files(path);
      if (graph_files.size() == 0) {
        logstream(LOG_WARNING) << "No files found matching " << prefix << std::endl;
      }
#ifdef _OPENMP
#pragma omp parallel for
#endif
      for(size_t i = 0; i < graph_files.size(); ++i) {
        if ((parallel_ingress && (i % rpc.numprocs() == rpc.procid())) ||
            (!parallel_ingress && (rpc.procid() == 0))) {
          logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
          // is it a gzip file ?
          const bool gzip = boost::ends_with(graph_files[i], ".gz");
          // open the stream
          graphlab::hdfs::fstream in_file(hdfs, graph_files[i]);
          boost::iostreams::filtering_stream<boost::iostreams::input> fin;
          if(gzip) fin.push(boost::iostreams::gzip_decompressor());
          fin.push(in_file);
          const bool success = load_from_stream(graph_files[i], fin, line_parser);
          if(!success) {
            logstream(LOG_FATAL)
              << "\n\tError parsing file: " << graph_files[i] << std::endl;
          }
          fin.pop();
          if (gzip) fin.pop();
        }
      }
      rpc.full_barrier();
    } // end of load from hdfs


    /**
     *  \brief Load a the graph from a given path using a user defined
     *  line parser. This function should be called on all machines
     *  simultaneously.
     *
     *  This functions loads all files in the filesystem or on HDFS matching
     *  the pattern "[prefix]*".
     *
     *  Examples:
     *
     *  <b> prefix = "webgraph.txt" </b>
     *
     *  will load the file webgraph.txt if such a file exists. It will also
     *  load all files in the current directory which begins with "webgraph.txt".
     *  For instance, webgraph.txt.0, webgraph.txt.1, etc.
     *
     *  <b>prefix = "graph/data"</b>
     *
     *  will load all files in the "graph" directory which begin with "data"
     *
     *  <b> prefix = "hdfs:///hdfs_server/graph/data" </b>
     *
     *  will load all files from the HDFS server in the "/graph/" directory
     *  which begin with "data".
     *
     *  If files have the ".gz" suffix, it is automatically decompressed.
     *
     *  The line_parser is a user defined function matching the following
     *  prototype:
     *
     *  \code
     *  bool parser(graph_type& graph,
     *              const std::string& filename,
     *              const std::string& line);
     *  \endcode
     *
     *  The load() function will call the parser one line at a time, and the
     *  paser function should process the line and call add_vertex / add_edge
     *  functions in the graph. It should return true on success, and false
     *  on failure. Since the parsing may be parallelized,
     *  the parser should treat each line independently
     *  and not depend on a sequential pass through a file.
     *
     *  For instance, if the graph is in a simple edge list format, a parser
     *  could be:
     *  \code
     *  bool edge_list_parser(graph_type& graph,
     *                        const std::string& filename,
     *                        const std::string& line) {
     *    if (line.empty()) return true;
     *    vertex_id_type source, target;
     *    if (sscanf(line.c_str(), "%u %u", source, target) < 2) {
     *      // parsed less than 2 objects, failure.
     *      return false;
     *    }
     *    else {
     *      graph.add_edge(source, target);
     *      return true;
     *    }
     *  }
     *  \endcode
     *  \note Note that this is not an example a reliable parser since sscanf
     *  may break if the size of vertex_id_type changes
     *
     *  \param prefix The file prefix to read from. All files matching
     *                the pattern "[prefix]*" are loaded. If prefix begins with
     *                "hdfs://" the files are read from hdfs.
     *  \param line_parser A user defined parsing function
     */
    void load(std::string prefix, line_parser_type line_parser) {
      rpc.full_barrier();
      if (prefix.length() == 0) return;
      if(boost::starts_with(prefix, "hdfs://")) {
        load_from_hdfs(prefix, line_parser);
      } else {
        load_from_posixfs(prefix, line_parser);
      }
      rpc.full_barrier();
    } // end of load

    /**
     * \brief Constructs a synthetic power law graph. Must be called on
     * all machines simultaneously.
     *
     * This function constructs a synthetic out-degree power law of "nverts"
     * vertices with a particular alpha parameter.
     * In other words, the probability that a vertex has out-degree \f$d\f$,
     * is given by:
     *
     * \f[ P(d) \propto d^{-\alpha} \f]
     *
     * By default, the out-degree distribution of each vertex
     * will have power-law distribution, but the in-degrees will be nearly
     * uniform. This can be reversed by setting the second argument "in_degree"
     * to true.
     *
     * \param nverts Number of vertices to generate
     * \param in_degree If set to true, the graph will have power-law in-degree.
     *                  Defaults to false.
     * \param alpha The alpha parameter in the power law distribution. Defaults
     *              to 2.1
     * \param truncate Limits the maximum degree of any vertex. (thus generating
     *                 a truncated power-law distribution). Necessary
     *                 for large number of vertices (hundreds of millions)
     *                 since this function allocates a PDF vector of
     *                 "nverts" to sample from.
     */
    void load_synthetic_powerlaw(size_t nverts, bool in_degree = false,
                                 double alpha = 2.1, size_t truncate = (size_t)(-1)) {
      rpc.full_barrier();
      std::vector<double> prob(std::min(nverts, truncate), 0);
      logstream(LOG_INFO) << "constructing pdf" << std::endl;
      for(size_t i = 0; i < prob.size(); ++i)
        prob[i] = std::pow(double(i+1), -alpha);
      logstream(LOG_INFO) << "constructing cdf" << std::endl;
      random::pdf2cdf(prob);
      logstream(LOG_INFO) << "Building graph" << std::endl;
      size_t target_index = rpc.procid();
      size_t addedvtx = 0;

      // A large prime number
      const size_t HASH_OFFSET = 2654435761;
      for(size_t source = rpc.procid(); source < nverts;
          source += rpc.numprocs()) {
        const size_t out_degree = random::multinomial_cdf(prob) + 1;
        for(size_t i = 0; i < out_degree; ++i) {
          target_index = (target_index + HASH_OFFSET)  % nverts;
          while (source == target_index) {
            target_index = (target_index + HASH_OFFSET)  % nverts;
          }
          if(in_degree) add_edge(target_index, source);
          else add_edge(source, target_index);
        }
        ++addedvtx;
        if (addedvtx % 10000000 == 0) {
          logstream(LOG_EMPH) << addedvtx << " inserted\n";
        }
      }
      rpc.full_barrier();
    } // end of load random powerlaw


    /**
     *  \brief load a graph with a standard format. Must be called on all
     *  machines simultaneously.
     *
     *  The supported graph formats are described in \ref graph_formats.
     */
    void load_format(const std::string& path, const std::string& format) {
      line_parser_type line_parser;
      if (format == "snap") {
        line_parser = builtin_parsers::snap_parser<distributed_graph>;
        load(path, line_parser);
      } else if (format == "adj") {
        line_parser = builtin_parsers::adj_parser<distributed_graph>;
        load(path, line_parser);
      } else if (format == "tsv") {
        line_parser = builtin_parsers::tsv_parser<distributed_graph>;
        load(path, line_parser);
      } else if (format == "csv") {
        line_parser = builtin_parsers::csv_parser<distributed_graph>;
        load(path, line_parser);
      } else if (format == "graphjrl") {
        line_parser = builtin_parsers::graphjrl_parser<distributed_graph>;
        load(path, line_parser);
      } else if (format == "bintsv4") {
         load_direct(path,&graph_type::load_bintsv4_from_stream);
      } else if (format == "bin") {
         load_binary(path);
      } else {
        logstream(LOG_ERROR)
          << "Unrecognized Format \"" << format << "\"!" << std::endl;
        return;
      }
    } // end of load


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

   ///
   vertex_set neighbors(const vertex_set& cur,
                        edge_dir_type edir) {
     // foreach master bit which is set, set its corresponding mirror
     // synchronize master to mirrors
     vertex_set ret(empty_set());
     ret.make_explicit(*this);

     foreach(size_t lvid, cur.get_lvid_bitset(*this)) {
       if (edir == IN_EDGES || edir == ALL_EDGES) {
         foreach(local_edge_type e, l_vertex(lvid).in_edges()) {
           ret.set_lvid_unsync(e.source().id());
         }
       }
       if (edir == OUT_EDGES || edir == ALL_EDGES) {
         foreach(local_edge_type e, l_vertex(lvid).out_edges()) {
           ret.set_lvid_unsync(e.target().id());
         }
       }
     }
     ret.synchronize_mirrors_to_master_or(*this, vset_exchange);
     ret.synchronize_master_to_mirrors(*this, vset_exchange);
     return ret;
   }


   /**
    * \brief Constructs a vertex set from a predicate operation which
    * is executed on each vertex.
    *
    * This function selects a subset of vertices on which the predicate
    * evaluates to true.
    For instance if vertices contain an integer, the following
    * code will construct a set of vertices containing only vertices with data
    * which are a multiple of 2.
    *
    * \code
    * bool is_multiple_of_2(const graph_type::vertex_type& vertex) {
    *   return vertex.data() % 2 == 0;
    * }
    * vertex_set even_vertices = graph.select(is_multiple_of_2);
    * \endcode
    *
    * select() also takes a second argument which restricts the set of vertices
    * queried. For instance,
    * \code
    * bool is_multiple_of_3(const graph_type::vertex_type& vertex) {
    *   return vertex.data() % 3 == 0;
    * }
    * vertex_set div_6_vertices = graph.select(is_multiple_of_3, even_vertices);
    * \endcode
    * will select from the set of even vertices, all vertices which are also
    * divisible by 3. The resultant set is therefore the set of all vertices
    * which are divisible by 6.
    *
    * \param select_functor A function/functor which takes a
    *                       const vertex_type& argument and returns a boolean
    *                       denoting of the vertex is to be included in the
    *                       returned set
    * \param vset Optional. The set of vertices to evaluate the selection on.
    *                       Defaults to complete_set()
    */
   template <typename FunctionType>
   vertex_set select(FunctionType select_functor,
                     const vertex_set& vset = complete_set()) {
     vertex_set ret(empty_set());

     ret.make_explicit(*this);
#ifdef _OPENMP
        #pragma omp for
#endif
     for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
       if (lvid2record[i].owner == rpc.procid() &&
           vset.l_contains((lvid_type)i)) {
         const vertex_type vtx(l_vertex(i));
         if (select_functor(vtx)) ret.set_lvid(i);
       }
     }
     ret.synchronize_master_to_mirrors(*this, vset_exchange);
     return ret;
   }

   void sync_vertex_set_master_to_mirrors(vertex_set& vset) {
     vset.synchronize_master_to_mirrors(*this, vset_exchange);
   }

   /**
    * \brief Returns the number of vertices in a vertex set.
    *
    * This function must be called on all machines and returns the number of
    * vertices contained in the vertex set.
    *
    * For instance:
    * \code
    *   graph.vertex_set_size(graph.complete_set());
    * \endcode
    * will always evaluate to graph.num_vertices();
    */
   size_t vertex_set_size(const vertex_set& vset) {
     size_t count = 0;
     for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
        count += (lvid2record[i].owner == rpc.procid() &&
                  vset.l_contains((lvid_type)i));
     }
     rpc.all_reduce(count);
     return count;
   }


   /**
    * \brief Returns true if the vertex set is empty
    *
    * This function must be called on all machines and returns
    * true if the vertex set is empty
    */
   bool vertex_set_empty(const vertex_set& vset) {
     if (vset.lazy) return !vset.is_complete_set;

     size_t count = vset.get_lvid_bitset(*this).empty();
     rpc.all_reduce(count);
     return count == rpc.numprocs();
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




    /** \internal
     * \brief converts a local vertex ID to a local vertex object
     */
    local_vertex_type l_vertex(lvid_type vid) {
      return local_vertex_type(*this, vid);
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

    distributed_control& dc() {
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
     *  vertex type while provides access to local graph vertices.
     */
    struct local_vertex_type {
      distributed_graph& graph_ref;
      lvid_type lvid;

      local_vertex_type(distributed_graph& graph_ref, lvid_type lvid):
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
      distributed_graph& graph_ref;
      typename local_graph_type::edge_type e;
    public:
      local_edge_type(distributed_graph& graph_ref,
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
      distributed_graph& graph_ref;
      make_local_edge_type_functor(distributed_graph& graph_ref):
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

      local_edge_list_type(distributed_graph& graph_ref,
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
    mutable dc_dist_object<distributed_graph> rpc;

  public:

    // For the warp engine to find the remote instances of this class
    size_t get_rpc_obj_id() {
      return rpc.get_obj_id();
    }

  private:
    bool finalized;

    /** The local graph data */
    local_graph_type local_graph;

    /** The map from global vertex ids to vertex records */
    std::vector<vertex_record>  lvid2record;

    /** The map from global vertex ids to local vertex ids */
    vid2lvid_map_type vid2lvid;

    /** Simple data structure storing the graph statistics */
    graph_stats stats;

    /** pointer to the distributed ingress object*/
    distributed_ingress_base<graph_type>* ingress_ptr;

    /** Buffered Exchange used by synchronize() */
    buffered_exchange<std::pair<vertex_id_type, vertex_data_type> > vertex_exchange;

    /** Buffered Exchange used by vertex sets */
    buffered_exchange<vertex_id_type> vset_exchange;

    /** Command option to disable parallel ingress. Used for simulating single node ingress */
    bool parallel_ingress;


    lock_manager_type lock_manager;

    void set_ingress_method(const std::string& method,
        size_t bufsize = 50000, bool usehash = false, bool userecent = false) {
      if(ingress_ptr != NULL) { delete ingress_ptr; ingress_ptr = NULL; }
      if (method == "oblivious") {
        if (rpc.procid() == 0) logstream(LOG_EMPH) << "Use oblivious ingress, usehash: " << usehash
          << ", userecent: " << userecent << std::endl;
        ingress_ptr = new distributed_oblivious_ingress<graph_type>(rpc.dc(), *this, usehash, userecent);
      } else if  (method == "random") {
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
          ingress_ptr = new distributed_oblivious_ingress<graph_type>(rpc.dc(), *this, usehash, userecent);
        }
        if (rpc.procid() == 0)logstream(LOG_EMPH) << "Automatically determine ingress method: " << ingress_auto << std::endl;
      }
    } // end of set ingress method


    /**
       \internal
       This internal function is used to load a single line from an input stream
     */
    template<typename Fstream>
    bool load_from_stream(std::string filename, Fstream& fin,
                          line_parser_type& line_parser) {
      size_t linecount = 0;
      timer ti; ti.start();
      while(fin.good() && !fin.eof()) {
        std::string line;
        std::getline(fin, line);
        if(line.empty()) continue;
        if(fin.fail()) break;
        const bool success = line_parser(*this, filename, line);
        if (!success) {
          logstream(LOG_WARNING)
            << "Error parsing line " << linecount << " in "
            << filename << ": " << std::endl
            << "\t\"" << line << "\"" << std::endl;
          return false;
        }
        ++linecount;
        if (ti.current_time() > 5.0) {
          logstream(LOG_INFO) << linecount << " Lines read" << std::endl;
          ti.start();
        }
      }
      return true;
    } // end of load from stream


    template<typename Fstream, typename Writer>
    void save_vertex_to_stream(vertex_type& vertex, Fstream& fout, Writer writer) {
      fout << writer.save_vertex(vertex);
    } // end of save_vertex_to_stream


    template<typename Fstream, typename Writer>
    void save_edge_to_stream(edge_type& edge, Fstream& fout, Writer writer) {
      std::string ret = writer.save_edge(edge);
      fout << ret;
    } // end of save_edge_to_stream


    void save_bintsv4_to_stream(std::ostream& out) {
      for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
        uint32_t src = l_vertex(i).global_id();
        foreach(local_edge_type e, l_vertex(i).out_edges()) {
          uint32_t dest = e.target().global_id();
          out.write(reinterpret_cast<char*>(&src), 4);
          out.write(reinterpret_cast<char*>(&dest), 4);
        }
        if (l_vertex(i).owner() == rpc.procid()) {
          vertex_type gv = vertex_type(l_vertex(i));
          // store disconnected vertices if I am the master of the vertex
          if (gv.num_in_edges() == 0 && gv.num_out_edges() == 0) {
            out.write(reinterpret_cast<char*>(&src), 4);
            uint32_t dest = (uint32_t)(-1);
            out.write(reinterpret_cast<char*>(&dest), 4);
          }
        }
      }
    }

    bool load_bintsv4_from_stream(std::istream& in) {
      while(in.good()) {
        uint32_t src, dest;
        in.read(reinterpret_cast<char*>(&src), 4);
        in.read(reinterpret_cast<char*>(&dest), 4);
        if (in.fail()) break;
        if (dest == (uint32_t)(-1)) {
          add_vertex(src);
        }
        else {
          add_edge(src, dest);
        }
      }
      return true;
    }


    /** \brief Saves a distributed graph using a direct ostream saving function
     *
     * This function saves a sequence of files numbered
     * \li [prefix]_0
     * \li [prefix]_1
     * \li [prefix]_2
     * \li etc.
     *
     * This files can be loaded with direct_stream_load().
     */
    void save_direct(const std::string& prefix, bool gzip,
                    boost::function<void (graph_type*, std::ostream&)> saver) {
      rpc.full_barrier();
      finalize();
      timer savetime;  savetime.start();
      std::string fname = prefix + "_" + tostr(rpc.procid() + 1) + "_of_" +
                          tostr(rpc.numprocs());
      if (gzip) fname = fname + ".gz";
      logstream(LOG_INFO) << "Save graph to " << fname << std::endl;
      if(boost::starts_with(fname, "hdfs://")) {
        graphlab::hdfs hdfs;
        graphlab::hdfs::fstream out_file(hdfs, fname, true);
        boost::iostreams::filtering_stream<boost::iostreams::output> fout;
        if (gzip) fout.push(boost::iostreams::gzip_compressor());
        fout.push(out_file);
        if (!fout.good()) {
          logstream(LOG_FATAL) << "\n\tError opening file: " << fname << std::endl;
          exit(-1);
        }
        saver(this, boost::ref(fout));
        fout.pop();
        if (gzip) fout.pop();
        out_file.close();
      } else {
        std::ofstream out_file(fname.c_str(),
                               std::ios_base::out | std::ios_base::binary);
        if (!out_file.good()) {
          logstream(LOG_FATAL) << "\n\tError opening file: " << fname << std::endl;
          exit(-1);
        }
        boost::iostreams::filtering_stream<boost::iostreams::output> fout;
        if (gzip) fout.push(boost::iostreams::gzip_compressor());
        fout.push(out_file);
        saver(this, boost::ref(fout));
        fout.pop();
        if (gzip) fout.pop();
        out_file.close();
      }
      logstream(LOG_INFO) << "Finish saving graph to " << fname << std::endl
                          << "Finished saving bintsv4 graph: "
                          << savetime.current_time() << std::endl;
      rpc.full_barrier();
    } // end of save



    /**
     *  \brief Load a graph from a collection of files in stored on
     *  the filesystem using the user defined line parser. Like
     *  \ref load(const std::string& path, line_parser_type line_parser)
     *  but only loads from the filesystem.
     */
    void load_direct_from_posixfs(std::string prefix,
                           boost::function<bool (graph_type*, std::istream&)> parser) {
      std::string directory_name; std::string original_path(prefix);
      boost::filesystem::path path(prefix);
      std::string search_prefix;
      if (boost::filesystem::is_directory(path)) {
        // if this is a directory
        // force a "/" at the end of the path
        // make sure to check that the path is non-empty. (you do not
        // want to make the empty path "" the root path "/" )
        directory_name = path.native();
      }
      else {
        directory_name = path.parent_path().native();
        search_prefix = path.filename().native();
        directory_name = (directory_name.empty() ? "." : directory_name);
      }
      std::vector<std::string> graph_files;
      fs_util::list_files_with_prefix(directory_name, search_prefix, graph_files);
      if (graph_files.size() == 0) {
        logstream(LOG_WARNING) << "No files found matching " << original_path << std::endl;
      }
      for(size_t i = 0; i < graph_files.size(); ++i) {
        if (i % rpc.numprocs() == rpc.procid()) {
          logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
          // is it a gzip file ?
          const bool gzip = boost::ends_with(graph_files[i], ".gz");
          // open the stream
          std::ifstream in_file(graph_files[i].c_str(),
                                std::ios_base::in | std::ios_base::binary);
          // attach gzip if the file is gzip
          boost::iostreams::filtering_stream<boost::iostreams::input> fin;
          // Using gzip filter
          if (gzip) fin.push(boost::iostreams::gzip_decompressor());
          fin.push(in_file);
          const bool success = parser(this, boost::ref(fin));
          if(!success) {
            logstream(LOG_FATAL)
              << "\n\tError parsing file: " << graph_files[i] << std::endl;
          }
          fin.pop();
          if (gzip) fin.pop();
        }
      }
      rpc.full_barrier();
    }

    /**
     *  \brief Load a graph from a collection of files in stored on
     *  the HDFS using the user defined line parser. Like
     *  \ref load(const std::string& path, line_parser_type line_parser)
     *  but only loads from HDFS.
     */
    void load_direct_from_hdfs(std::string prefix,
                         boost::function<bool (graph_type*, std::istream&)> parser) {
      // force a "/" at the end of the path
      // make sure to check that the path is non-empty. (you do not
      // want to make the empty path "" the root path "/" )
      std::string path = prefix;
      if (path.length() > 0 && path[path.length() - 1] != '/') path = path + "/";
      if(!hdfs::has_hadoop()) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to load a graph from HDFS but GraphLab"
          << "\n\twas built without HDFS."
          << std::endl;
      }
      hdfs& hdfs = hdfs::get_hdfs();
      std::vector<std::string> graph_files;
      graph_files = hdfs.list_files(path);
      if (graph_files.size() == 0) {
        logstream(LOG_WARNING) << "No files found matching " << prefix << std::endl;
      }
      for(size_t i = 0; i < graph_files.size(); ++i) {
        if (i % rpc.numprocs() == rpc.procid()) {
          logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
          // is it a gzip file ?
          const bool gzip = boost::ends_with(graph_files[i], ".gz");
          // open the stream
          graphlab::hdfs::fstream in_file(hdfs, graph_files[i]);
          boost::iostreams::filtering_stream<boost::iostreams::input> fin;
          if(gzip) fin.push(boost::iostreams::gzip_decompressor());
          fin.push(in_file);
          const bool success = parser(this, boost::ref(fin));
          if(!success) {
            logstream(LOG_FATAL)
              << "\n\tError parsing file: " << graph_files[i] << std::endl;
          }
          fin.pop();
          if (gzip) fin.pop();
        }
      }
      rpc.full_barrier();
    }

    void load_direct(std::string prefix,
             boost::function<bool (graph_type*, std::istream&)> parser) {
      rpc.full_barrier();
      if(boost::starts_with(prefix, "hdfs://")) {
        load_direct_from_hdfs(prefix, parser);
      } else {
        load_direct_from_posixfs(prefix, parser);
      }
      rpc.full_barrier();
    } // end of load

    friend class tests::distributed_graph_test;
  }; // End of graph
} // end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif

