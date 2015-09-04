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

#ifndef GRAPHLAB_DYNAMIC_LOCAL_GRAPH_HPP
#define GRAPHLAB_DYNAMIC_LOCAL_GRAPH_HPP


#include <cmath>

#include <string>
#include <list>
#include <vector>
#include <set>
#include <map>

#include <queue>
#include <algorithm>
#include <functional>
#include <fstream>

#include <boost/bind.hpp>
#include <boost/unordered_set.hpp>
#include <boost/type_traits.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/range/iterator_range.hpp>

#include <graph/graph_basic_types.hpp>
#include <graph/local_edge_buffer.hpp>
#include <random/random.hpp>
#include <graphlab/util/generics/shuffle.hpp>
#include <graphlab/util/generics/counting_sort.hpp>
#include <graphlab/util/generics/dynamic_csr_storage.hpp>
#include <parallel/atomic.hpp>

#include <logger/logger.hpp>
#include <logger/assertions.hpp>

#include <serialization/iarchive.hpp>
#include <serialization/oarchive.hpp>

#include <random/random.hpp>
#include <graphlab/macros_def.hpp>


namespace graphlab {
  template<typename VertexData, typename EdgeData>
  class dynamic_local_graph {

   public:
    /** The type of the vertex data stored in the local_graph. */
    typedef VertexData vertex_data_type;

    /** The type of the edge data stored in the local_graph. */
    typedef EdgeData edge_data_type;

    typedef graphlab::vertex_id_type vertex_id_type;
    typedef graphlab::edge_id_type edge_id_type;

   private:
    /**
     * \internal
     * CSR/CSC storage types
     */
    typedef dynamic_csr_storage<std::pair<lvid_type, edge_id_type>, edge_id_type> csr_type;
    typedef typename csr_type::iterator csr_edge_iterator;

   public:
    typedef std::pair<std::vector<VertexData>, std::vector<EdgeData> > data_container_t;
    typedef std::pair<csr_type, csr_type> structure_container_t;

   private:
    class edge_iterator;

   public:
    typedef boost::iterator_range<edge_iterator> edge_list_type;

    /** Vertex object which provides access to the vertex data
     * and information about it.
     */
    class vertex_type;

      /** Edge object which provides access to the edge data
     * and information about it.
     */
    class edge_type;

   public:
    // CONSTRUCTORS ============================================================>
    /** Create an local_graph with given data and structure containers. */
    dynamic_local_graph(data_container_t& d, structure_container_t& s):
        vertices(d.first), edges(d.second),
        _csr_storage(s.first), _csc_storage(s.second) { }

    /** Create an empty local_graph. */
    dynamic_local_graph(): 
        _vertex_data_ptr(new std::vector<VertexData>()),
        _edge_data_ptr(new std::vector<EdgeData>()),
        _csr_storage_ptr(new csr_type()),
        _csc_storage_ptr(new csr_type()),
        vertices(*_vertex_data_ptr),
        edges(*_edge_data_ptr),
        _csr_storage(*_csr_storage_ptr),
        _csc_storage(*_csc_storage_ptr) { }

    /** Create a local_graph with nverts vertices. */
    dynamic_local_graph(size_t nverts) :
        _vertex_data_ptr(new std::vector<VertexData>(nverts)),
        _edge_data_ptr(new std::vector<EdgeData>()),
        _csr_storage_ptr(new csr_type()),
        _csc_storage_ptr(new csr_type()),
        vertices(*_vertex_data_ptr),
        edges(*_edge_data_ptr),
        _csr_storage(*_csr_storage_ptr),
        _csc_storage(*_csc_storage_ptr) { }

    /*
     * Disable Default Copy Constructor and copy assignment
     */
    dynamic_local_graph(const dynamic_local_graph& other) = delete;

    dynamic_local_graph& operator=(const dynamic_local_graph& other) = delete;


    
    // METHODS =================================================================>

    static bool is_dynamic() {
      return true;
    }

    /**
     * \brief Resets the local_graph state.
     */
    void clear() {
      vertices.clear();
      edges.clear();
      _csc_storage.clear();
      _csr_storage.clear();
      std::vector<VertexData>().swap(vertices);
      std::vector<EdgeData>().swap(edges);
      edge_buffer.clear();
    }

    /** \brief Get the number of vertices */
    size_t num_vertices() const {
      return vertices.size();
    } // end of num vertices

    /** \brief Get the number of edges */
    size_t num_edges() const {
        return edges.size();
    } // end of num edges


    /**
     * \brief Creates a vertex containing the vertex data and returns the id
     * of the new vertex id. Vertex ids are assigned in increasing order with
     * the first vertex having id 0.
     */
    void add_vertex(lvid_type vid, const VertexData& vdata = VertexData() ) {
      if(vid >= vertices.size()) {
        // Enable capacity doubling if resizing beyond capacity
        if(vid >= vertices.capacity()) {
          const size_t new_size = std::max(2 * vertices.capacity(),
                                           size_t(vid));
          vertices.reserve(new_size);
        }
        vertices.resize(vid+1);
      }
      vertices[vid] = vdata;
    } // End of add vertex;

    void reserve(size_t num_vertices) {
      ASSERT_GE(num_vertices, vertices.size());
      vertices.reserve(num_vertices);
    }

    /**
     * \brief Add additional vertices up to provided num_vertices.  This will
     * fail if resizing down.
     */
    void resize(size_t num_vertices ) {
      ASSERT_GE(num_vertices, vertices.size());
      vertices.resize(num_vertices);
    } // End of resize

    void reserve_edge_space(size_t n) {
      edge_buffer.reserve_edge_space(n);
    }
    /**
     * \brief Creates an edge connecting vertex source to vertex target.  Any
     * existing data will be cleared. Should not be called after finalization.
     */
    edge_id_type add_edge(lvid_type source, lvid_type target,
                          const EdgeData& edata = EdgeData()) {
      if(source == target) {
        logstream(LOG_FATAL)
          << "Attempting to add self edge (" << source << " -> " << target <<  ").  "
          << "This operation is not permitted in GraphLab!" << std::endl;
        ASSERT_MSG(source != target, "Attempting to add self edge!");
      }

      if(source >= vertices.size() || target >= vertices.size())
        add_vertex(std::max(source, target));

      // Add the edge to the set of edge data (this copies the edata)
      edge_buffer.add_edge(source, target, edata);

      // This is not the final edge_id, so we always return 0.
      return 0;
    } // End of add edge

    /**
     * \brief Add edges in block.
     */
    void add_edges(const std::vector<lvid_type>& src_arr,
                   const std::vector<lvid_type>& dst_arr,
                   const std::vector<EdgeData>& edata_arr) {
      ASSERT_TRUE((src_arr.size() == dst_arr.size())
                  && (src_arr.size() == edata_arr.size()));

      for (size_t i = 0; i < src_arr.size(); ++i) {
        lvid_type source = src_arr[i];
        lvid_type target = dst_arr[i];
        if ( source >= vertices.size()
             || target >= vertices.size() ) {
          logstream(LOG_FATAL)
            << "Attempting add_edge (" << source
            << " -> " << target
            << ") when there are only " << vertices.size()
            << " vertices" << std::endl;
          ASSERT_MSG(source < vertices.size(), "Invalid source vertex!");
          ASSERT_MSG(target < vertices.size(), "Invalid target vertex!");
        }

        if(source == target) {
          logstream(LOG_FATAL)
            << "Attempting to add self edge (" << source << " -> " << target <<  ").  "
            << "This operation is not permitted in GraphLab!" << std::endl;
          ASSERT_MSG(source != target, "Attempting to add self edge!");
        }
      }
      edge_buffer.add_block_edges(src_arr, dst_arr, edata_arr);
    } // End of add block edges


    /** \brief Returns a vertex of given ID. */
    vertex_type vertex(lvid_type vid) {
      ASSERT_LT(vid, vertices.size());
      return vertex_type(*this, vid);
    }

    /** \brief Returns a vertex of given ID. */
    const vertex_type vertex(lvid_type vid) const {
      ASSERT_LT(vid, vertices.size());
      return vertex_type(*this, vid);
    }

    /** \brief Returns a reference to the data stored on the vertex v. */
    VertexData& vertex_data(lvid_type v) {
      ASSERT_LT(v, vertices.size());
      return vertices[v];
    } // end of data(v)

    /** \brief Returns a constant reference to the data stored on the vertex v. */
    const VertexData& vertex_data(lvid_type v) const {
      ASSERT_LT(v, vertices.size());
      return vertices[v];
    } // end of data(v)

    /**
     * \brief Finalize the local_graph data structure by
     * sorting edges to maximize the efficiency of graphlab.
     * This function takes O(|V|log(degree)) time and will
     * fail if there are any duplicate edges.
     * Detail implementation depends on the type of graph_storage.
     * This is also automatically invoked by the engine at start.
     */
    void finalize() {

      graphlab::timer mytimer; mytimer.start();
#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize starts." << std::endl;
#endif
      std::vector<edge_id_type> src_permute;
      std::vector<edge_id_type> dest_permute;
      std::vector<edge_id_type> src_counting_prefix_sum;
      std::vector<edge_id_type> dest_counting_prefix_sum;

#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize: Sort by source vertex" << std::endl;
#endif
      counting_sort(edge_buffer.source_arr, dest_permute, &src_counting_prefix_sum);
#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize: Sort by dest id" << std::endl;
#endif
      counting_sort(edge_buffer.target_arr, src_permute, &dest_counting_prefix_sum);

      std::vector< std::pair<lvid_type, edge_id_type> >  csr_values;
      std::vector< std::pair<lvid_type, edge_id_type> >  csc_values;

      csr_values.reserve(dest_permute.size());
      edge_id_type begineid = edges.size();
      for (size_t i = 0; i < dest_permute.size(); ++i) {
        csr_values.push_back(std::pair<lvid_type, edge_id_type> (edge_buffer.target_arr[dest_permute[i]],
                                                                 begineid + dest_permute[i]));
      }
      csc_values.reserve(src_permute.size());

      for (size_t i = 0; i < src_permute.size(); ++i) {
        csc_values.push_back(std::pair<lvid_type, edge_id_type> (edge_buffer.source_arr[src_permute[i]],
                                                                 begineid + src_permute[i]));
      }
      ASSERT_EQ(csc_values.size(), csr_values.size());

      // fast path with first time insertion.
      if (edges.size() == 0) {
        edges.swap(edge_buffer.data);
        edge_buffer.clear();
        // warp into csr csc storage.
        _csr_storage.wrap(src_counting_prefix_sum, csr_values);
        _csc_storage.wrap(dest_counting_prefix_sum, csc_values);
      } else {
        // insert edge data
        edges.reserve(edges.size() + edge_buffer.size());
        edges.insert(edges.end(), edge_buffer.data.begin(), edge_buffer.data.end());
        std::vector<EdgeData>().swap(edge_buffer.data);
        edge_buffer.clear();
        size_t begin, end;
        for (size_t i = 0; i < src_counting_prefix_sum.size(); ++i) {
          begin = src_counting_prefix_sum[i];
          end = (i==src_counting_prefix_sum.size()-1)
              ? csr_values.size()
              : src_counting_prefix_sum[i+1];
          if (end > begin) {
            _csr_storage.insert(i, csr_values.begin()+begin, csr_values.begin()+end);
          }
        }
        for (size_t i = 0; i < dest_counting_prefix_sum.size(); ++i) {
          begin = dest_counting_prefix_sum[i];
          end = (i==dest_counting_prefix_sum.size()-1)
              ? csc_values.size()
              : dest_counting_prefix_sum[i+1];
          if (end > begin) {
            _csc_storage.insert(i, csc_values.begin()+begin, csc_values.begin()+end);
          }
        }
        _csr_storage.repack();
        _csc_storage.repack();
      }
      ASSERT_EQ(_csr_storage.num_values(), _csc_storage.num_values());
      ASSERT_EQ(_csr_storage.num_values(), edges.size());

#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "End of finalize." << std::endl;
#endif
      logstream(LOG_INFO) << "Graph finalized in " << mytimer.current_time()
                          << " secs" << std::endl;

#ifdef DEBUG_GRAPH
      _csr_storage.meminfo(std::cerr);
      _csc_storage.meminfo(std::cerr);
#endif
    } // End of finalize


    /** \brief Load the local_graph from an archive */
    void load(iarchive& arc) {
      clear();
      // read the vertices
      arc >> vertices
          >> edges
          >> _csr_storage
          >> _csc_storage;
    } // end of load

    /** \brief Save the local_graph to an archive */
    void save(oarchive& arc) const {
      // Write the number of edges and vertices
      arc << vertices
          << edges
          << _csr_storage
          << _csc_storage;
    } // end of save



    /** swap two graphs */
    void swap(dynamic_local_graph& other) {
      std::swap(vertices, other.vertices);
      std::swap(edges, other.edges);
      std::swap(_csr_storage, other._csr_storage);
      std::swap(_csc_storage, other._csc_storage);
    } // end of swap


    /** \brief Load the local_graph from a file */
    void load(const std::string& filename) {
      std::ifstream fin(filename.c_str());
      iarchive iarc(fin);
      iarc >> *this;
      fin.close();
    } // end of load

    /**
     * \brief save the local_graph to the file given by the filename
     */
    void save(const std::string& filename) const {
      std::ofstream fout(filename.c_str());
      oarchive oarc(fout);
      oarc << *this;
      fout.close();
    } // end of save

    /**
     * \brief save the adjacency structure to a text file.
     *
     * Save the adjacency structure as a text file in:
     *    src_Id, dest_Id \n
     *    src_Id, dest_Id \n
     * format.
     */
    void save_adjacency(const std::string& filename) const {
      std::ofstream fout(filename.c_str());
      ASSERT_TRUE(fout.good());

      for (size_t i = 0; i < num_vertices(); ++i) {
        vertex_type v(i);
        edge_list_type ls = v.out_edges();
        foreach(edge_type e, ls) {
          fout << (lvid_type)i << ", " << e.target().id() << "\n";
          ASSERT_TRUE(fout.good());
        }
      }
      fout.close();
    }

 /****************************************************************************
 *                       Internal Functions                                 *
 *                     ----------------------                               *
 * These functions functions and types provide internal access to the       *
 * underlying local_graph representation. They should not be used unless you      *
 * *really* know what you are doing.                                        *
 ****************************************************************************/
    /**
     * \internal
     * \brief Returns the number of in edges of the vertex with the given id. */
    size_t num_in_edges(const lvid_type v) const {
      return _csc_storage.begin(v).pdistance_to(_csc_storage.end(v));
    }

    /**
     * \internal
     * \brief Returns the number of in edges of the vertex with the given id. */
    size_t num_out_edges(const lvid_type v) const {
      return _csr_storage.begin(v).pdistance_to(_csr_storage.end(v));
    }

    /**
     * \internal
     * \brief Returns a list of in edges of the vertex with the given id. */
    edge_list_type in_edges(lvid_type v) {
      edge_iterator begin = edge_iterator(*this, edge_iterator::CSC,
                                          _csc_storage.begin(v), v);
      edge_iterator end = edge_iterator(*this, edge_iterator::CSC,
                                        _csc_storage.end(v), v);
      return boost::make_iterator_range(begin, end);
    }

    /**
     * \internal
     * \brief Returns a list of out edges of the vertex with the given id. */
    edge_list_type out_edges(lvid_type v) {
      edge_iterator begin = edge_iterator(*this, edge_iterator::CSR,
                                          _csr_storage.begin(v), v);
      edge_iterator end = edge_iterator(*this, edge_iterator::CSR,
                                        _csr_storage.end(v), v);
      return boost::make_iterator_range(begin, end);
    }

    /**
     * \internal
     * \brief Returns edge data of edge_type e
     * */
    EdgeData& edge_data(edge_id_type eid) {
      ASSERT_LT(eid, num_edges());
      return edges[eid];
    }
    /**
     * \internal
     * \brief Returns const edge data of edge_type e
     * */
    const EdgeData& edge_data(edge_id_type eid) const {
      ASSERT_LT(eid, num_edges());
      return edges[eid];
    }

    /**
     * \internal
     * \brief Returns the estimated memory footprint of the local_graph. */
    size_t estimate_sizeof() const {
      const size_t vlist_size = sizeof(vertices) +
        sizeof(VertexData) * vertices.capacity();
      size_t elist_size = _csr_storage.estimate_sizeof()
          + _csc_storage.estimate_sizeof()
          + sizeof(edges) + sizeof(EdgeData)*edges.capacity();
      size_t ebuffer_size = edge_buffer.estimate_sizeof();
      return vlist_size + elist_size + ebuffer_size;
    }

    /** \internal
     * \brief For debug purpose, returns the largest vertex id in the edge_buffer
     */
    const lvid_type maxlvid() const {
      if (edge_buffer.size()) {
        lvid_type max(0);
        foreach(lvid_type i, edge_buffer.source_arr)
         max = std::max(max, i);
        foreach(lvid_type i, edge_buffer.target_arr)
         max = std::max(max, i);
        return max;
      } else {
        return lvid_type(-1);
      }
    }



  private:
    // PRIVATE DATA MEMBERS ===================================================>
    std::shared_ptr< std::vector<VertexData> > _vertex_data_ptr;
    std::shared_ptr< std::vector<EdgeData> > _edge_data_ptr;
    std::shared_ptr< csr_type > _csr_storage_ptr;
    std::shared_ptr< csr_type > _csc_storage_ptr;
    
    /** The vertex data is simply a vector of vertex data */
    std::vector<VertexData>& vertices;
    std::vector<EdgeData>& edges;

    /** Stores the edge data and edge relationships. */
    csr_type& _csr_storage;
    csr_type& _csc_storage;

    /** The edge data is a vector of edges where each edge stores its
        source, destination, and data. Used for temporary storage. The
        data is transferred into CSR+CSC representation in
        Finalize. This will be cleared after finalized.*/
    local_edge_buffer<VertexData, EdgeData> edge_buffer;

    /**************************************************************************/
    /*                                                                        */
    /*                            declare friends                             */
    /*                                                                        */
    /**************************************************************************/
    friend class local_graph_test;
  }; // End of class dynamic_local_graph


  template<typename VertexData, typename EdgeData>
  std::ostream& operator<<(std::ostream& out,
                           const dynamic_local_graph<VertexData, EdgeData>& local_graph) {
    for(lvid_type vid = 0; vid < local_graph.num_vertices(); ++vid) {
      foreach(edge_id_type eid, local_graph.out_edge_ids(vid))
        out << vid << ", " << local_graph.target(eid) << '\n';
    }
    return out;
  }
} // end of namespace graphlab


/////////////////////// Implementation of Helper Class ////////////////////////////

namespace graphlab {
  template<typename VertexData, typename EdgeData>
  class dynamic_local_graph<VertexData, EdgeData>::vertex_type {
     public:
       vertex_type(dynamic_local_graph& lgraph_ref, lvid_type vid):lgraph_ref(lgraph_ref),vid(vid) { }

       /// \brief Returns a constant reference to the data on the vertex.
       const vertex_data_type& data() const {
         return lgraph_ref.vertex_data(vid);
       }
       /// \brief Returns a reference to the data on the vertex.
       vertex_data_type& data() {
         return lgraph_ref.vertex_data(vid);
       }
       /// \brief Returns the number of in edges of the vertex.
       size_t num_in_edges() const {
         return lgraph_ref.num_in_edges(vid);
       }
       /// \brief Returns the number of out edges of the vertex.
       size_t num_out_edges() const {
         return lgraph_ref.num_out_edges(vid);
       }
       /// \brief Returns the ID of the vertex.
       lvid_type id() const {
         return vid;
       }
       /// \brief Returns a list of in edges.
       edge_list_type in_edges() {
         return lgraph_ref.in_edges(vid);
       }
       /// \brief Returns a list of out edges.
       edge_list_type out_edges() {
         return lgraph_ref.out_edges(vid);
       }
     private:
       dynamic_local_graph& lgraph_ref;
       lvid_type vid;
    };

    template<typename VertexData, typename EdgeData>
    class dynamic_local_graph<VertexData, EdgeData>::edge_type {
     public:
      edge_type(dynamic_local_graph& lgraph_ref, lvid_type _source, lvid_type _target, edge_id_type _eid) :
        lgraph_ref(lgraph_ref), _source(_source), _target(_target), _eid(_eid) { }

      /// \brief Returns a constant reference to the data on the edge.
      const edge_data_type& data() const {
        return lgraph_ref.edge_data(_eid);
      }
      /// \brief Returns a reference to the data on the edge.
      edge_data_type& data() {
        return lgraph_ref.edge_data(_eid);
      }
      /// \brief Returns the source vertex of the edge.
      vertex_type source() const {
        return vertex_type(lgraph_ref, _source);
      }
      /// \brief Returns the target vertex of the edge.
      vertex_type target() const {
        return vertex_type(lgraph_ref, _target);
      }
      /// \brief Returns the internal ID of this edge
      edge_id_type id() const { return _eid; }

     private:
      dynamic_local_graph& lgraph_ref;
      lvid_type _source;
      lvid_type _target;
      edge_id_type _eid;
    };

    template<typename VertexData, typename EdgeData>
    class dynamic_local_graph<VertexData, EdgeData>::edge_iterator :
        public boost::iterator_facade < edge_iterator,
                                        edge_type,
                                        boost::random_access_traversal_tag,
                                        edge_type> {
         public:
           enum list_type {CSR, CSC};

           edge_iterator(dynamic_local_graph& lgraph_ref, list_type _type,
                         csr_edge_iterator _iter, lvid_type _vid)
               : lgraph_ref(lgraph_ref), _type(_type), _iter(_iter), _vid(_vid) {}

         private:
           friend class boost::iterator_core_access;

           void increment() {
             ++_iter;
           }
           bool equal(const edge_iterator& other) const
           {
             ASSERT_EQ(_type, other._type);
             return _iter == other._iter;
           }
           edge_type dereference() const {
             return make_value();
           }
           void advance(int n) {
             _iter += n;
           }
           ptrdiff_t distance_to(const edge_iterator& other) const {
             return (other._iter - _iter);
           }
         private:
           edge_type make_value() const {
            typename csr_edge_iterator::reference ref = *_iter;
             switch (_type) {
              case CSC: {
                return edge_type(lgraph_ref, ref.first, _vid, ref.second);
              }
              case CSR: {
                return edge_type(lgraph_ref, _vid, ref.first, ref.second);
              }
              default: return edge_type(lgraph_ref, -1, -1, -1);
             }
           }
           dynamic_local_graph& lgraph_ref;
           const list_type _type;
           csr_edge_iterator _iter;
           const lvid_type _vid;
        }; // end of edge_iterator

} // end of namespace


namespace std {
  /**
   * Swap two graphs
   */
  template<typename VertexData, typename EdgeData>
  inline void swap(graphlab::dynamic_local_graph<VertexData,EdgeData>& a,
                   graphlab::dynamic_local_graph<VertexData,EdgeData>& b) {
    a.swap(b);
  } // end of swap
}; // end of namespace std


#include <graphlab/macros_undef.hpp>
#endif
