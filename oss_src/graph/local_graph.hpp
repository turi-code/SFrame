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

#ifndef GRAPHLAB_LOCAL_GRAPH_HPP
#define GRAPHLAB_LOCAL_GRAPH_HPP

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
#include <graphlab/util/generics/vector_zip.hpp>
#include <graphlab/util/generics/csr_storage.hpp>
#include <parallel/atomic.hpp>

#include <logger/logger.hpp>
#include <logger/assertions.hpp>

#include <serialization/iarchive.hpp>
#include <serialization/oarchive.hpp>

#include <random/random.hpp>
#include <graphlab/macros_def.hpp>

namespace graphlab { 

  template<typename VertexData, typename EdgeData>
  class local_graph {
  public:
    
    /** The type of the vertex data stored in the local_graph. */
    typedef VertexData vertex_data_type;

    /** The type of the edge data stored in the local_graph. */
    typedef EdgeData edge_data_type;

    typedef graphlab::vertex_id_type vertex_id_type;
    typedef graphlab::edge_id_type edge_id_type;

    class edge_type;
    class vertex_type;

  private:
    class edge_iterator;

  public:
    typedef boost::iterator_range<edge_iterator> edge_list_type;

    /** Vertex object which provides access to the vertex data
     * and information about it.
     */ 
    class vertex_type {
     public:
       vertex_type(local_graph& lgraph_ref, lvid_type vid):lgraph_ref(lgraph_ref),vid(vid) { }

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
       local_graph& lgraph_ref;
       lvid_type vid;
    };
    /** Edge object which provides access to the edge data
     * and information about it.
     */
    class edge_type {
     public:
      edge_type(local_graph& lgraph_ref, lvid_type _source, lvid_type _target, edge_id_type _eid) : 
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
      local_graph& lgraph_ref;
      lvid_type _source;
      lvid_type _target;
      edge_id_type _eid;
    };

  public:

    // CONSTRUCTORS ============================================================>
    
    /** Create an empty local_graph. */
    local_graph() : finalized(false) { }

    /** Create a local_graph with nverts vertices. */
    local_graph(size_t nverts) :
      vertices(nverts),
      finalized(false) { }

    // METHODS =================================================================>
    
    static bool is_dynamic() {
      return false;
    }

    /**
     * \brief Resets the local_graph state.
     */
    void clear() {
      finalized = false;
      vertices.clear();
      edges.clear();
      _csc_storage.clear();
      _csr_storage.clear();
      std::vector<VertexData>().swap(vertices);
      std::vector<EdgeData>().swap(edges);
      edge_buffer.clear();
    }

    /**
     * \brief Finalize the local_graph data structure by
     * sorting edges to maximize the efficiency of graphlab.  
     * This function takes O(|V|log(degree)) time and will 
     * fail if there are any duplicate edges.
     * Detail implementation depends on the type of graph_storage.
     * This is also automatically invoked by the engine at start.
     */
    void finalize() {   
      if(finalized) return;
      graphlab::timer mytimer; mytimer.start();
#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize starts." << std::endl;
#endif
      std::vector<edge_id_type> permute;
      std::vector<edge_id_type> src_counting_prefix_sum;
      std::vector<edge_id_type> dest_counting_prefix_sum;
           
#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize: Sort by source vertex" << std::endl;
#endif
      // Sort edges by source;
      // Begin of counting sort.
      counting_sort(edge_buffer.source_arr, permute, &src_counting_prefix_sum);

      // Inplace permute of edge_data, edge_src, edge_target array.
#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize: Inplace permute by source id" << std::endl;
#endif
      lvid_type swap_src; lvid_type swap_target; EdgeData  swap_data;
      for (size_t i = 0; i < permute.size(); ++i) {
        if (i != permute[i]) {
          // Reserve the ith entry;
          size_t j = i;
          swap_data = edge_buffer.data[i];
          swap_src = edge_buffer.source_arr[i];
          swap_target = edge_buffer.target_arr[i];
          // Begin swap cycle:
          while (j != permute[j]) {
            size_t next = permute[j];
            if (next != i) {
              edge_buffer.data[j] = edge_buffer.data[next];
              edge_buffer.source_arr[j] = edge_buffer.source_arr[next];
              edge_buffer.target_arr[j] = edge_buffer.target_arr[next];
              permute[j] = j;
              j = next;
            } else {
              // end of cycle
              edge_buffer.data[j] = swap_data;
              edge_buffer.source_arr[j] = swap_src;
              edge_buffer.target_arr[j] = swap_target;
              permute[j] = j;
              break;
            }
          }
        }
      }
#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize: Sort by dest id" << std::endl;
#endif
      counting_sort(edge_buffer.target_arr, permute, &dest_counting_prefix_sum); 
      // Shuffle source array
#ifdef DEBUG_GRAPH
      logstream(LOG_DEBUG) << "Graph2 finalize: Outofplace permute by dest id" << std::endl;
#endif

      outofplace_shuffle(edge_buffer.source_arr, permute);
      // Use inplace shuffle to reduce peak memory footprint:
      // inplace_shuffle(edge_buffer.source_arr, permute);
      // counting_sort(edge_buffer.target_arr, permute);

      // warp into csr csc storage.
      _csr_storage.wrap(src_counting_prefix_sum, edge_buffer.target_arr);
      std::vector<std::pair<lvid_type, edge_id_type> > csc_value = vector_zip(edge_buffer.source_arr, permute);
      //ASSERT_EQ(csc_value.size(), edge_buffer.size());
      _csc_storage.wrap(dest_counting_prefix_sum, csc_value); 
      edges.swap(edge_buffer.data);
      ASSERT_EQ(_csr_storage.num_values(), _csc_storage.num_values());
      ASSERT_EQ(_csr_storage.num_values(), edges.size());
#ifdef DEBGU_GRAPH
      logstream(LOG_DEBUG) << "End of finalize." << std::endl;
#endif

      logstream(LOG_INFO) << "Graph finalized in " << mytimer.current_time() 
                          << " secs" << std::endl;
      finalized = true;
    } // End of finalize

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
    void add_vertex(lvid_type vid, 
                    const VertexData& vdata = VertexData() ) {
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
      if (finalized) {
        logstream(LOG_FATAL)
          << "Attempting add edge to a finalized local_graph." << std::endl;
        ASSERT_MSG(false, "Add edge to a finalized local_graph.");
      }

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
      if (finalized) {
        logstream(LOG_FATAL)
          << "Attempting add edges to a finalized local_graph." << std::endl;
      }

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

    /** \brief Load the local_graph from an archive */
    void load(iarchive& arc) {
      clear();    
      // read the vertices
      arc >> vertices
          >> edges 
          >> _csr_storage
          >> _csc_storage
          >> finalized;
    } // end of load

    /** \brief Save the local_graph to an archive */
    void save(oarchive& arc) const {
      // Write the number of edges and vertices
      arc << vertices
          << edges
          << _csr_storage  
          << _csc_storage
          << finalized;
    } // end of save
    
    /** swap two graphs */
    void swap(local_graph& other) {
      finalized = other.finalized;
      std::swap(vertices, other.vertices);
      std::swap(edges, other.edges);
      std::swap(_csr_storage, other._csr_storage);
      std::swap(_csc_storage, other._csc_storage);
      std::swap(finalized, other.finalized);
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
      ASSERT_TRUE(finalized);
      return (_csc_storage.end(v) - _csc_storage.begin(v));
    }

    /** 
     * \internal
     * \brief Returns the number of in edges of the vertex with the given id. */
    size_t num_out_edges(const lvid_type v) const {
      ASSERT_TRUE(finalized);
      return (_csr_storage.end(v) - _csr_storage.begin(v));
    }

    /** 
     * \internal
     * \brief Returns a list of in edges of the vertex with the given id. */
    edge_list_type in_edges(lvid_type v) {
      edge_iterator begin = edge_iterator(*this, _csc_storage.begin(v), v);
      edge_iterator end = edge_iterator(*this, _csc_storage.end(v), v);
      return boost::make_iterator_range(begin, end);
    }

    /** 
     * \internal
     * \brief Returns a list of out edges of the vertex with the given id. */
    edge_list_type out_edges(lvid_type v) {

      csr_type::iterator base_begin = _csr_storage.begin(v);
      csr_type::iterator base_end = _csr_storage.end(v);

      edge_id_type begin_eid = base_begin - _csr_storage.begin(0); 
      edge_id_type end_eid = base_end - _csr_storage.begin(0); 

      boost::counting_iterator<edge_id_type> counter_begin(begin_eid);
      boost::counting_iterator<edge_id_type> counter_end(end_eid);

      edge_iterator begin = 
          edge_iterator(*this,
              csr_edge_iterator(csr_iterator_tuple(base_begin, counter_begin)), v);

      edge_iterator end = 
          edge_iterator(*this,
              csr_edge_iterator(csr_iterator_tuple(base_end, counter_end)), v);

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
      // std::cerr << "local_graph: tmplist size: " << (double)elist_size/(1024*1024)
      //           << "  gstoreage size: " << (double)store_size/(1024*1024)
      //           << "  vdata list size: " << (double)vlist_size/(1024*1024)
      //           << std::endl;
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
    /** 
     * \internal
     * CSR/CSC storage types
     */
    typedef csr_storage<lvid_type, edge_id_type> csr_type;
    typedef csr_storage<std::pair<lvid_type, edge_id_type>, edge_id_type> csc_type; 

    typedef boost::tuple<csr_type::iterator,
                         boost::counting_iterator<edge_id_type>
                         > csr_iterator_tuple;

    typedef boost::zip_iterator<csr_iterator_tuple> csr_edge_iterator;
    typedef csc_type::iterator csc_edge_iterator;

    class edge_iterator : 
        public boost::iterator_facade <
        edge_iterator,
        edge_type,
        boost::random_access_traversal_tag,
        edge_type> {
         public:
           edge_iterator(local_graph& lgraph_ref, 
                         csc_edge_iterator iter, lvid_type sourceid) 
               : lgraph_ref(lgraph_ref), _type(CSC), csc_iter(iter), vid(sourceid) {}
           edge_iterator(local_graph& lgraph_ref,
                         csr_edge_iterator iter, lvid_type destid) 
               : lgraph_ref(lgraph_ref), _type(CSR), csr_iter(iter), vid(destid) {}

         private:
           friend class boost::iterator_core_access;

           void increment() {
             switch (_type) {
              case CSC: ++csc_iter; break;
              case CSR: ++csr_iter; break;
              default: return;
             }
           }
           bool equal(const edge_iterator& other) const
           {
             ASSERT_EQ(_type, other._type);
             switch (_type) {
              case CSC: return csc_iter == other.csc_iter;
              case CSR: return csr_iter == other.csr_iter;
              default: return true;
             }
           }
           edge_type dereference() const { 
             return make_value();
           }
           void decrement() {
             switch (_type) {
              case CSC: --csc_iter; break;
              case CSR: --csr_iter; break;
              default: return;
             }
           }
           void advance(int n) {
             switch (_type) {
              case CSC: csc_iter+=n; break;
              case CSR: csr_iter+=n; break;
              default: return;
             }
           } 
           ptrdiff_t distance_to(const edge_iterator& other) const {
             switch (_type) {
              case CSC: return other.csc_iter - csc_iter;
              case CSR: return other.csr_iter - csr_iter;
              default: return 0;
             }
           }
         private:
           edge_type make_value() const {
             switch (_type) {
              case CSC: {
                typename csc_edge_iterator::reference val
                    = *csc_iter;
                return edge_type(lgraph_ref, val.first, vid, val.second);
              }
              case CSR: {
                typename csr_edge_iterator::reference val
                    = *csr_iter;
                return edge_type(lgraph_ref,
                                 vid,
                                 val.template get<0>(),
                                 val.template get<1>());
              }
              default: return edge_type(lgraph_ref, -1, -1, -1);
             }
           }
           enum list_type {CSR, CSC}; 
           local_graph& lgraph_ref;
           const list_type _type;
           csc_edge_iterator csc_iter;
           csr_edge_iterator csr_iter;
           const lvid_type vid;
        }; // end of edge_iterator


    /**************************************************************************/
    /*                                                                        */
    /*                          PRIVATE DATA MEMBERS                          */
    /*                                                                        */
    /**************************************************************************/
    /** The vertex data is simply a vector of vertex data */
    std::vector<VertexData> vertices;

    /** Stores the edge data and edge relationships. */
    csr_type _csr_storage;
    csc_type _csc_storage;
    std::vector<EdgeData> edges;

    /** The edge data is a vector of edges where each edge stores its
        source, destination, and data. Used for temporary storage. The
        data is transferred into CSR+CSC representation in
        Finalize. This will be cleared after finalized.*/
    local_edge_buffer<VertexData, EdgeData> edge_buffer;
   
    /** Mark whether the local_graph is finalized.  Graph finalization is a
        costly procedure but it can also dramatically improve
        performance. */
    bool finalized;


    /**************************************************************************/
    /*                                                                        */
    /*                            declare friends                             */
    /*                                                                        */
    /**************************************************************************/
    friend class local_graph_test; 
  }; // End of class local_graph


  template<typename VertexData, typename EdgeData>
  std::ostream& operator<<(std::ostream& out,
                           local_graph<VertexData, EdgeData>& g) {
    typedef typename local_graph<VertexData, EdgeData>::edge_type edge_type;
    for(lvid_type vid = 0; vid < g.num_vertices(); ++vid) {
      foreach(const edge_type& e, g.out_edges(vid))
        out << e.source().id() << ", " << e.target().id() << '\n';
    }
    return out;
  }
} // end of namespace graphlab


namespace std {
  /**
   * Swap two graphs
   */
  template<typename VertexData, typename EdgeData>
  inline void swap(graphlab::local_graph<VertexData,EdgeData>& a,
                   graphlab::local_graph<VertexData,EdgeData>& b) {
    a.swap(b);
  } // end of swap

}; // end of namespace std

#include <graphlab/macros_undef.hpp>
#endif
