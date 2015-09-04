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


/**
 * \file graph_ops.hpp
 *
 * This file supports basic graph io operations to simplify reading
 * and writing adjacency structures from files.
 *
 */

#ifndef GRAPHLAB_GRAPH_OPS_HPP
#define GRAPHLAB_GRAPH_OPS_HPP



#include <iostream>
#include <fstream>
#include <string>

#include <boost/algorithm/string/predicate.hpp>
#include <graph/distributed_graph.hpp>

#include <graphlab/macros_def.hpp>
namespace graphlab {
  

  namespace graph_ops {
    
    
    /**
     * builds a topological_sort of the graph returning it in topsort. 
     * 
     * \param[out] topsort Resultant topological sort of the graph vertices.
     *
     * function will return false if graph is not acyclic.
     */
    template <typename VertexType, typename EdgeType>
    bool topological_sort(const distributed_graph<VertexType, EdgeType>& graph, 
                          std::vector<vertex_id_type>& topsort) {
      typedef distributed_graph<VertexType, EdgeType> graph_type;
      topsort.clear();
      topsort.reserve(graph.num_vertices());
      std::vector<size_t> indeg;
      indeg.resize(graph.num_vertices());
      std::queue<vertex_id_type> q;
      for (size_t i = 0;i < graph.num_vertices(); ++i) {
        indeg[i] = graph.get_in_edges(i).size();
        if (indeg[i] == 0) {
          q.push(i);
        }
      }
    
      while (!q.empty()) {
        vertex_id_type v = q.front();
        q.pop();
        topsort.push_back(v);
        foreach(typename graph_type::edge_type edge, graph.get_out_edges(v)) {
          vertex_id_type destv = edge.target();
          --indeg[destv];
          if (indeg[destv] == 0) {
            q.push(destv);
          }
        }
      }
      if (q.empty() && topsort.size() != graph.num_vertices()) {
        return false;
      }
      return true;
    } // end of topological sort


    template <typename VertexType, typename EdgeType>
    size_t num_neighbors(const distributed_graph<VertexType, EdgeType>& graph, 
                         vertex_id_type& vid) {
      typedef distributed_graph<VertexType, EdgeType> graph_type;
      typename graph_type::edge_list_type in_edges =  graph.in_edges(vid); 
      typename graph_type::edge_list_type out_edges = graph.out_edges(vid);
      typename graph_type::edge_list_type::const_iterator i = in_edges.begin();
      typename graph_type::edge_list_type::const_iterator j = out_edges.begin();
      size_t count = 0;      
      for( ; i != in_edges.end() && j != out_edges.end(); ++count) {
        if(i->source() == j->target()) { 
          ++i; ++j; 
        } else if(i->source() < j->target()) { 
          ++i; 
        } else { 
          ++j; 
        }
      }
      for( ; i != in_edges.end(); ++i, ++count);
      for( ; j != out_edges.end(); ++j, ++count);
      return count;
    } // end of num_neighbors



    template <typename VertexType, typename EdgeType>
    void neighbors(const distributed_graph<VertexType, EdgeType>& graph, 
                   const vertex_id_type vid,   
                   std::vector<vertex_id_type>& neighbors ) {
      typedef distributed_graph<VertexType, EdgeType> graph_type;
      typename graph_type::edge_list_type in_edges =  graph.in_edges(vid); 
      typename graph_type::edge_list_type out_edges = graph.out_edges(vid);
      typename graph_type::edge_list_type::const_iterator i = in_edges.begin();
      typename graph_type::edge_list_type::const_iterator j = out_edges.begin();
      while(i != in_edges.end() && j != out_edges.end()) {
        if(i->source() == j->target()) { 
          neighbors.push_back(i->source()); 
          ++i; ++j; 
        } else if(i->source() < j->target()) {
          neighbors.push_back(i->source()); 
          ++i; 
        } else { 
          neighbors.push_back(j->target()); 
          ++j; 
        } 
      }
      for( ; i != in_edges.end(); ++i) neighbors.push_back(i->source());
      for( ; j != out_edges.end(); ++j) neighbors.push_back(j->target());
    } // end of neighbors




    


    
    template <typename VertexType, typename EdgeType>
    bool save_metis_structure(const std::string& filename,
                              const distributed_graph<VertexType, EdgeType>& graph) { 
      typedef distributed_graph<VertexType, EdgeType> graph_type;
      typedef typename graph_type::edge_type          edge_type;
      typedef typename graph_type::edge_list_type     edge_list_type;
    
      std::ofstream fout(filename.c_str());
      if(!fout.good()) return false;
      // Count the number of actual edges
      size_t nedges = 0;
      for(vertex_id_type i = 0; i < graph.num_vertices(); ++i)
        nedges += num_neighbors(graph, i);
      fout << graph.num_vertices() << ' ' << (nedges/2) << '\n';
      // Save the adjacency structure
      std::vector<vertex_id_type> neighbor_set;
      for(vertex_id_type i = 0; i < graph.num_vertices(); ++i) {
        neighbors(graph, i, neighbor_set);
        for(size_t j = 0; j < neighbor_set.size(); ++j) {
          fout << (neighbor_set[j] + 1);
          if(j + 1 < neighbor_set.size()) fout << ' ';
        }
        fout << '\n';
      }
      fout.close();
      return true;
    } // end of save metis





    template <typename VertexType, typename EdgeType>
    bool save_edge_list_structure(const std::string& filename,
                                  const distributed_graph<VertexType, EdgeType>& graph) { 
      typedef distributed_graph<VertexType, EdgeType> graph_type;
      typedef typename graph_type::edge_type          edge_type;
      typedef typename graph_type::edge_list_type     edge_list_type;

      std::ofstream fout(filename.c_str());
      if(!fout.good()) return false;
      for(vertex_id_type i = 0; i < graph.num_vertices(); ++i) 
        foreach(edge_type edge, graph.out_edges(i)) 
          fout << edge.source() << '\t' << edge.target() << '\n';      
      fout.close();
      return true;
    } // end of save metis




    template <typename VertexType, typename EdgeType>
    bool save_zoltan_hypergraph_structure(const std::string& filename,
                                          const distributed_graph<VertexType, EdgeType>& graph) { 
      typedef distributed_graph<VertexType, EdgeType> graph_type;
      typedef typename graph_type::edge_type          edge_type;
      typedef typename graph_type::edge_list_type     edge_list_type;

      std::ofstream fout(filename.c_str());
      if(!fout.good()) return false;

      // ok. I need to uniquely number each edge.
      // how?
      boost::unordered_map<std::pair<vertex_id_type, 
        vertex_id_type>, size_t> edgetoid;
      size_t curid = 0;
      for(vertex_id_type i = 0; i < graph.num_vertices(); ++i) {
        foreach(const typename graph_type::edge_type& edge, graph.in_edges(i)) {
          std::pair<vertex_id_type, vertex_id_type> e = 
            std::make_pair(edge.source(), edge.target());
          if (e.first > e.second) std::swap(e.first, e.second);
          if (edgetoid.find(e) == edgetoid.end()) {
            edgetoid[e] = curid;
            ++curid;
          }
        }
        foreach(const typename graph_type::edge_type& edge, graph.out_edges(i)) {
          std::pair<vertex_id_type, vertex_id_type> e = 
            std::make_pair(edge.source(), edge.target());
          if (e.first > e.second) std::swap(e.first, e.second);
          if (edgetoid.find(e) == edgetoid.end()) {
            edgetoid[e] = curid;
            ++curid;
          }
        }
      }

      size_t numedges = curid;
      // each edge is a vertex, each vertex is an edge
      // a pin is total adjacency of a hyper edge
      fout << numedges << "\n\n";
      for (size_t i = 0;i < numedges; ++i) {
        fout << i+1 << "\n";
      }
      fout << "\n";
      fout << graph.num_vertices() << "\n\n";
      
      fout << numedges * 2 << "\n\n";
      // loop over the "hyperedge" and write out the edges it is adjacent to
      for(vertex_id_type i = 0; i < graph.num_vertices(); ++i) {
        boost::unordered_set<size_t> adjedges;
        foreach(const typename graph_type::edge_type& edge, graph.in_edges(i)) {
          std::pair<vertex_id_type, vertex_id_type> e = 
            std::make_pair(edge.source(), edge.target());
          if (e.first > e.second) std::swap(e.first, e.second);
          adjedges.insert(edgetoid[e]);
        }
        foreach(const typename graph_type::edge_type& edge, graph.out_edges(i)) {
          std::pair<vertex_id_type, vertex_id_type> e = 
            std::make_pair(edge.source(), edge.target());
          if (e.first > e.second) std::swap(e.first, e.second);
          adjedges.insert(edgetoid[e]);
        }
        // write
        std::vector<size_t> adjedgesvec;
        std::copy(adjedges.begin(), adjedges.end(), 
                  std::inserter(adjedgesvec, adjedgesvec.end()));
        fout << i+1 << " " << adjedgesvec.size() << "\t";        
        for (size_t j = 0;j < adjedgesvec.size(); ++j) {
          fout << adjedgesvec[j] + 1;
          if (j < adjedgesvec.size() - 1) fout << "\t";
        }
        fout << "\n";
      }
      fout.close();
      return true;
    }  // end of save_zoltan_hypergraph_structure



  }; // end of graph ops
}; // end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif





