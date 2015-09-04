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

// standard C++ headers
#include <iostream>
#include <vector>
#include <cxxtest/TestSuite.h>


template<typename T>
std::vector<T> operator+=(std::vector<T>& v1, const std::vector<T>& v2) {
  for (size_t i = 0; i < v2.size(); ++i)
    v1.push_back(v2[i]);
  return v1;
}


#include <rpc/dc.hpp>
#include <rpc/dc_init_from_mpi.hpp>
#include <rpc/mpi_tools.hpp>
#include <graph/distributed_graph2.hpp>
#include <graph/graphio.hpp>
#include <graphlab/macros_def.hpp>


graphlab::distributed_control* dc;

template<typename K, typename V>
class map_reduce;

namespace tests{
   struct vertex_data: public graphlab::IS_POD_TYPE  {
     size_t value;
     vertex_data() : value(0) { }
     vertex_data(size_t n) : value(n) { }
     bool operator==(const vertex_data& other)  const {
       return value == other.value;
     }
   };

   struct edge_data: public graphlab::IS_POD_TYPE  {
     int from;
     int to;
     edge_data (int f = 0, int t = 0) : from(f), to(t) {}
     bool operator==(const edge_data& other)  const {
       return ((from == other.from) && (to == other.to));
     }
   };

   typedef graphlab::distributed_graph2<vertex_data, edge_data> graph_type;

class distributed_graph_test  {
 public:
   /**
    * Test adding vertex.
    */
   void test_add_vertex() {
     graph_type g(*dc);
     test_add_vertex_impl(g, 100);
     test_add_vertex_impl(g, 1000);
     test_add_vertex_impl(g, 10000);
     dc->cout() << "\n+ Pass test: graph add vertex. :) \n";
   }

   /**
    * Test adding edges
    */
   void test_add_edge() {
     graph_type g(*dc);
     test_add_edge_impl(g, 10);
     test_add_edge_impl(g, 1000);
     test_add_edge_impl(g, 10000);
     dc->cout() << "\n+ Pass test: graph add edge. :) \n";
   }

   /**
    * Test adding edges
    */
   void test_dynamic_add_edge() {
     graph_type g(*dc);
     if (g.is_dynamic()) {
       test_add_edge_impl(g, 10, true);
       test_add_edge_impl(g, 1000, true);
       test_add_edge_impl(g, 10000, true);
       dc->cout() << "\n+ Pass test: graph dynamically add edge. :) \n";
     } else {
       dc->cout() << "\n- Graph does not support dynamic. Please compile with -DUSE_DYNAMIC_GRAPH \n";
     }
   }

   /**
    * Test save load
    */
   void test_save_load() {
     graph_type g(*dc);
     for (size_t i = 0; i < 10; ++i) {
       g.add_edge(i, (i+1), edge_data(i, i+1));
     }
     g.finalize();
     for (auto format : {"tsv", "snap", "graphjrl", "bin"}) {
       test_save_load_impl(g, format);
     }
     if (g.is_dynamic()) {
       for (size_t i = 0; i < 10; ++i) {
         g.add_edge(i+1, (i), edge_data(i+1, i));
       }
       g.finalize();
       for (auto format : {"tsv", "snap", "graphjrl", "bin"}) {
         test_save_load_impl(g, format);
       }
     }
     dc->cout() << "\n+ Pass test: graph save load binary. :) \n";
   }

 private: 
   template<typename Graph>
       void test_add_vertex_impl(Graph& g, size_t nverts) {
         g.clear();
         ASSERT_EQ(g.num_vertices(), 0);
         for (size_t i = 0; i < nverts; ++i) {
           g.add_vertex(i, vertex_data(i));
         }
         ASSERT_EQ(g.num_vertices(), 0); 
         g.finalize();
         for (size_t i = 0; i < g.num_local_vertices(); ++i) {
           ASSERT_EQ(g.l_vertex(i).data().value, g.global_vid(i));
         }
         ASSERT_EQ(g.num_vertices(), nverts);

         // Test dynamic graph capability
         if (g.is_dynamic()) {
           // dynamic graph should support adding vertices after finalization
           // add more vertices and override existing vertex values
           for (size_t i = 0; i < 2*nverts; ++i) {
             g.add_vertex(i, vertex_data(i*2));
           }
           g.finalize();
           ASSERT_EQ(g.num_vertices(), 2*nverts);
           for (size_t i = 0; i < g.num_local_vertices(); ++i) {
             ASSERT_EQ(g.l_vertex(i).data().value, g.global_vid(i) * 2);
           }
         }
       }

   template<typename Graph>
       void test_add_edge_impl(Graph& g, size_t nedges, bool use_dynamic = false) {
         typedef typename Graph::vertex_id_type vertex_id_type;
         srand(0);
         g.clear();
         ASSERT_EQ(g.num_edges(), 0);
         boost::unordered_map<vertex_id_type, std::vector<vertex_id_type> > out_edges;
         boost::unordered_map<vertex_id_type, std::vector<vertex_id_type> > in_edges;
         boost::unordered_set< std::pair<vertex_id_type,vertex_id_type> > all_edges;
         while (all_edges.size() < nedges) {
           vertex_id_type src = rand() % (int)(3*sqrt(nedges));
           vertex_id_type dst = rand() % (int)(3*sqrt(nedges));
           if (src == dst)
             continue;
           std::pair<vertex_id_type,vertex_id_type> pair(src, dst);
           if (!all_edges.count(pair))  {
             all_edges.insert(pair);
             if (!out_edges.count(src)) {
               out_edges[src] = std::vector<vertex_id_type>();
             } 
             if (!in_edges.count(dst)) {
               in_edges[dst] = std::vector<vertex_id_type>();
             }
             in_edges[dst].push_back(src);
             out_edges[src].push_back(dst);
           }
         }
         typedef typename boost::unordered_set< std::pair<vertex_id_type,vertex_id_type> >::value_type pair_type; 
         int count = 0;
         foreach (const pair_type& p, all_edges) {
           if (count++ % dc->numprocs() == dc->procid()) {
             g.add_edge(p.first, p.second, edge_data(p.first, p.second));
           }
           if (use_dynamic && count % (nedges/5) == 0) {
             g.finalize();
           } 
         }
         if (!use_dynamic)
           ASSERT_EQ(g.num_edges(), 0); 

         g.finalize();
         check_adjacency(g, in_edges, out_edges, all_edges.size());
         check_edge_data(g);
         check_vertex_info(g);
       }

   template<typename Graph>
       void test_save_load_impl(Graph& g, std::string format) {
         typedef typename Graph::local_edge_type local_edge_type;

         // Only runs on a single machine, ok with multiple processes.
         using namespace boost::filesystem;
         std::string path_string = "";
         if (g.procid() == 0) {
           path_string = unique_path().string() + "-" + format;
         }
         if (g.procid() == 0) {
           if (!create_directory(path(path_string))) {
             logstream(LOG_WARNING) << "Unable to create tmp directory "
                                    << path_string << std::endl;
             return;
           }
         }
         g.dc().all_reduce(path_string);
         path prefix = path_string;
         prefix /= "test"; 
         dc->cout() << "Save to path: " << prefix.string() << std::endl;
         graphlab::io::save_format(g, prefix.string(), format, false);

         Graph g2(*dc);
         graphlab::io::load_format(g2, prefix.string(), format);

         ASSERT_EQ(g.num_vertices(), g2.num_vertices());
         ASSERT_EQ(g.num_edges(), g2.num_edges());

         if (format == "bin") {
           for (size_t i = 0; i < g.num_local_vertices(); ++i) {
             // check vertex records
             ASSERT_TRUE(g.l_get_vertex_record(i) == g2.l_get_vertex_record(i));

             // check vertex data 
             ASSERT_TRUE(g.l_vertex(i).data() == g2.l_vertex(i).data());

             // check local in edges
             ASSERT_EQ(g.l_in_edges(i).size(), g2.l_in_edges(i).size());
             size_t in_edge_size = g.l_in_edges(i).size();
             for (size_t j = 0; j < in_edge_size; ++j) {
               ASSERT_EQ(g.l_in_edges(i)[j].source().lvid,
                         g2.l_in_edges(i)[j].source().lvid);
               ASSERT_EQ(g.l_in_edges(i)[j].target().lvid,
                         g2.l_in_edges(i)[j].target().lvid);
               ASSERT_TRUE(g.l_in_edges(i)[j].data() == g2.l_in_edges(i)[j].data());
             }

             // check local out edges
             ASSERT_EQ(g.l_out_edges(i).size(), g2.l_out_edges(i).size());
             size_t out_edge_size = g.l_out_edges(i).size();
             for (size_t j = 0; j < out_edge_size; ++j) {
               ASSERT_EQ(g.l_out_edges(i)[j].source().lvid,
                         g2.l_out_edges(i)[j].source().lvid);
               ASSERT_EQ(g.l_out_edges(i)[j].target().lvid,
                         g2.l_out_edges(i)[j].target().lvid);
               ASSERT_TRUE(g.l_out_edges(i)[j].data() == g2.l_out_edges(i)[j].data());
             }
           }
         }
         if (dc->procid() == 0) {
            dc->cout() << "Remove path: " << path_string<< std::endl;
            remove_all(path(path_string));
         }
       }

   template<typename Graph>
       void check_edge_data(Graph& g) {
         typedef typename Graph::local_edge_list_type local_edge_list_type;
         typedef typename Graph::local_edge_type local_edge_type;
         typedef typename Graph::vertex_type vertex_type;
         typedef typename Graph::vertex_id_type vertex_id_type;
         for (size_t i = 0; i < g.num_local_vertices(); ++i) {
           const local_edge_list_type& in_edges = g.l_in_edges(i);
           foreach (const local_edge_type& e, in_edges) {
             ASSERT_EQ(e.data().from, g.global_vid(e.source().id()));
             ASSERT_EQ(e.data().to, g.global_vid(e.target().id()));
           }
           const local_edge_list_type& out_edges = g.l_out_edges(i);
           foreach (const local_edge_type& e, out_edges) {
             ASSERT_EQ(e.data().from, g.global_vid(e.source().id()));
             ASSERT_EQ(e.data().to, g.global_vid(e.target().id()));
           }
         }
       }

   /**
    * Helper function to check the in/out edges of the graph.
    */
   template<typename Graph>
       void check_adjacency(Graph& g, 
                            boost::unordered_map<typename Graph::vertex_id_type, 
                            std::vector<typename Graph::vertex_id_type> >& in_edges,
                            boost::unordered_map<typename Graph::vertex_id_type, 
                            std::vector<typename Graph::vertex_id_type> >& out_edges,
                            size_t nedges) {
         typedef typename Graph::local_edge_list_type local_edge_list_type;
         typedef typename Graph::local_edge_type local_edge_type;
         typedef typename Graph::vertex_type vertex_type;
         typedef typename Graph::vertex_id_type vertex_id_type;

         // check total edge size 
         ASSERT_EQ(g.num_edges(), nedges);
         size_t sum_local_edges = g.num_local_edges();
         dc->all_reduce(sum_local_edges);
         ASSERT_EQ(g.num_edges(), sum_local_edges);

         // check local edge size
         size_t local_in_edge_size = 0;
         size_t local_out_edge_size = 0;
         for (size_t i = 0; i < g.num_local_vertices(); ++i) {
           local_in_edge_size += g.l_in_edges(i).size();
           local_out_edge_size += g.l_out_edges(i).size();
         }
         ASSERT_EQ(local_in_edge_size, g.num_local_edges());
         ASSERT_EQ(local_out_edge_size, g.num_local_edges());

         // check adjacency list
         typedef map_reduce< vertex_id_type, std::vector<vertex_id_type> > dist_adj_type;
         dist_adj_type local_out_adj, local_in_adj;

         for (size_t i = 0; i < g.num_local_vertices(); ++i) {
           std::vector<vertex_id_type> outids, inids;
           vertex_id_type gvid = g.global_vid(i);
           const local_edge_list_type& ls_out = g.l_out_edges(i);
           const local_edge_list_type& ls_in = g.l_in_edges(i);
           foreach (const local_edge_type& e, ls_out) {
             ASSERT_EQ(e.source().id(), i);
             outids.push_back(g.global_vid(e.target().id()));
           }
           foreach (const local_edge_type& e, ls_in) {
             ASSERT_EQ(e.target().id(), i);
             inids.push_back(g.global_vid(e.source().id()));
           }
           local_out_adj.data[gvid] = outids;
           local_in_adj.data[gvid] = inids;
         }
         dc->all_reduce(local_out_adj);
         dc->all_reduce(local_in_adj);

         typedef typename boost::unordered_map<vertex_id_type, std::vector<vertex_id_type> >::const_iterator iter_type;

         // check out adjacency 
         for (iter_type it = out_edges.begin(); it != out_edges.end(); ++it) {
           vertex_id_type id = it->first;
           std::vector<vertex_id_type> expected = it->second;
           std::vector<vertex_id_type> actual = local_out_adj.data[id];
           std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
           ASSERT_EQ(actual.size(), expected.size());
           if (g.vid2lvid.count(id))
             ASSERT_EQ(g.num_out_edges(id), expected.size());
           for (size_t i = 0; i < actual.size(); ++i) {
             ASSERT_EQ(actual[i], expected[i]);
           }
         }

         // check in adjacency
         for (iter_type it = in_edges.begin(); it != in_edges.end(); ++it) {
           vertex_id_type id = it->first;
           std::vector<vertex_id_type> expected = it->second;
           std::vector<vertex_id_type> actual = local_in_adj.data[id];
           std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
           ASSERT_EQ(actual.size(), expected.size());
           if (g.vid2lvid.count(id))
             ASSERT_EQ(g.num_in_edges(id), expected.size());
           for (size_t i = 0; i < actual.size(); ++i) {
             ASSERT_EQ(actual[i], expected[i]);
           }
         }
       }

   template<typename Graph>
       struct vertex_info {
         typename Graph::vertex_id_type vid;
         typename Graph::vertex_data_type data;
         typename Graph::mirror_type mirrors;
         graphlab::procid_t master;
         size_t num_in_edges, num_out_edges;

         bool operator==(const vertex_info& other) {
           return ((master == other.master) && 
                   (vid == other.vid) && 
                   (data == other.data) &&
                   (mirrors == other.mirrors) &&
                   (num_in_edges == other.num_in_edges) &&
                   (num_out_edges == other.num_out_edges));
         }

         void load(graphlab::iarchive& arc) {
           arc >> vid
               >> master 
               >> mirrors
               >> num_in_edges
               >> num_out_edges
               >> data;
         }

         void save(graphlab::oarchive& arc) const {
           arc << vid
               << master
               << mirrors
               << num_in_edges
               << num_out_edges
               << data;
         } // end of save
       };

   template<typename Graph>
       void check_vertex_info(Graph& g) {
         typedef typename Graph::vertex_id_type vertex_id_type;
         typedef typename Graph::vertex_data_type vertex_data_type;
         typedef typename Graph::vertex_type vertex_type;
         typedef typename Graph::local_vertex_type local_vertex_type;
         typedef vertex_info<Graph> vinfo_type;
         typedef typename boost::unordered_map<vertex_id_type, vinfo_type > vinfo_map_type; 

         vinfo_map_type vid2info;
         std::vector<vertex_id_type> vids;

         for (size_t i = 0; i < g.num_local_vertices(); ++i) {
           vertex_type v = g.vertex(g.global_vid(i));
           local_vertex_type lv = g.l_vertex(i);
           ASSERT_EQ(v.local_id(), lv.id());
           ASSERT_EQ(v.id(), lv.global_id());

           vinfo_type info;
           info.vid = v.id();
           info.num_in_edges = v.num_in_edges();
           info.num_out_edges = v.num_out_edges();
           info.data = v.data();
           info.mirrors = lv.mirrors();
           info.master = lv.owner();
           // master should not be in the mirror set
           ASSERT_TRUE(info.mirrors.get(info.master) == 0);

           vid2info[v.id()] = info;
           if (lv.owned()) 
             vids.push_back(v.id());
         }

         // gather the vid->record map on each machine
         std::vector<vinfo_map_type> vinfo_map_gather(dc->numprocs());
         vinfo_map_gather[dc->procid()] = vid2info;
         dc->all_gather(vinfo_map_gather);
         dc->all_reduce(vids);

         ASSERT_EQ(vids.size(), g.num_vertices());

         // check the consistency of vertex_record on each machine. 
         foreach(vertex_id_type vid, vids) {
           std::vector<vinfo_type> records;
           std::vector<size_t> mirror_expected;

           for (size_t i = 0; i < vinfo_map_gather.size(); ++i) {
             if (vinfo_map_gather[i].count(vid)) {
               records.push_back(vinfo_map_gather[i][vid]);
               mirror_expected.push_back(i);
             }
           }

           // check vertex records are consistent  across machines.
           for (size_t i = 1; i < records.size(); ++i) {
             ASSERT_TRUE(records[i] == records[0]);
           }

           // recevied record size == mirror size + 1
           ASSERT_EQ(records.size(), records[0].mirrors.popcount()+1);

           for (size_t i = 0; i < mirror_expected.size(); ++i) {
             size_t procid =  mirror_expected[i];
             ASSERT_TRUE(records[0].mirrors.get(procid) || (records[0].master == procid));
           }
         } // end for loop over all vertices
       }
}; // end of distributed_graph_test

} // namespace

using namespace tests;

template<typename K, typename V>
class map_reduce {
 public:
   boost::unordered_map<K, V> data; 
   void save(graphlab::oarchive& oarc) const {
     oarc << data;
   }
   void load(graphlab::iarchive& iarc) {
     iarc >> data;
   }
   map_reduce& operator+=(const map_reduce& other) {
     for (typename boost::unordered_map<K, V>::const_iterator it = other.data.begin();
          it != other.data.end(); ++it) {
       K key = it->first;
       V val = it->second;
       if (data.count(key)) {
         data[key] += val;
       } else {
         data[key] = val;
       }
     }
     return *this;
   }
};




int main(int argc, char** argv) {
  graphlab::mpi_tools::init(argc, argv);
  dc = new graphlab::distributed_control();

  // run tests
  distributed_graph_test testsuit; 
  testsuit.test_add_vertex();
  testsuit.test_add_edge();
  testsuit.test_dynamic_add_edge();
  testsuit.test_save_load();

  delete(dc);
  graphlab::mpi_tools::finalize();
}

#include <graphlab/macros_undef.hpp>
