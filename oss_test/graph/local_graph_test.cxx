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
#include <cxxtest/TestSuite.h>

// includes the entire graphlab framework
#include <graph/local_graph.hpp>
#include <graph/dynamic_local_graph.hpp>
#include <random/random.hpp>
#include <graphlab/macros_def.hpp>

/**
 * Unit test for graphlab::local_graph.hpp
 */
class local_graph_test : public CxxTest::TestSuite {
public:
  struct vertex_data {
    size_t value;
    vertex_data() : value(0) { }
    vertex_data(size_t n) : value(n) { }
  };

  struct edge_data { 
    int from; 
    int to;
    edge_data (int f = 0, int t = 0) : from(f), to(t) {}
  };

  /**
   * Test add vertex and add edges
   */
  void test_add_vertex() {
    graphlab::local_graph<vertex_data, edge_data> g;
    test_add_vertex_impl(g, 100);
    test_add_vertex_impl(g, 10000);
    test_add_vertex_impl(g, 100000);
    std::cout << "\n+ Pass test: graph add vertex. :) \n";

    graphlab::dynamic_local_graph<vertex_data, edge_data> g2;
    test_add_vertex_impl(g2, 100);
    test_add_vertex_impl(g2, 10000);
    test_add_vertex_impl(g2, 100000);
    std::cout << "\n+ Pass test: dynamic graph add vertex. :) \n";
  }

  void test_add_edge() {
    graphlab::local_graph<vertex_data, edge_data> g;
    test_add_edge_impl(g, 100);
    test_add_edge_impl(g, 10000);
    test_add_edge_impl(g, 100000);
    std::cout << "\n+ Pass test: graph add edge. :) \n";
    graphlab::dynamic_local_graph<vertex_data, edge_data> g2;
    test_add_edge_impl(g2, 100);
    test_add_edge_impl(g2, 10000);
    test_add_edge_impl(g2, 100000);
      std::cout << "\n+ Pass test: dynamic graph add edge. :) \n";
  }

  void test_dynamic_add_edge() {
    graphlab::dynamic_local_graph<vertex_data, edge_data> g2;
    test_add_edge_impl(g2, 100, true); // add edge dynamically
    test_add_edge_impl(g2, 10000, true);
    test_add_edge_impl(g2, 100000, true);
    std::cout << "\n+ Pass test: graph dynamicly add edge. :) \n";
  }

  void test_powerlaw_graph() {
    graphlab::local_graph<vertex_data, edge_data> g;
    graphlab::dynamic_local_graph<vertex_data, edge_data> g2;
    test_powerlaw_graph_impl(g, 100); // add edge (powerlaw) 
    test_powerlaw_graph_impl(g, 10000);

    test_powerlaw_graph_impl(g2, 100); // add edge (powerlaw) 
    test_powerlaw_graph_impl(g2, 10000);

    test_powerlaw_graph_impl(g2, 100, true); // add edge (powerlaw) dynamically
    test_powerlaw_graph_impl(g2, 10000, true);
    std::cout << "\n+ Pass test: powerlaw graph add edge. :) \n";
  }

  void test_edge_case() {
    graphlab::local_graph<vertex_data, edge_data> g;
    test_edge_case_impl(g);
    std::cout << "\n+ Pass test: edge case test. :) \n";

    graphlab::dynamic_local_graph<vertex_data, edge_data> g2;
    test_edge_case_impl(g2);
    std::cout << "\n+ Pass test: dynamic graph edge case test. :) \n";
  }


  void test_sparse_graph() {
    graphlab::local_graph<vertex_data, edge_data> g;
    test_sparse_graph_impl(g);
    std::cout << "\n+ Pass test: sparse graph test. :) \n";

    graphlab::dynamic_local_graph<vertex_data, edge_data> g2;
    test_sparse_graph_impl(g2);
    std::cout << "\n+ Pass test: sparse dyanmic graph test. :) \n";
  }

  void test_grid_graph() {
    graphlab::local_graph<vertex_data, edge_data> g;
    test_grid_graph_impl(g);
    std::cout << "\n+ Pass test: grid graph test. :) \n";

    graphlab::dynamic_local_graph<vertex_data, edge_data> g2;
    test_grid_graph_impl(g2);
    std::cout << "\n+ Pass test: grid dynamic graph test. :) \n";
  }

private: 
  template<typename Graph>
  void test_add_vertex_impl(Graph& g, size_t nverts) {
    g.clear();
    TS_ASSERT_EQUALS(g.num_vertices(), 0);
    for (size_t i = 0; i < nverts; ++i) {
      g.add_vertex(i, vertex_data(i));
    }
    TS_ASSERT_EQUALS(g.num_vertices(), nverts); 
    for (size_t i = 0; i < g.num_vertices(); ++i) {
      TS_ASSERT_EQUALS(g.vertex(i).data().value, i);
    }
    g.finalize();
    TS_ASSERT_EQUALS(g.num_vertices(), nverts);

    // graph should still support adding vertices after finalization
    // add more vertices and override existing vertex values
    for (size_t i = 0; i < 2*nverts; ++i) {
      g.add_vertex(i, vertex_data(i*2));
    }
    TS_ASSERT_EQUALS(g.num_vertices(), 2*nverts);
    for (size_t i = 0; i < g.num_vertices(); ++i) {
      TS_ASSERT_EQUALS(g.vertex(i).data().value, 2*i);
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
    typedef typename Graph::edge_list_type edge_list_type;
    typedef typename Graph::edge_type edge_type;
    typedef typename Graph::vertex_type vertex_type;
    typedef typename Graph::vertex_id_type vertex_id_type;

    // check size 
    TS_ASSERT_EQUALS(g.num_edges(), nedges);

    size_t nedges_actual = 0;
    // check out edges
    typedef typename boost::unordered_map<vertex_id_type, std::vector<vertex_id_type> >::iterator iter_type;
    for (iter_type it = out_edges.begin(); it != out_edges.end(); ++it) {
      vertex_id_type src = it->first;
      std::set<vertex_id_type> dst_expected = std::set<vertex_id_type>(it->second.begin(), it->second.end());
      const edge_list_type& ls = g.out_edges(src);
      foreach (const edge_type& e, ls) {
        TS_ASSERT_EQUALS(e.source().id(), src);
        ASSERT_TRUE(dst_expected.count(e.target().id()) == 1);
        dst_expected.erase(e.target().id());
      }
      nedges_actual += ls.size();
    }
    TS_ASSERT_EQUALS(nedges_actual, g.num_edges());
    TS_ASSERT_EQUALS(nedges_actual, nedges);

    nedges_actual = 0;
    // check in edges
    for (iter_type it = in_edges.begin(); it != in_edges.end(); ++it) {
      vertex_id_type dst = it->first;
      std::set<vertex_id_type> src_expected = std::set<vertex_id_type>(it->second.begin(), it->second.end());
      const edge_list_type& ls = g.in_edges(dst);
      foreach (const edge_type& e, ls) {
        TS_ASSERT_EQUALS(e.target().id(), dst);
        ASSERT_TRUE(src_expected.count(e.source().id()) == 1);
        src_expected.erase(e.source().id());
      }
      nedges_actual += ls.size();
    }
    TS_ASSERT_EQUALS(nedges_actual, g.num_edges());
    TS_ASSERT_EQUALS(nedges_actual, nedges);
  }

template<typename Graph>
  void check_edge_data(Graph& g) {
      typedef typename Graph::edge_list_type edge_list_type;
      typedef typename Graph::edge_type edge_type;
      typedef typename Graph::vertex_type vertex_type;
      typedef typename Graph::vertex_id_type vertex_id_type;
    for (size_t i = 0; i < g.num_vertices(); ++i) {
      const edge_list_type& in_edges = g.in_edges(i);
      foreach (const edge_type& e, in_edges) {
        TS_ASSERT_EQUALS(e.data().from, e.source().id());
        TS_ASSERT_EQUALS(e.data().to, e.target().id());
      }
      const edge_list_type& out_edges = g.out_edges(i);
      foreach (const edge_type& e, out_edges) {
        TS_ASSERT_EQUALS(e.data().from, e.source().id());
        TS_ASSERT_EQUALS(e.data().to, e.target().id());
      }
    }
  } 

  template<typename Graph>
  void test_add_edge_impl(Graph& g, size_t nedges, bool use_dynamic=false) {
    typedef typename Graph::vertex_id_type vertex_id_type;
    srand(0);
    g.clear();
    TS_ASSERT_EQUALS(g.num_edges(), 0);
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
    size_t count = 0;
    foreach (const pair_type& p, all_edges) {
      g.add_edge(p.first, p.second, edge_data(p.first, p.second));
      ++count;
      if (use_dynamic && (all_edges.size()/5) == 0) {
        g.finalize();
      }
    }
    if (!use_dynamic)
      TS_ASSERT_EQUALS(g.num_edges(), 0); 
    g.finalize();
    check_adjacency(g, in_edges, out_edges, all_edges.size());
    check_edge_data(g);
  }

  
  template<typename Graph>
  void test_edge_case_impl(Graph& g) {
    // TODO: 
    // self edges
    // duplicate edges
    std::cout << "Warning: test not implemented" << std::endl;
  }

  /**
   * Construct a star like sparse graph and test the in/out neighbors.
   */
  template<typename Graph>
  void test_sparse_graph_impl (Graph& g) {
    typedef typename Graph::edge_list_type edge_list_type;
    typedef typename Graph::edge_type edge_type;
    typedef typename Graph::vertex_type vertex_type;
    typedef typename Graph::vertex_id_type vertex_id_type;

    size_t num_v = 10;
    size_t num_e = 6;

    for (size_t i = 0; i < num_v; ++i) {
      vertex_data vdata;
      g.add_vertex(vertex_id_type(i), vdata);
    }

    /**
     * Create a star graph.
     */
    g.add_edge(1,3,edge_data(1,3));
    g.add_edge(2,3,edge_data(2,3));
    g.add_edge(4,3,edge_data(4,3));
    g.add_edge(5,3,edge_data(5,3));
    g.add_edge(3,2, edge_data(3,2));
    g.add_edge(3,5, edge_data(3,5));
    g.finalize();

    TS_ASSERT_EQUALS(g.num_vertices(), num_v);
    TS_ASSERT_EQUALS(g.num_edges(), num_e);

    /**
     * Test number of in/out edges.
     */
    for (vertex_id_type i = 0; i < 6; ++i) {
      edge_list_type inedges = g.in_edges(i);
      edge_list_type outedges = g.out_edges(i);
      size_t arr_insize[] = {0,0,1,4,0,1};
      size_t arr_outsize[] = {0,1,1,2,1,1};
      if (i != 3) {
        TS_ASSERT_EQUALS(inedges.size(), arr_insize[i]);
        TS_ASSERT_EQUALS(outedges.size(), arr_outsize[i]);
        if (outedges.size() > 0)
          {
            TS_ASSERT_EQUALS(outedges[0].source().id(), i);
            TS_ASSERT_EQUALS(outedges[0].target().id(), 3);
            edge_data data = (outedges[0]).data();
            TS_ASSERT_EQUALS(data.from, i);
            TS_ASSERT_EQUALS(data.to, 3);
          }
      } else {
        std::set<vertex_id_type> out_neighbors;
        out_neighbors.insert(5);
        out_neighbors.insert(2);
        TS_ASSERT_EQUALS(outedges.size(), out_neighbors.size());
        for (size_t j = 0; j < 2; ++j) {
          edge_data data = (outedges[j]).data();
          TS_ASSERT_EQUALS(data.from, 3);
          ASSERT_TRUE(out_neighbors.count(data.to) == 1);
          out_neighbors.erase(data.to);
        }

        std::set<vertex_id_type> in_neighbors;
        in_neighbors.insert(5);
        in_neighbors.insert(4);
        in_neighbors.insert(2);
        in_neighbors.insert(1);
        TS_ASSERT_EQUALS(inedges.size(), in_neighbors.size());
        for (size_t j = 0; j < 4; ++j) {
          edge_data data = (inedges[j]).data();
          TS_ASSERT_EQUALS(data.to, 3);
          ASSERT_TRUE(in_neighbors.count(data.from) == 1);
          in_neighbors.erase(data.from);
        }
      }
    }

    for (vertex_id_type i = 6; i < num_v; ++i) {
      edge_list_type inedges = g.in_edges(i);
      edge_list_type outedges = g.out_edges(i);
      TS_ASSERT_EQUALS(0, inedges.size());
      TS_ASSERT_EQUALS(0, outedges.size());
    }
  }

  /**
     In this function, we construct the 3 by 3 grid graph.
  */
  template<typename Graph>
  void test_grid_graph_impl(Graph& g, bool verbose = false) {
    typedef typename Graph::edge_list_type edge_list_type;
    typedef typename Graph::edge_type edge_type;
    typedef typename Graph::vertex_type vertex_type;
    typedef typename Graph::vertex_id_type vertex_id_type;

    g.clear();
    if (verbose) 
      std::cout << "-----------Begin Grid Test: ID Accessors--------------------" << std::endl;
    size_t dim = 3;
    size_t num_vertices = 0;
    size_t num_edge = 0;


    // here we create dim * dim vertices.
    for (size_t i = 0; i < dim * dim; ++i) {
      // create the vertex data, randomizing the color
      vertex_data vdata;
      vdata.value = 0;
      // create the vertex
      g.add_vertex(vertex_id_type(i), vdata);
      ++num_vertices;
    }

    // create the edges. The add_edge(i,j,edgedata) function creates
    // an edge from i->j. with the edgedata attached.   edge_data edata;

    for (size_t i = 0;i < dim; ++i) {
      for (size_t j = 0;j < dim - 1; ++j) {
        // add the horizontal edges in both directions
        //
        g.add_edge(dim * i + j, dim * i + j + 1, edge_data(dim*i+j, dim*i+j+1));
        g.add_edge(dim * i + j + 1, dim * i + j, edge_data(dim*i+j+1, dim*i+j));

        // add the vertical edges in both directions
        g.add_edge(dim * j + i, dim * (j + 1) + i, edge_data(dim*j+i, dim*(j+1)+i));
        g.add_edge(dim * (j + 1) + i, dim * j + i, edge_data(dim*(j+1)+i, dim*j+i));
        num_edge += 4;
      }
    }

    // the graph is now constructed
    // we need to call finalize. 
    g.finalize();

    if (verbose) printf("Test num_vertices()...\n");
    TS_ASSERT_EQUALS(g.num_vertices(), num_vertices);
    if (verbose) printf("+ Pass test: num_vertices :)\n\n");

    if (verbose) printf("Test num_edges()...\n");
    TS_ASSERT_EQUALS(g.num_edges(), num_edge);
    if (verbose) printf("+ Pass test: num_edges :)\n\n");

    // Symmetric graph: #inneighbor == outneighbor
    if (verbose) printf("Test num_in_neighbors() == num_out_neighbors() ...\n");
    for (size_t i = 0; i < num_vertices; ++i) {
      TS_ASSERT_EQUALS(g.in_edges(i).size(), g.vertex(i).num_in_edges());
      TS_ASSERT_EQUALS(g.out_edges(i).size(), g.vertex(i).num_out_edges());
      TS_ASSERT_EQUALS(g.in_edges(i).size(), g.out_edges(i).size());
    }
    TS_ASSERT_EQUALS(g.in_edges(4).size(), 4);
    TS_ASSERT_EQUALS(g.in_edges(0).size(), 2);
    if (verbose) printf("+ Pass test: #in = #out...\n\n");


    if (verbose) 
      printf("Test iterate over in/out_edges and get edge data: \n");
    for (vertex_id_type i = 0; i < num_vertices; ++i) {
      const edge_list_type& out_edges = g.out_edges(i);
      const edge_list_type& in_edges = g.in_edges(i);

      if (verbose) {
        std::cout << "Test v: " << i << "\n"
                  << "In edge ids: ";
        foreach(edge_type edge, in_edges) 
            std::cout << "(" << edge.data().from << ","
                      << edge.data().to << ") ";
        std::cout <<std::endl;

        std::cout << "Out edge ids: ";
        foreach(edge_type edge, out_edges) 
            std::cout << "(" << edge.data().from << "," 
                      << edge.data().to << ") ";
        std::cout <<std::endl;
      }

      foreach(edge_type edge, out_edges) {
        edge_data edata = edge.data();
        TS_ASSERT_EQUALS(edge.source().id(), i);
        TS_ASSERT_EQUALS(edata.from, edge.source().id());
        TS_ASSERT_EQUALS(edata.to, edge.target().id());
      }

      foreach(edge_type edge, in_edges) {
        edge_data edata = edge.data();
        TS_ASSERT_EQUALS(edge.target().id(), i);
        TS_ASSERT_EQUALS(edata.from, edge.source().id());
        TS_ASSERT_EQUALS(edata.to, edge.target().id());
      }
    }
    if (verbose)
      printf("+ Pass test: iterate edgelist and get data. :) \n");

    for (vertex_id_type i = 0; i < num_vertices; ++i) {
      vertex_type v = g.vertex(i);
      const edge_list_type& out_edges = v.out_edges();
      const edge_list_type& in_edges = v.in_edges();

      if (verbose) {
        std::cout << "Test v: " << i << std::endl;
        printf("In edge ids: ");
        foreach(edge_type edge, in_edges) 
            std::cout << "(" << edge.data().from << ","
                      << edge.data().to << ") ";
        std::cout <<std::endl;

        printf("Out edge ids: ");
        foreach(edge_type edge, out_edges) 
            std::cout << "(" << edge.data().from << "," 
                      << edge.data().to << ") ";
        std::cout <<std::endl;
      }

      foreach(edge_type edge, out_edges) {
        edge_data edata = edge.data();
        TS_ASSERT_EQUALS(edge.source().id(), i);
        TS_ASSERT_EQUALS(edata.from, edge.source().id());
        TS_ASSERT_EQUALS(edata.to, edge.target().id());
      }

      foreach(edge_type edge, in_edges) {
        edge_data edata = edge.data();
        TS_ASSERT_EQUALS(edge.target().id(), i);
        TS_ASSERT_EQUALS(edata.from, edge.source().id());
        TS_ASSERT_EQUALS(edata.to, edge.target().id());
      }
    }
    if (verbose) {
      printf("+ Pass test: iterate edgelist and get data. :) \n");
      std::cout << "-----------End Grid Test--------------------" << std::endl;
    }
  }

  /**
   * Test powerlaw graph.
   */
  template<typename Graph>
  void test_powerlaw_graph_impl(Graph& g, size_t nverts, bool use_dynamic = false, double alpha = 2.1) {
    graphlab::random::seed(0);
    g.clear();
    typedef typename Graph::edge_list_type edge_list_type;
    typedef typename Graph::edge_type edge_type;
    typedef typename Graph::vertex_type vertex_type;
    typedef typename Graph::vertex_id_type vertex_id_type;

    boost::unordered_map<vertex_id_type, std::vector<vertex_id_type> > out_edges;
    boost::unordered_map<vertex_id_type, std::vector<vertex_id_type> > in_edges;
    boost::unordered_set< std::pair<vertex_id_type,vertex_id_type> > all_edges;

      // construct powerlaw out degree distribution 
      std::vector<double> prob(nverts, 0);
      for(size_t i = 0; i < prob.size(); ++i)
        prob[i] = std::pow(double(i+1), -alpha);
      graphlab::random::pdf2cdf(prob);

      vertex_id_type dst = 0;

      // A large prime number
      const size_t HASH_OFFSET = 2654435761;
      // construct powerlaw graph with no dup edges
      for(vertex_id_type src  = 0; src < nverts; ++src) {
        const size_t out_degree = graphlab::random::multinomial_cdf(prob) + 1;
        for(size_t i = 0; i < out_degree; ++i) {
          dst = (dst + HASH_OFFSET)  % nverts;
          while (src == dst) {
            dst = (dst + HASH_OFFSET)  % nverts;
          }
          std::pair<vertex_id_type, vertex_id_type> pair(src, dst);
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
      }

    typedef typename boost::unordered_set< std::pair<vertex_id_type, vertex_id_type> >::value_type pair_type; 
    size_t count = 0;
    foreach (const pair_type& p, all_edges) {
      g.add_edge(p.first, p.second, edge_data(p.first, p.second));
      ++count;
      if (use_dynamic && count % (all_edges.size()/5) == 0) {
        g.finalize();
      }
    }
    if (!use_dynamic)
      TS_ASSERT_EQUALS(g.num_edges(), 0); 
    g.finalize();
    check_adjacency(g, in_edges, out_edges, all_edges.size());
    check_edge_data(g);
  }
};

#include <graphlab/macros_undef.hpp>
