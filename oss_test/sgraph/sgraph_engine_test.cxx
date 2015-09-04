/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <sgraph/sgraph.hpp>
#include <sgraph/sgraph_engine.hpp>
#include <cxxtest/TestSuite.h>

#include "sgraph_test_util.hpp"
#include "sgraph_check_degree_count.hpp"
#include "sgraph_check_pagerank.hpp"

using namespace graphlab;

// Implement degree count function using sgraph_engine
std::vector<std::pair<flexible_type, flexible_type>> degree_count_fn (
  sgraph& g, sgraph::edge_direction dir) {

  sgraph_compute::sgraph_engine<flexible_type> ga;
  typedef sgraph_compute::sgraph_engine<flexible_type>::graph_data_type graph_data_type;
  typedef sgraph::edge_direction edge_direction;
  std::vector<std::shared_ptr<sarray<flexible_type>>> 
      gather_results = ga.gather(g,
                                 [](const graph_data_type& center, 
                                    const graph_data_type& edge, 
                                    const graph_data_type& other, 
                                    edge_direction edgedir,
                                    flexible_type& combiner) {
                                   combiner = combiner + 1;
                                 },
                                 flexible_type(0),
                                 dir);
  std::vector<std::shared_ptr<sarray<flexible_type>>> vertex_ids 
      = g.fetch_vertex_data_field(sgraph::VID_COLUMN_NAME);

  TS_ASSERT_EQUALS(gather_results.size(), vertex_ids.size());
  std::vector<std::pair<flexible_type, flexible_type>> ret;

  for (size_t i = 0;i < gather_results.size(); ++i) {
    std::vector<flexible_type> degree_vec;
    std::vector<flexible_type> id_vec;
    gather_results[i]->get_reader()->read_rows(0, g.num_vertices(), degree_vec);
    vertex_ids[i]->get_reader()->read_rows(0, g.num_vertices(), id_vec);
    TS_ASSERT_EQUALS(degree_vec.size(), id_vec.size());
    for (size_t j = 0; j < degree_vec.size(); ++j) {
      ret.push_back({id_vec[j], degree_vec[j]});
    }
  }
  return ret;
}

// Implement degree count function using sgraph_engine
void pagerank_fn(sgraph& g,  size_t num_iterations) {
  sgraph_compute::sgraph_engine<flexible_type> ga;
  typedef sgraph_compute::sgraph_engine<flexible_type>::graph_data_type graph_data_type;
  typedef sgraph::edge_direction edge_direction;
  // count the outgoing degree
  std::vector<std::shared_ptr<sarray<flexible_type>>> vertex_combine = ga.gather(g,
                                                      [](const graph_data_type& center, 
                                                         const graph_data_type& edge, 
                                                         const graph_data_type& other, 
                                                         edge_direction edgedir,
                                                         flexible_type& combiner) {
                                                      combiner = combiner + 1;
                                                      },
                                                      flexible_type(0),
                                                      edge_direction::OUT_EDGE);
  // merge the outgoing degree to graph
  std::vector<sframe>& vdata = g.vertex_group();
  for (size_t i = 0; i < g.get_num_partitions(); ++i) {
    ASSERT_LT(i, vdata.size());
    ASSERT_LT(i, vertex_combine.size());
    vdata[i] = vdata[i].add_column(vertex_combine[i], "__out_degree__");
  }

  size_t degree_idx = vdata[0].column_index("__out_degree__");
  size_t data_idx = vdata[0].column_index("vdata");

  // now we compute the pagerank
  for (size_t iter = 0; iter < num_iterations; ++iter) {
    vertex_combine = ga.gather(g,
        [=](const graph_data_type& center,
            const graph_data_type& edge,
            const graph_data_type& other,
            edge_direction edgedir,
            flexible_type& combiner) {
           combiner = combiner + 0.85 * (other[data_idx] / other[degree_idx]);
        },
        flexible_type(0.15),
        edge_direction::IN_EDGE);
    for (size_t i = 0; i < g.get_num_partitions(); ++i) {
      vdata[i] = vdata[i].replace_column(vertex_combine[i], "vdata");
    }
    // g.get_vertices().debug_print();
  }
}

class sgraph_engine_test: public CxxTest::TestSuite {

public:
 void test_degree_count() {
   check_degree_count(degree_count_fn);
 }

 void test_pagerank() {
   check_pagerank(pagerank_fn);
 }

};
