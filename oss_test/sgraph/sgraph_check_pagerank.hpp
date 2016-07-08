/*
* Copyright (C) 2016 Turi
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
#ifndef GRAPHLAB_SGRAPH_TEST_PAGERANK_HPP
#define GRAPHLAB_SGRAPH_TEST_PAGERANK_HPP

#include <sgraph/sgraph.hpp>
#include "sgraph_test_util.hpp"

using namespace graphlab;

typedef std::function<void(sgraph&,  size_t)> pagerank_fn_type;

void check_pagerank(pagerank_fn_type compute_pagerank) {
  size_t n_vertex = 10;
  size_t n_partition = 2;
  {
    // for symmetic ring graph, all vertices should have the same pagerank
    sgraph ring_graph  = create_ring_graph(n_vertex, n_partition, false);
    compute_pagerank(ring_graph, 3);
    sframe vdata = ring_graph.get_vertices();
    size_t data_column_index = vdata.column_index("vdata");
    std::vector<std::vector<flexible_type>> vdata_buffer;
    vdata.get_reader()->read_rows(0, ring_graph.num_vertices(), vdata_buffer);
    for (auto& row : vdata_buffer) {
      TS_ASSERT_EQUALS(row[data_column_index], 1.0);
    }
  }
  {
    // for star graph, the center's pagerank = 0.15 + 0.85 * (n-1)) 
    sgraph star_graph = create_star_graph(n_vertex, n_partition);
    // star_graph.get_edges().debug_print();
    compute_pagerank(star_graph, 3);
    sframe vdata = star_graph.get_vertices();
    // vdata.debug_print();
    size_t id_column_index = vdata.column_index("__id");
    size_t data_column_index = vdata.column_index("vdata");
    std::vector<std::vector<flexible_type>> vdata_buffer;
    vdata.get_reader()->read_rows(0, star_graph.num_vertices(), vdata_buffer);
    for (auto& row : vdata_buffer) {
      if (row[id_column_index] == 0) {
        double expected = 0.15 + 0.85 * 0.15 * (n_vertex-1);
        TS_ASSERT_DELTA(row[data_column_index], expected, 0.0001);
      } else {
        TS_ASSERT_DELTA(row[data_column_index], 0.15, 0.0001);
      }
    }
  }
}
#endif
