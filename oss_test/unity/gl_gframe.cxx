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
#include <cxxtest/TestSuite.h>
#include <boost/range/combine.hpp>
#include <unity/lib/gl_gframe.hpp>
#include <unity/lib/gl_sgraph.hpp>
#include <unity/lib/gl_sframe.hpp>

using namespace graphlab;

const flexible_type None = FLEX_UNDEFINED;

class gl_gframe_test: public CxxTest::TestSuite {

public:
  void test_empty_constructor() {
    gl_sgraph g;
    gl_sframe vertices = g.vertices();
    gl_sframe edges = g.edges();

    std::vector<flexible_type> empty;
    gl_sframe vertices_expected{{"__id", empty}};
    gl_sframe edges_expected{{"__src_id", empty}, {"__dst_id", empty}};

    _assert_sframe_equals(vertices, vertices_expected);
    _assert_sframe_equals(edges, edges_expected);
  }

  void test_constructor() {
    gl_sframe vertices{{"__id", {1,2,3}}};
    gl_sframe edges{{"__src_id", {1,2,3}}, {"__dst_id", {2,3,1}}};
    gl_sgraph g(vertices, edges, "__id", "__src_id", "__dst_id");

    gl_gframe gf_vertices = g.vertices();
    gl_gframe gf_edges = g.edges();

    _assert_sframe_equals(vertices, gf_vertices.sort("__id"));
    _assert_sframe_equals(edges, gf_edges.sort("__src_id"));
  }

  void test_vertex_gframe_binding() {
    gl_sframe vertices{{"__id", {1,2,3}}};
    gl_sframe edges{{"__src_id", {1,2,3}}, {"__dst_id", {2,3,1}}};

    gl_sgraph g(vertices, edges, "__id", "__src_id", "__dst_id");

    gl_gframe gf_vertices = g.vertices();
    gl_gframe gf_edges = g.edges();

    // add vertex field to graph 
    g.add_vertex_field(0, "zeros");
    _assert_sframe_equals(gf_vertices.sort("__id"), g.get_vertices().sort("__id"));

    // remove vertex field from graph 
    g.remove_vertex_field("zeros");
    _assert_sframe_equals(gf_vertices.sort("__id"), g.get_vertices().sort("__id"));

    // add a column to vertex gframe affects graph
    gf_vertices.add_column(1, "ones");
    _assert_sframe_equals(gf_vertices.sort("__id"), g.get_vertices().sort("__id"));

    // remove column from vertex gframe
    gf_vertices.remove_column("ones");
    _assert_sframe_equals(gf_vertices.sort("__id"), g.get_vertices().sort("__id"));

    // assign by sarray ref
    gf_vertices["id_copy"] = gf_vertices["__id"];
    vertices["id_copy"] = vertices["__id"];
    _assert_sframe_equals(gf_vertices.sort("__id"), vertices);
    _assert_sframe_equals(gf_vertices.sort("__id"), g.get_vertices().sort("__id"));

    // rename
    gf_vertices.rename({{"id_copy", "__id_copy"}});
    vertices.rename({{"id_copy", "__id_copy"}});
    _assert_sframe_equals(gf_vertices.sort("__id"), vertices);
    _assert_sframe_equals(gf_vertices.sort("__id"), g.get_vertices().sort("__id"));
  }

  void test_edge_gframe_binding() {
    gl_sframe vertices{{"__id", {1,2,3}}};
    gl_sframe edges{{"__src_id", {1,2,3}}, {"__dst_id", {2,3,1}}};

    gl_sgraph g(vertices, edges, "__id", "__src_id", "__dst_id");

    gl_gframe gf_vertices = g.vertices();
    gl_gframe gf_edges = g.edges();

    // add vertex field to graph 
    g.add_edge_field(0, "zeros");
    _assert_sframe_equals(gf_edges.sort({"__src_id", "__dst_id"}),
        g.get_edges().sort({"__src_id", "__dst_id"}));

    // remove vertex field from graph 
    g.remove_edge_field("zeros");
    _assert_sframe_equals(gf_edges.sort({"__src_id", "__dst_id"}),
        g.get_edges().sort({"__src_id", "__dst_id"}));

    // add a column to edge gframe affects graph 
    gf_edges.add_column(1, "ones");
    _assert_sframe_equals(gf_edges.sort({"__src_id", "__dst_id"}),
        g.get_edges().sort({"__src_id", "__dst_id"}));

    // remove column from edge gframe
    gf_edges.remove_column("ones");
    _assert_sframe_equals(gf_edges.sort({"__src_id", "__dst_id"}),
        g.get_edges().sort({"__src_id", "__dst_id"}));

    // assign by sarray ref
    gf_edges["id_copy"] = gf_edges["__src_id"];
    edges["id_copy"] = edges["__src_id"];
    _assert_sframe_equals(gf_edges.sort("__src_id"), edges);
    _assert_sframe_equals(gf_edges.sort({"__src_id", "__dst_id"}),
        g.get_edges().sort({"__src_id", "__dst_id"}));

    // rename
    gf_edges.rename({{"id_copy", "__src_id_copy"}});
    edges.rename({{"id_copy", "__src_id_copy"}});
    _assert_sframe_equals(gf_edges.sort("__src_id"), edges);
    _assert_sframe_equals(gf_edges.sort({"__src_id", "__dst_id"}),
        g.get_edges().sort({"__src_id", "__dst_id"}));
  }

private:
  void _assert_flexvec_equals(const std::vector<flexible_type>& sa, 
      const std::vector<flexible_type>& sb) {
    TS_ASSERT_EQUALS(sa.size(), sb.size());
    for (size_t i = 0;i < sa.size() ;++i) {
      TS_ASSERT_EQUALS(sa[i], sb[i]);
    }
  }

  void _assert_sframe_equals(gl_sframe sa, gl_sframe sb) {
    TS_ASSERT_EQUALS(sa.size(), sb.size());
    TS_ASSERT_EQUALS(sa.num_columns(), sb.num_columns());
    auto a_cols = sa.column_names();
    auto b_cols = sb.column_names();
    std::sort(a_cols.begin(), a_cols.end());
    std::sort(b_cols.begin(), b_cols.end());
    for (size_t i = 0;i < a_cols.size(); ++i) TS_ASSERT_EQUALS(a_cols[i], b_cols[i]);
    sb = sb[sa.column_names()];
    for (size_t i = 0; i < sa.size(); ++i) {
      _assert_flexvec_equals(sa[i], sb[i]);
    }
  }
};
