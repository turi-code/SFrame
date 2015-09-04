/*
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef _RCPP_SGRAPH_HPP
#define _RCPP_SGRAPH_HPP

#include <Rcpp.h>
#include <unity/lib/gl_sgraph.hpp>

class RcppSGraph {

  public:
    // default constructor
    RcppSGraph();

    // constructor from an R external pointer
    RcppSGraph(SEXP sxptr);

    RcppSGraph(SEXP vertices, SEXP edges, const std::string& vid,
               const std::string& src_id, const std::string& dst_id);

    void load(std::string & file);

    void show();

    SEXP get();

    void save(const std::string & file);

    SEXP num_vertices();

    SEXP num_edges();

    SEXP get_edges();

    SEXP get_vertices();

    SEXP get_fields();

    SEXP get_vertex_fields();

    SEXP get_edge_fields();

    SEXP add_vertices(SEXP vertices, const std::string& vid_field);

    SEXP add_edges(SEXP edges, const std::string& src_field,
                   const std::string& dst_field);

    SEXP select_vertex_fields(const std::vector<std::string>& fields);

    SEXP select_edge_fields(const std::vector<std::string>& fields);

    SEXP select_fields(const std::vector<std::string>& fields);

  private:

    graphlab::gl_sgraph sgraph;

};

#endif