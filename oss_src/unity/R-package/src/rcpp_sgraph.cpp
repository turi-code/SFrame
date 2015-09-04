/*
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "rcpp_sgraph.hpp"
#include <unity/lib/gl_sframe.hpp>

using namespace graphlab;

RcppSGraph::RcppSGraph() {
    sgraph = gl_sgraph();
}

// construct from an external pointer
RcppSGraph::RcppSGraph(SEXP sxptr) {
    Rcpp::XPtr<gl_sgraph> ptr(sxptr);
    sgraph = *ptr.get();
}

RcppSGraph::RcppSGraph(SEXP vertices, SEXP edges, const std::string& vid,
                       const std::string& src_id, const std::string& dst_id) {
    Rcpp::XPtr<gl_sframe> vptr(vertices);
    gl_sframe sframe_v = *vptr.get();
    Rcpp::XPtr<gl_sframe> eptr(edges);
    gl_sframe sframe_e = *eptr.get();
    sgraph = gl_sgraph(sframe_v, sframe_e, vid, src_id, dst_id);
}

void RcppSGraph::load(std::string & file) {
    sgraph = gl_sgraph(file);
}

SEXP RcppSGraph::get() {
    return Rcpp::XPtr<gl_sgraph>(new gl_sgraph(sgraph));
}

void RcppSGraph::show() {
    std::vector<std::string> v_fields = sgraph.get_vertex_fields();
    std::vector<std::string> e_fields = sgraph.get_edge_fields();
    Rcpp::Rcout << "Num of vertices: " << sgraph.num_vertices() << std::endl
                << "Num of edges: " << sgraph.num_edges() << std::endl;

    Rcpp::Rcout << "Vertex Fields:[";
    for (size_t i = 0 ; i < v_fields.size() - 1; i++) {
        Rcpp::Rcout << "'" << v_fields[i] << "', ";
    }
    Rcpp::Rcout << v_fields[v_fields.size() - 1] << "']" << std::endl;

    Rcpp::Rcout << "Edge Fields:[";
    for (size_t i = 0 ; i < e_fields.size() - 1; i++) {
        Rcpp::Rcout << "'" << e_fields[i] << "', ";
    }
    Rcpp::Rcout << e_fields[e_fields.size() - 1] << "']" << std::endl;
}

void RcppSGraph::save(const std::string & file) {
    sgraph.save(file);
}

SEXP RcppSGraph::num_vertices() {
    return Rcpp::wrap(sgraph.num_vertices());
}

SEXP RcppSGraph::num_edges() {
    return Rcpp::wrap(sgraph.num_edges());
}

SEXP RcppSGraph::get_edges() {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sgraph.get_edges()), true);
}

SEXP RcppSGraph::get_vertices() {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sgraph.get_vertices()), true);
}

SEXP RcppSGraph::get_fields() {
    return Rcpp::wrap(sgraph.get_fields());
}

SEXP RcppSGraph::get_vertex_fields() {
    return Rcpp::wrap(sgraph.get_vertex_fields());
}

SEXP RcppSGraph::get_edge_fields() {
    return Rcpp::wrap(sgraph.get_edge_fields());
}

SEXP RcppSGraph::add_vertices(SEXP vertices, const std::string& vid_field) {
    Rcpp::XPtr<gl_sframe> ptr(vertices);
    gl_sframe sframe = *ptr.get();
    gl_sgraph sg = sgraph.add_vertices(sframe, vid_field);
    return Rcpp::XPtr<gl_sgraph>(new gl_sgraph(sg), true);
}

SEXP RcppSGraph::add_edges(SEXP edges, const std::string& src_field,
                           const std::string& dst_field) {
    Rcpp::XPtr<gl_sframe> ptr(edges);
    gl_sframe sframe = *ptr.get();
    gl_sgraph sg = sgraph.add_edges(sframe, src_field, dst_field);
    return Rcpp::XPtr<gl_sgraph>(new gl_sgraph(sg), true);
}

SEXP RcppSGraph::select_vertex_fields(const std::vector<std::string>& fields) {
    return Rcpp::XPtr<gl_sgraph>(new gl_sgraph(sgraph.select_vertex_fields(fields)), true);
}

SEXP RcppSGraph::select_edge_fields(const std::vector<std::string>& fields) {
    return Rcpp::XPtr<gl_sgraph>(new gl_sgraph(sgraph.select_edge_fields(fields)), true);
}

SEXP RcppSGraph::select_fields(const std::vector<std::string>& fields) {
    return Rcpp::XPtr<gl_sgraph>(new gl_sgraph(sgraph.select_fields(fields)), true);
}

RCPP_MODULE(gl_sgraph) {
    using namespace Rcpp;

    class_<RcppSGraph>("gl_sgraph")
    .constructor("Initialises a new Rccp Sgraph object.")
    .constructor<SEXP>("Initialises a new Rccp Sgraph object.")
    .constructor<SEXP, SEXP, std::string, std::string, std::string>("Initialises a new Rccp Sgraph object.")
    .method( "load", &RcppSGraph::load)
    .method( "save", &RcppSGraph::save)
    .method( "get", &RcppSGraph::get)
    .method( "show", &RcppSGraph::show )
    .method( "num_vertices", &RcppSGraph::num_vertices )
    .method( "num_edges", &RcppSGraph::num_edges )
    .method( "get_edges", &RcppSGraph::get_edges )
    .method( "get_vertices", &RcppSGraph::get_vertices )
    .method( "get_fields", &RcppSGraph::get_fields )
    .method( "get_vertex_fields", &RcppSGraph::get_vertex_fields )
    .method( "get_edge_fields", &RcppSGraph::get_edge_fields )
    .method( "add_vertices", &RcppSGraph::add_vertices )
    .method( "add_edges", &RcppSGraph::add_edges )
    .method( "select_vertex_fields", &RcppSGraph::select_vertex_fields )
    .method( "select_edge_fields", &RcppSGraph::select_edge_fields )
    .method( "select_fields", &RcppSGraph::select_fields )
    ;
}

