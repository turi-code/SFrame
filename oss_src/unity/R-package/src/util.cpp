
#include "util.hpp"


#include <Rcpp.h>

namespace Rcpp {

template <> graphlab::gl_sframe as(SEXP x) {
    SEXP tmp = R_NilValue;
    PROTECT(tmp = GET_SLOT(x, Rf_install("pointer")));
    Rcpp::XPtr<graphlab::gl_sframe> ptr(tmp);
    UNPROTECT(1);
    graphlab::gl_sframe s = *ptr.get();
    return s;
}

template <> graphlab::gl_sarray as(SEXP x) {
    SEXP tmp = R_NilValue;
    PROTECT(tmp = GET_SLOT(x, Rf_install("pointer")));
    Rcpp::XPtr<graphlab::gl_sarray> ptr(tmp);
    UNPROTECT(1);
    graphlab::gl_sarray s = *ptr.get();
    return s;
}

template <> graphlab::gl_sgraph as(SEXP x) {
    SEXP tmp = R_NilValue;
    PROTECT(tmp = GET_SLOT(x, Rf_install("pointer")));
    Rcpp::XPtr<graphlab::gl_sgraph> ptr(tmp);
    UNPROTECT(1);
    graphlab::gl_sgraph s = *ptr.get();
    return s;
}

template <> SEXP wrap(const graphlab::gl_sframe& sf) {
    SEXP xptr = Rcpp::XPtr<graphlab::gl_sframe>(new graphlab::gl_sframe(sf), true);
    SEXP mod_name = Rf_install("gl_sframe");
    Rcpp::Function new_sf("new");
    SEXP mod = new_sf(mod_name, xptr);
    SEXP res = new_sf("sframe");
    SET_SLOT(res, Rf_install("backend"), mod);
    SET_SLOT(res, Rf_install("names"), Rcpp::wrap(sf.column_names()));
    SET_SLOT(res, Rf_install("pointer"), xptr);    
    return res ;
}

template <> SEXP wrap(const graphlab::gl_sarray& sa) {
    SEXP xptr = Rcpp::XPtr<graphlab::gl_sarray>(new graphlab::gl_sarray(sa), true);
    SEXP mod_name = Rf_install("gl_sarray");
    Rcpp::Function new_sf("new");
    SEXP mod = new_sf(mod_name, xptr);
    SEXP res = new_sf("sarray");
    SET_SLOT(res, Rf_install("backend"), mod);
    SET_SLOT(res, Rf_install("pointer"), xptr);    
    return res ;
}

template <> SEXP wrap(const graphlab::gl_sgraph& sg) {
    SEXP xptr = Rcpp::XPtr<graphlab::gl_sgraph>(new graphlab::gl_sgraph(sg), true);
    SEXP mod_name = Rf_install("gl_sgraph");
    Rcpp::Function new_sf("new");
    SEXP mod = new_sf(mod_name, xptr);
    SEXP res = new_sf("sgraph");
    SET_SLOT(res, Rf_install("backend"), mod);
    SET_SLOT(res, Rf_install("pointer"), xptr);    
    return res ;
}

}
