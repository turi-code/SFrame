
#ifndef __SKELETON_UTIL_
#define __SKELETON_UTIL_

#include <graphlab/sdk/gl_sframe.hpp>
#include <graphlab/sdk/gl_sarray.hpp>
#include <graphlab/sdk/gl_sgraph.hpp>

#include <Rdefines.h>
#include <RcppCommon.h>

namespace Rcpp {

template <> graphlab::gl_sframe as(SEXP x);

template <> graphlab::gl_sarray as(SEXP x);

template <> graphlab::gl_sgraph as(SEXP x);

template <> SEXP wrap(const graphlab::gl_sframe& sf) ;

template <> SEXP wrap(const graphlab::gl_sgraph& sf) ;

}

#endif