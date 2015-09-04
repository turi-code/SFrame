/*
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef RCPP_LAMBDA_UTIL_HPP
#define RCPP_LAMBDA_UTIL_HPP

#include <RcppCommon.h>
#include "flexible_type/flexible_type.hpp"

typedef std::map<graphlab::flexible_type, graphlab::flexible_type> dict;

namespace Rcpp {

template <> graphlab::flexible_type as(SEXP value) ;

template <> SEXP wrap(const graphlab::flexible_type& value) ;

}

#endif
