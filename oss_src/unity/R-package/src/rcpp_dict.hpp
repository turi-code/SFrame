/*
 * Copyright (C) 2015 Dato, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef _RCPP_DICT_HPP
#define _RCPP_DICT_HPP

#include <flexible_type/flexible_type.hpp>
#include <Rcpp.h>

typedef std::map<graphlab::flexible_type, graphlab::flexible_type> rcpp_dict;

class RcppDict {

  public:

    RcppDict(SEXP lst) ;

    size_t length() ;

    void show() ;

    SEXP at(SEXP i);

    void set(SEXP i, SEXP val);

    std::vector< std::string > get_keys();

    SEXP get_values();

    SEXP get() ;

    void rm(SEXP col);

    bool has_key(SEXP k) ;

  private:

    rcpp_dict dict;

};


#endif