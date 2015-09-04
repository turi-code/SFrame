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

#ifndef _RCPP_SARRAY_HPP
#define _RCPP_SARRAY_HPP

// macro for operator between sarray and sarray/flex type
#define SARRAY_OP(op)                                                                    \
SEXP operator op (SEXP xptr) {                                                           \
    if (TYPEOF(xptr) == EXTPTRSXP) {                                                     \
        Rcpp::XPtr<graphlab::gl_sarray> ptr(xptr);                                       \
        graphlab::gl_sarray s2 = *ptr.get();                                             \
        return Rcpp::XPtr<graphlab::gl_sarray>(new graphlab::gl_sarray(sarray op s2));   \
    } else {                                                                             \
        graphlab::flexible_type value = Rcpp::as<graphlab::flexible_type>(xptr);         \
        return Rcpp::XPtr<graphlab::gl_sarray>(new graphlab::gl_sarray(sarray op value));\
    }                                                                                    \
}

// macro for operator only between sarray and sarray
#define SARRAY_OP2(op)                                                             \
SEXP operator op (SEXP xptr) {                                                     \
    Rcpp::XPtr<graphlab::gl_sarray> ptr(xptr);                                     \
    graphlab::gl_sarray s2 = *ptr.get();                                           \
    return Rcpp::XPtr<graphlab::gl_sarray>(new graphlab::gl_sarray(sarray op s2)); \
}


#include <Rcpp.h>
#include <unity/lib/gl_sarray.hpp>

class RcppSArray {

  public:

    // default constructor
    RcppSArray();

    // constructor from an external pointer
    RcppSArray(SEXP sxptr);

    RcppSArray(size_t start, size_t end);

    // the show() function to be called when typing an object name in R console
    void show();

    // return the underlying object as an external pointer
    SEXP get();

    // convert from R's vector, used for as.sarray
    // there might be warnings when converting between vector and sarray
    void from_vec(SEXP vec);

    // convert to R's vector, used for as.vector
    SEXP to_vec();

    SEXP operator[](int i) const;

    SARRAY_OP(+)
    SARRAY_OP(-)
    SARRAY_OP(*)
    SARRAY_OP(/)
    SARRAY_OP(==)
    SARRAY_OP(>)
    SARRAY_OP(>=)
    SARRAY_OP(<)
    SARRAY_OP(<=)
    SARRAY_OP(+=)
    SARRAY_OP(-=)
    SARRAY_OP(*=)
    SARRAY_OP(/=)
    SARRAY_OP2(&&)
    SARRAY_OP2(||)

    SEXP minus(SEXP xptr);

    size_t length();

    SEXP head(int n);

    SEXP tail(int n);

    SEXP min(bool rm_na);

    SEXP max(bool rm_na);

    SEXP sum(bool rm_na);

    SEXP mean();

    SEXP std(bool rm_na);

    SEXP unique();

    SEXP sort(bool decreasing);

    void save(const std::string& path, const std::string& format);

    SEXP sample(double fraction, size_t seed);

    void from_dict_list(Rcpp::List lst) ;

    void from_list(Rcpp::List lst) ;

    SEXP apply(Rcpp::List funlst, const std::vector<std::string>& fun_names,
               const std::vector<std::string>& pkgs, const std::string& r_home);

    SEXP astype(const std::string& type);

  private:

    graphlab::gl_sarray sarray;

};

#endif