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

#include <lambda/rcpplambda.hpp>
#include <lambda/rcpplambda_utils.hpp>
#include "rcpp_dict.hpp"

using namespace graphlab;

RcppDict::RcppDict(SEXP sxp) {

    if (TYPEOF(sxp) == VECSXP) {
        Rcpp::List lst(sxp);
        Rcpp::CharacterVector names = lst.names();
        for (size_t i = 0; i < names.size(); i++) {
            dict[Rcpp::as<flexible_type>(names[i])] = Rcpp::as<flexible_type>(lst[i]);
        }

    } else if (TYPEOF(sxp) == EXTPTRSXP) {
        Rcpp::XPtr<rcpp_dict> ptr(sxp);
        dict = *ptr.get() ;
    }
}

size_t RcppDict::length() {
    return dict.size();
}

void RcppDict::show() {
    Rcpp::Rcout << "{ ";
    std::map<flexible_type, flexible_type>::iterator it;
    std::map<flexible_type, flexible_type>::iterator final_it = dict.end();
    final_it--;
    if (dict.size() <= 10) {
        for (it = dict.begin(); it != dict.end(); it++) {
            if (it != final_it) {
                Rcpp::Rcout << it->first << " : " << it->second << ", " ;
            } else {
                Rcpp::Rcout << it->first << " : " << it->second << " }"<< std::endl;
            }
        }
    } else {
        int i = 0;
        for (it = dict.begin(); it != dict.end(); it++) {
            i++;
            if (i < 10) {
                Rcpp::Rcout << it->first << " : " << it->second << ", " ;
            } else {
                Rcpp::Rcout << " ...... }"<< std::endl;
                break;
            }
        }
    }

}

SEXP RcppDict::at(SEXP i) {
    return Rcpp::wrap(dict[Rcpp::as<flexible_type>(i)]);
}

void RcppDict::set(SEXP i, SEXP val) {
    dict[Rcpp::as<flexible_type>(i)] = Rcpp::as<flexible_type>(val);
}

std::vector< std::string > RcppDict::get_keys() {
    std::vector< std::string > keys;
    transform(dict.begin(), dict.end(), back_inserter(keys), [](rcpp_dict::value_type& val) {
        return val.first;
    } );
    return keys;
}


SEXP RcppDict::get_values() {
    std::vector< flexible_type > values;
    transform(dict.begin(), dict.end(), back_inserter(values), [](rcpp_dict::value_type& val) {
        return val.second;
    } );
    return Rcpp::wrap(values);
}

SEXP RcppDict::get() {
    return Rcpp::XPtr<rcpp_dict>(new rcpp_dict(dict), true) ;
}

void RcppDict::rm(SEXP col) {
    flexible_type col_name = Rcpp::as<flexible_type>(col);
    dict.erase(col_name);
}

bool RcppDict::has_key(SEXP k) {
    flexible_type key = Rcpp::as<flexible_type>(k);
    return dict.find(key) != dict.end();
}

RCPP_MODULE(gl_dict) {
    using namespace Rcpp;

    class_<RcppDict>("gl_dict")
    .constructor<SEXP>("Initialises a new dict object from a list")
    .method( "length", &RcppDict::length )
    .method( "show", &RcppDict::show )
    .method( "at", &RcppDict::at )
    .method( "set", &RcppDict::set )
    .method( "get", &RcppDict::get )
    .method( "get_keys", &RcppDict::get_keys )
    .method( "get_values", &RcppDict::get_values )
    .method( "has_key", &RcppDict::has_key )
    .method( "rm", &RcppDict::rm )
    ;
}