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

#include <lambda/rcpplambda_utils.hpp>
#include "rcpp_sarray.hpp"
#include "rcpp_dict.hpp"
#include <parallel/lambda_omp.hpp>
#include <lambda/lambda_master.hpp>
#include <serialization/rcpp_serialization.hpp>

using namespace graphlab ;

RcppSArray::RcppSArray() {
    sarray = gl_sarray();
}

RcppSArray::RcppSArray(SEXP sxptr) {
    Rcpp::XPtr<gl_sarray> ptr(sxptr);
    sarray = *ptr.get();
}

RcppSArray::RcppSArray(size_t start, size_t end) {
    sarray = gl_sarray::from_sequence(start, end);
}

// the show() function to be called when typing an object name in R console
void RcppSArray::show() {
    Rcpp::Rcout << sarray << std::endl;
}

SEXP RcppSArray::get() {
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray));
}

void RcppSArray::from_vec(SEXP vec) {
    std::vector<flexible_type> values = Rcpp::as<flexible_type>(vec);
    sarray.construct_from_vector(values);
}

SEXP RcppSArray::to_vec() {

    auto ra = sarray.range_iterator();
    auto iter = ra.begin();
    flexible_type values = *iter;
    flex_type_enum type = values.get_type();
    int i = 0;
    switch(type) {
    case flex_type_enum::STRING: {
        std::vector<std::string> res(sarray.size());
        while (iter != ra.end()) {
            res[i] = (*iter).to<flex_string>();
            ++iter;
            ++i;
        }
        return Rcpp::wrap(res);
    }
    case flex_type_enum::FLOAT: {
        std::vector<double> res(sarray.size());
        while (iter != ra.end()) {
            res[i] = (*iter).to<flex_float>();
            ++iter;
            ++i;
        }
        return Rcpp::wrap(res);
    }
    case flex_type_enum::INTEGER: {
        std::vector<int> res(sarray.size());
        while (iter != ra.end()) {
            res[i] = (*iter).to<flex_int>();
            ++iter;
            ++i;
        }
        return Rcpp::wrap(res);
    }
    default: {
        Rcpp::stop("Incompatible types found!");
        break;
    }
    }
}

SEXP RcppSArray::operator[](int i) const {
    auto ra = sarray.range_iterator(i);
    auto iter = ra.begin();
    flexible_type value = *iter;
    return Rcpp::wrap(value);
}

SEXP RcppSArray::minus(SEXP xptr) {
    flexible_type value = Rcpp::as<flexible_type>(xptr);
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(value - sarray));
}

SEXP RcppSArray::head(int n) {
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.head(n)), true);
}

SEXP RcppSArray::tail(int n) {
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.tail(n)), true);
}

void RcppSArray::from_dict_list(Rcpp::List lst) {

    std::vector<flexible_type> f_vec(lst.size());
    for (size_t i = 0; i < lst.size(); i++) {
        Rcpp::XPtr<rcpp_dict> ptr((SEXP)lst[i]);
        rcpp_dict d = *ptr.get() ;
        flex_dict f_dict;
        for(auto imap: d) {
            f_dict.push_back(std::make_pair(imap.first, imap.second)) ;
        }
        f_vec[i] = f_dict;
    }
    sarray = gl_sarray(f_vec) ;
}

void RcppSArray::from_list(Rcpp::List lst) {
    std::vector<flexible_type> f_vec = Rcpp::as<flexible_type>(lst) ;
    sarray = gl_sarray(f_vec) ;
}

size_t RcppSArray::length() {
    return sarray.size();
}

SEXP RcppSArray::min(bool rm_na) {
    if (rm_na) {
        gl_sarray tmp = sarray.dropna();
        return Rcpp::wrap(tmp.min());
    } else {
        return Rcpp::wrap(sarray.min());
    }
}

SEXP RcppSArray::max(bool rm_na) {
    if (rm_na) {
        gl_sarray tmp = sarray.dropna();
        return Rcpp::wrap(tmp.max());
    } else {
        return Rcpp::wrap(sarray.max());
    }
}

SEXP RcppSArray::sum(bool rm_na) {
    if (rm_na) {
        gl_sarray tmp = sarray.dropna();
        return Rcpp::wrap(tmp.sum());
    } else {
        return Rcpp::wrap(sarray.sum());
    }
}

SEXP RcppSArray::mean() {
    return Rcpp::wrap(sarray.mean());
}

SEXP RcppSArray::std(bool rm_na) {
    if (rm_na) {
        gl_sarray tmp = sarray.dropna();
        return Rcpp::wrap(tmp.std());
    } else {
        return Rcpp::wrap(sarray.std());
    }
}

SEXP RcppSArray::unique() {
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.unique()), true);
}

SEXP RcppSArray::sort(bool decreasing) {
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.sort(!decreasing)), true);
}

void RcppSArray::save(const std::string& path, const std::string& format) {
    sarray.save(path, format);
}

SEXP RcppSArray::sample(double fraction, size_t seed) {
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.sample(fraction, seed)), true);
}

SEXP RcppSArray::apply(Rcpp::List fun_lst,
                       const std::vector<std::string>& fun_names,
                       const std::vector<std::string>& pkgs, const std::string& r_home) {

    std::string pkgs_str = "";

    if (pkgs[0] != "") {
        pkgs_str = "c(";
        for (size_t i = 0; i < pkgs.size() - 1; i++) {
            if (pkgs[i] != "" ) {
                pkgs_str += "'" + pkgs[i] + "',";
            }
        }

        pkgs_str += "'" + pkgs[pkgs.size() - 1] + "')";
    }

    // serialize a Rcpp::Function to std::string
    // get the Rcpp::Function by Rcpp::Function fun(unserializeFromStr(test));
    std::string f_str = "";

    for (int i = fun_lst.size() - 1; i >= 0; i--) {
        Rcpp::Function f(fun_lst[i]);
        f_str = f_str + serializeToStr(f) + "\n" + fun_names[i] + "\n";
    }

    // set the binary path for lambda worker
    graphlab::lambda::lambda_master::set_lambda_worker_binary(
        r_home + "/sframe/rcpplambda_worker");

    auto lambda_hash =
        graphlab::lambda::lambda_master::get_instance().make_lambda(pkgs_str + "\n" + f_str);

    std::vector< std::vector<flexible_type> > out_vec(sarray.size());

    std::vector<std::string> colnames;

    parallel_for(0, sarray.size(), [&](size_t i) {
        auto ra = sarray.range_iterator(i, i + 1);
        auto iter = ra.begin();
        flexible_type val = *iter;
        graphlab::lambda::lambda_master::get_instance().bulk_eval(lambda_hash,
        colnames, {{val}}, out_vec[i], false, 123);
    });

    std::vector<flexible_type> array(out_vec.size());

    for (size_t i = 0; i < out_vec.size(); i++) {
        array[i] = out_vec[i][0];
    }

    graphlab::lambda::lambda_master::get_instance().release_lambda(lambda_hash);

    return Rcpp::XPtr<gl_sarray>(new gl_sarray(array));
}

SEXP RcppSArray::astype(const std::string& type) {//astype(flex_type_enum dtype
    if (type == "int") {
        return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.astype(flex_type_enum::INTEGER)));
    } else if (type == "vector") {
        return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.astype(flex_type_enum::VECTOR)));
    } else if (type == "list") {
        return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.astype(flex_type_enum::LIST)));
    } else if (type == "dict") {
        return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.astype(flex_type_enum::DICT)));
    } else if (type == "double") {
        return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.astype(flex_type_enum::FLOAT)));
    } else if (type == "string") {
        return Rcpp::XPtr<gl_sarray>(new gl_sarray(sarray.astype(flex_type_enum::STRING)));
    }
}

RCPP_MODULE(gl_sarray) {
    using namespace Rcpp;

    class_<RcppSArray>("gl_sarray")
    .constructor("Initialises a new Rccp Sarray object.")
    .constructor<SEXP>("Initialises a new Rccp Sarray object.")
    .constructor<size_t, size_t>("Initialises a new Rccp Sarray object.")
    .method( "show", &RcppSArray::show)
    .method( "get", &RcppSArray::get)
    .method( "from_vec", &RcppSArray::from_vec )
    .method( "to_vec", &RcppSArray::to_vec )
    .method( "at", &RcppSArray::operator[] )
    .method( "op_add", &RcppSArray::operator+ )
    .method( "op_minus", &RcppSArray::operator- )
    .method( "minus", &RcppSArray::minus )
    .method( "op_multiply", &RcppSArray::operator* )
    .method( "op_divide", &RcppSArray::operator/ )
    .method( "op_equal", &RcppSArray::operator== )
    .method( "op_greater", &RcppSArray::operator> )
    .method( "op_less", &RcppSArray::operator< )
    .method( "op_geq", &RcppSArray::operator>= )
    .method( "op_leq", &RcppSArray::operator<= )
    .method( "op_and", &RcppSArray::operator<= )
    .method( "op_or", &RcppSArray::operator<= )
    .method( "length", &RcppSArray::length )
    .method( "head", &RcppSArray::head )
    .method( "tail", &RcppSArray::tail )
    .method( "min", &RcppSArray::min )
    .method( "max", &RcppSArray::max )
    .method( "sum", &RcppSArray::sum )
    .method( "mean", &RcppSArray::mean )
    .method( "std", &RcppSArray::std )
    .method( "unique", &RcppSArray::unique )
    .method( "sort", &RcppSArray::sort )
    .method( "save", &RcppSArray::save )
    .method( "sample", &RcppSArray::sample )
    .method( "from_dict_list", &RcppSArray::from_dict_list )
    .method( "from_list", &RcppSArray::from_list )
    .method( "apply", &RcppSArray::apply )
    .method( "astype", &RcppSArray::astype )
    ;
}
