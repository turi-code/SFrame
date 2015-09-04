/*
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef _RCPP_SFRAME_HPP
#define _RCPP_SFRAME_HPP

#include <Rcpp.h>
#include <unity/lib/gl_sframe.hpp>

class RcppSFrame {

  public:
    // default constructor
    RcppSFrame();

    // constructor from an R external pointer
    RcppSFrame(SEXP sxptr);

    // load a binary SFrame saved previously
    void load_from_sframe_index(const std::string& index_file);

    // load a csv file
    void load_from_csvs(const std::string& csv_file,
                        const std::vector<std::string>& opt_map_key,
                        SEXP opt_map_value, SEXP column_name, SEXP column_type);

    // convert from R's data.frame, used for as.sframe
    // there might be warnings when converting between data.frame and sfram
    // different types of data are support in data.frame and sframe.
    // no factor and logical types in sframe
    void from_dataframe(Rcpp::DataFrame df);

    void from_sarray(SEXP xptr);

    // convert to R's data.frame, used for as.data.frame
    Rcpp::DataFrame to_dataframe();

    // save the sframe to file, binary or csv format
    void save(const std::string& path, const std::string& format);

    // number of columns
    size_t ncol();

    // number of rows
    size_t nrow();

    // sample from a sframe
    SEXP sample(double fraction, size_t seed);

    // first n row
    SEXP head(int n);

    // last n row
    SEXP tail(int n);

    // column names
    std::vector<std::string> column_names();

    // rename columns, old names and new names must have the same length
    void rename(const std::vector<std::string>& old_names,
                const std::vector<std::string>& new_names);

    // select columns by name to support '['
    SEXP select_columns(const std::vector<std::string>& colnames);

    // select only one column by name to support '[', a gl_sarray will be returned
    SEXP select_one_column(const std::string& colname);

    // remove column by name
    void remove_column(const std::string& name);

    // random split the sframe into two, a list of two sframes is returned
    Rcpp::List random_split(double fraction, int seed);

    // the top k rows by one column
    SEXP topk(const std::string& column_name, size_t k, bool reverse);

    // sort the sframe by one column
    SEXP sortby(const std::string& column, bool ascending);

    SEXP dropna(const std::vector<std::string>& columns, const std::string& how);

    // group by functions for sframe in R
    SEXP group_by(const std::vector<std::string>& groupkeys,
                  const std::vector<std::string>& new_col_names, SEXP operators);

    // apply an R function over a sframe
    // we need to pass the list of function/function names/libraries to load
    // R.home() is used to find the rcpplambda_worker
    SEXP apply(Rcpp::List funlst, const std::vector<std::string>& fun_names,
               const std::vector<std::string>& pkgs, const std::string& r_home);

    SEXP logical_filter(SEXP filter);

    // the show() function to be called when typing an object name in R console
    void show();

    // the sframe is empty or not
    bool empty();

    // add column function to support '[<-'
    void add_column(SEXP sa, const std::string& name);

    SEXP cbind(SEXP xptr);

    SEXP rbind(SEXP xptr);

    // return the underlying object as an external pointer
    SEXP get();

    SEXP unique();

    SEXP join(SEXP xptr, const std::vector<std::string>& join_keys,
              const std::string& how);

    SEXP unpack(const std::string& unpack_column);

    SEXP pack(const std::vector<std::string>& columns,
              const std::string& new_column_name, const std::string& type, SEXP fill_na);

    SEXP stack(const std::string& column_name,
               const std::vector<std::string>& new_column_names);

    SEXP unstack(const std::vector<std::string>& column_names,
                 const std::string& new_column_name);

  private:

    // the underlying gl_sframe
    graphlab::gl_sframe sframe;

};


#endif
