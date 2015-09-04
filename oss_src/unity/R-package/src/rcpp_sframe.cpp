/*
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "rcpp_sframe.hpp"

#include <parallel/lambda_omp.hpp>
#include <unity/lib/gl_sarray.hpp>
#include <lambda/lambda_master.hpp>
#include <lambda/rcpplambda_utils.hpp>
#include <serialization/rcpp_serialization.hpp>

using namespace graphlab;

RcppSFrame::RcppSFrame() {
    sframe = gl_sframe();
}

// construct from an external pointer
RcppSFrame::RcppSFrame(SEXP sxptr) {
    Rcpp::XPtr<gl_sframe> ptr(sxptr);
    sframe = *ptr.get();
}

void RcppSFrame::load_from_sframe_index(const std::string& index_file) {
    sframe.construct_from_sframe_index(index_file);
}

// load from csv and parse all options from R
// we use R to guess the value type if not specify
void RcppSFrame::load_from_csvs(const std::string& csv_file,
                                const std::vector<std::string>& opt_map_key,
                                SEXP opt_map_value,
                                SEXP column_name,
                                SEXP column_type) {

    std::map<std::string, flexible_type> opt_map;

    Rcpp::List value_lst(opt_map_value);

    for (size_t i = 0; i < value_lst.size(); i++) {
        Rcpp::CharacterVector value(value_lst[i]);

        if (Rcpp::as<std::string>(value[0]) == "")
            continue;

        if (opt_map_key[i] == "output_columns" || opt_map_key[i] == "na.string") {
            std::vector<flexible_type> values(value.size());
            for (size_t j = 0; j < value.size(); j++) {
                values[j] = Rcpp::as<std::string>(value[j]);
            }
            opt_map[opt_map_key[i]] = values;

        } else {
            if (Rcpp::as<std::string>(value[0]) == "TRUE") {
                opt_map[opt_map_key[i]] = flexible_type(1);
            } else if (Rcpp::as<std::string>(value[0]) == "FALSE") {
                opt_map[opt_map_key[i]] = flexible_type(0);
            } else {
                opt_map[opt_map_key[i]] = Rcpp::as<std::string>(value[0]);
            }
        }
    }

    std::map<std::string, flex_type_enum> column_type_hints;

    if (column_type != NULL) {
        Rcpp::CharacterVector column_name_lst(column_name);
        Rcpp::CharacterVector column_type_lst(column_type);
        for (size_t i = 0; i < column_name_lst.size(); i++) {
            if (Rcpp::as<std::string>(column_type_lst[i]) == "integer") {
                column_type_hints[Rcpp::as<std::string>(column_name_lst[i])] =
                    flex_type_enum::INTEGER;
            } else if (Rcpp::as<std::string>(column_type_lst[i]) == "double") {
                column_type_hints[Rcpp::as<std::string>(column_name_lst[i])] =
                    flex_type_enum::FLOAT;
            } else if (Rcpp::as<std::string>(column_type_lst[i]) == "character") {
                column_type_hints[Rcpp::as<std::string>(column_name_lst[i])] =
                    flex_type_enum::STRING;
            } else {
                column_type_hints[Rcpp::as<std::string>(column_name_lst[i])] =
                    flex_type_enum::DICT;
            }
        }
    }

    sframe.construct_from_csvs(csv_file, opt_map, column_type_hints);
}

// as.sframe on a data.frame
// error if trying to convert factor type
void RcppSFrame::from_dataframe(Rcpp::DataFrame df) {

    std::map<std::string, std::vector<flexible_type> > data_map;

    Rcpp::CharacterVector colnames = df.names();
    for (size_t i = 0; i < colnames.size(); i++) {
        Rcpp::List data(df[i]);
        std::vector<flexible_type> values(data.size());
        int j = 0 ;
        for( Rcpp::List::iterator it = data.begin(); it != data.end(); ++it ) {
            switch( TYPEOF(*it) ) {
            case REALSXP: {
                double tmp = Rcpp::as<double>(*it);
                values[j] = tmp;
                break;
            }
            case INTSXP: {
                if( Rf_isFactor(*it) ) {
                    Rcpp::stop("Incompatible types. Try stringsAsFactors = FALSE when constructing the data.frame.");
                }
                int tmp = Rcpp::as<int>(*it);
                values[j] = tmp;
                break;
            }
            case LGLSXP: {
                // logical/boolean type will be converted into integer
                Rcpp::Rcout << "Logical type has been converted into int" << std::endl;
                bool tmp = Rcpp::as<bool>(*it);
                values[j] = tmp;
                break;
            }
            case STRSXP:
            case CHARSXP: {
                std::string tmp = Rcpp::as<std::string>(*it);
                values[j] = tmp;
                break;
            }
            default: {
                Rcpp::stop("incompatible types found");
            }
            }
            ++j;
        }
        data_map[Rcpp::as<std::string>(colnames[i])] = values;
    }
    sframe.construct_from_dataframe(data_map);
}

void RcppSFrame::from_sarray(SEXP xptr) {
    Rcpp::XPtr<gl_sarray> ptr(xptr);
    gl_sarray sarray = *ptr.get();
    std::map<std::string, gl_sarray> cols;
    cols["tmp"] = sarray;
    sframe = gl_sframe(cols);
}

size_t RcppSFrame::ncol() {
    sframe.num_columns();
}

size_t RcppSFrame::nrow() {
    sframe.size();
}

SEXP RcppSFrame::sample(double fraction, size_t seed) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.sample(fraction, seed)), true);
}

SEXP RcppSFrame::head(int n) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.head(n)), true);
}

SEXP RcppSFrame::tail(int n) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.tail(n)), true);
}

void RcppSFrame::save(const std::string& path, const std::string& format) {
    sframe.save(path, format);
}

std::vector<std::string> RcppSFrame::column_names() {
    return sframe.column_names();
}

void RcppSFrame::rename(const std::vector<std::string>& old_names,
                        const std::vector<std::string>& new_names) {
    std::map<std::string, std::string> old_to_new_names;
    for (size_t i = 0; i < old_names.size(); i++) {
        old_to_new_names[old_names[i]] = new_names[i];
    }
    sframe.rename(old_to_new_names);
}

SEXP RcppSFrame::select_columns(const std::vector<std::string>& colnames) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.select_columns(colnames)), true);
}

SEXP RcppSFrame::select_one_column(const std::string& colname) {
    return Rcpp::XPtr<gl_sarray>(new gl_sarray(sframe[colname]), true);
}

SEXP RcppSFrame::logical_filter(SEXP filter) {
    Rcpp::XPtr<gl_sarray> ptr(filter);
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe[*ptr.get()]), true);
}

void RcppSFrame::remove_column(const std::string& name) {
    sframe.remove_column(name);
}

Rcpp::DataFrame RcppSFrame::to_dataframe() {

    if (sframe.size() > 100000) {
        Rcpp::warning("converting large sframe");
    }

    std::vector<std::string> colnames = sframe.column_names();
    // List and DataFrame are the same except some attributes
    Rcpp::List df(colnames.size());
    size_t nrow = sframe.size();

    auto ra = sframe.range_iterator(0, 1);
    std::vector<flexible_type> row = *ra.begin();

    for (size_t i = 0; i < colnames.size(); i++) {
        flex_type_enum type = row[i].get_type();
        switch(type) {
        case flex_type_enum::STRING: {
            std::vector<std::string> data(nrow);
            for (size_t j = 0; j < nrow; j++) {
                ra = sframe.range_iterator(j, j + 1);
                row = * ra.begin();
                data[j] = row[i].to<flex_string>();
            }
            df[i] = Rcpp::wrap(data);
            break;
        }
        case flex_type_enum::FLOAT: {
            std::vector<double> data(nrow);
            for (size_t j = 0; j < nrow; j++) {
                ra = sframe.range_iterator(j, j + 1);
                row = * ra.begin();
                data[j] = row[i].to<flex_float>();
            }
            df[i] = Rcpp::wrap(data);
            break;
        }
        case flex_type_enum::INTEGER: {
            std::vector<int> data(nrow);
            for (size_t j = 0; j < nrow; j++) {
                ra = sframe.range_iterator(j, j + 1);
                row = * ra.begin();
                data[j] = row[i].to<flex_int>();
            }
            df[i] = Rcpp::wrap(data);
            break;
        }
        default: {
            Rcpp::stop("incompatible types found!");
            break;
        }
        }
    }

    // set the column names
    df.attr("names") = colnames;

    // make sure the string/character not converted into factor type
    // Rcpp internal functions used here, don't change unless really necessary
    SEXP as_df_symb = Rf_install("as.data.frame");
    SEXP strings_as_factors_symb = Rf_install("stringsAsFactors");
    Rcpp::Shield<SEXP> call(Rf_lang3(as_df_symb, df, Rcpp::wrap(false))) ;
    SET_TAG( CDDR(call), strings_as_factors_symb);
    Rcpp::Shield<SEXP> res(Rcpp_eval(call));
    Rcpp::DataFrame_Impl<Rcpp::PreserveStorage> out(res) ;
    return out ;

}

void RcppSFrame::show() {
    Rcpp::Rcout << sframe << std::endl;
}

bool RcppSFrame::empty() {
    return sframe.empty();
}

Rcpp::List RcppSFrame::random_split(double fraction, int seed) {
    std::pair<gl_sframe, gl_sframe> res = sframe.random_split(fraction, seed);
    // Rcpp::List::create is the bette way to construct a List
    return Rcpp::List::create(
               Rcpp::_["sframe1"]  = Rcpp::XPtr<gl_sframe>(new gl_sframe(res.first), true),
               Rcpp::_["sframe2"]  = Rcpp::XPtr<gl_sframe>(new gl_sframe(res.second), true));
}

// Rcpp::XPtr is heavily used for convenience and it is better way to handle external pointers
// you can think Rcpp::XPtr<gl_sframe> is the same with std::shared_ptr<gl_sframe>*
SEXP RcppSFrame::topk(const std::string& column_name, size_t k, bool reverse) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.topk(column_name, k, reverse)), true);
}

SEXP RcppSFrame::sortby(const std::string& column, bool ascending) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.sort(column, ascending)), true);
}

SEXP RcppSFrame::dropna(const std::vector<std::string>& columns,
                        const std::string& how) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.dropna(columns, how)), true);
}

// group by function for sframe
// here we parse the R command and translate into aggregate::groupby_descriptor_type
// better solution needed if possible
SEXP RcppSFrame::group_by(const std::vector<std::string>& keys,
                          const std::vector<std::string>& new_col_names, SEXP operators) {
    Rcpp::List op_lst(operators);

    std::map<std::string, aggregate::groupby_descriptor_type> group_by_operators;

    for (size_t i = 0; i < op_lst.size(); i++) {
        Rcpp::CharacterVector tmp(op_lst[i]);
        std::string op = Rcpp::as<std::string>(tmp[0]);
        if (op == "mean") {
            group_by_operators[new_col_names[i]] = aggregate::MEAN(Rcpp::as<std::string>(tmp[1]));
        } else if (op == "std") {
            group_by_operators[new_col_names[i]] = aggregate::STD(Rcpp::as<std::string>(tmp[1]));
        } else if (op == "sum") {
            group_by_operators[new_col_names[i]] = aggregate::SUM(Rcpp::as<std::string>(tmp[1]));
        } else if (op == "max") {
            group_by_operators[new_col_names[i]] = aggregate::MAX(Rcpp::as<std::string>(tmp[1]));
        } else if (op == "min") {
            group_by_operators[new_col_names[i]] = aggregate::MIN(Rcpp::as<std::string>(tmp[1]));
        } else if (op == "count") {
            group_by_operators[new_col_names[i]] = aggregate::COUNT();
        } else if (op == "var") {
            group_by_operators[new_col_names[i]] = aggregate::VAR(Rcpp::as<std::string>(tmp[1]));
        } else if (op == "select") {
            group_by_operators[new_col_names[i]] = aggregate::SELECT_ONE(Rcpp::as<std::string>(tmp[1]));
        } else if (op == "cat") {
            if (tmp.size() == 2) {
                group_by_operators[new_col_names[i]] = aggregate::CONCAT(Rcpp::as<std::string>(tmp[1]));
            } else {
                group_by_operators[new_col_names[i]] = aggregate::CONCAT(
                        Rcpp::as<std::string>(tmp[1]),
                        Rcpp::as<std::string>(tmp[2]));
            }
        } else if (op == "quantile") {
            std::vector<double> q = {0.25, 0.5, 0.75};
            group_by_operators[new_col_names[i]] = aggregate::QUANTILE(
                    Rcpp::as<std::string>(tmp[1]), q);
        } else if (op == "argmax") {
            group_by_operators[new_col_names[i]] = aggregate::ARGMAX(
                    Rcpp::as<std::string>(tmp[1]),
                    Rcpp::as<std::string>(tmp[2]));
        } else if (op == "argmin") {
            group_by_operators[new_col_names[i]] = aggregate::ARGMIN(
                    Rcpp::as<std::string>(tmp[1]),
                    Rcpp::as<std::string>(tmp[2]));
        }
    }

    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.groupby(keys, group_by_operators)), true);
}

// apply an R function over a sframe
// r_home here is used to help finding the rcpplambda_worker
SEXP RcppSFrame::apply(Rcpp::List fun_lst,
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

    std::vector< std::vector<flexible_type> > out_vec(sframe.size());

    std::vector<std::string> colnames = sframe.column_names();

    parallel_for(0, sframe.size(), [&](size_t i) {
        // don't use operator[] here!
        auto ra = sframe.range_iterator(i, i + 1);
        auto iter = ra.begin();
        std::vector<flexible_type> row = *iter;
        graphlab::lambda::lambda_master::get_instance().bulk_eval(lambda_hash,
                colnames, {row}, out_vec[i], false, 123);
    });

    std::vector<flexible_type> array(out_vec.size());

    for (size_t i = 0; i < out_vec.size(); i++) {
        array[i] = out_vec[i][0];
    }

    graphlab::lambda::lambda_master::get_instance().release_lambda(lambda_hash);

    return Rcpp::XPtr<gl_sarray>(new gl_sarray(array));
}

void RcppSFrame::add_column(SEXP sa, const std::string& name) {

    Rcpp::XPtr<gl_sarray> ptr(sa);
    gl_sarray * sarray_ptr = ptr.get();

    std::vector<std::string> colnames = sframe.column_names();

    if (std::find(colnames.begin(), colnames.end(), name)!=colnames.end()) {
        sframe.replace_add_column(*sarray_ptr, name);
    } else {
        sframe.add_column(*sarray_ptr, name);
    }

}

SEXP RcppSFrame::get() {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe));
}

SEXP RcppSFrame::cbind(SEXP xptr) {
    Rcpp::XPtr<gl_sframe> ptr(xptr);
    gl_sframe sframe_to_add = *ptr.get();
    gl_sframe res(sframe);
    res.add_columns(sframe_to_add);
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(res));
}

SEXP RcppSFrame::rbind(SEXP xptr) {
    Rcpp::XPtr<gl_sframe> ptr(xptr);
    gl_sframe sframe_to_add = *ptr.get();
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.append(sframe_to_add)));
}

SEXP RcppSFrame::unique() {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.unique()));
}

SEXP RcppSFrame::join(SEXP xptr, const std::vector<std::string>& join_keys,
                      const std::string& how) {
    Rcpp::XPtr<gl_sframe> ptr(xptr);
    gl_sframe sframe_to_join = *ptr.get();
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.join(sframe_to_join,
                                 join_keys, how)));
}

SEXP RcppSFrame::unpack(const std::string& unpack_column) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.unpack(unpack_column, "")));
}

SEXP RcppSFrame::pack(const std::vector<std::string>& columns,
                      const std::string& new_column_name,
                      const std::string& type, SEXP fill_na) {
    flex_type_enum dtype;

    if (type == "list") {
        dtype = flex_type_enum::LIST;
    } else if (type == "dict") {
        dtype = flex_type_enum::DICT;
    }

    if (TYPEOF(fill_na) == LGLSXP) {
        if (LOGICAL(fill_na)[0] == NA_LOGICAL) {
            return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.pack_columns(columns,
                                         new_column_name, dtype)));
        }
    }

    flexible_type tmp = Rcpp::as<graphlab::flexible_type>(fill_na) ;

    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.pack_columns(columns,
                                 new_column_name, dtype, tmp)));
}

SEXP RcppSFrame::stack(const std::string& column_name,
                       const std::vector<std::string>& new_column_names) {
    return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.stack(column_name,
                                 new_column_names)));
}

SEXP RcppSFrame::unstack(const std::vector<std::string>& column_names,
                         const std::string& new_column_name) {
    if (column_names.size() == 1) {
        return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.unstack(column_names[0],
                                     new_column_name)));
    } else {
        return Rcpp::XPtr<gl_sframe>(new gl_sframe(sframe.unstack(column_names,
                                     new_column_name)));
    }
}

// this is Rcpp module
// 1. syntax for .method is quite clear: 1st argument is the function name used in R, 2nd is the C++ function, 3rd is comments and optional
// 2 .constructor is to expose the constructor and the number of argumens is no more than 6
// 3 .method is the most common used to expose functions. Overload is allowed but not recommended
// 4 .const_method is a better choice in some situations
RCPP_MODULE(gl_sframe) {
    using namespace Rcpp;

    class_<RcppSFrame>("gl_sframe")
    .constructor( "Initialises a new Rccp Sframe object." )
    .constructor<SEXP>("Initialises a new Rccp Sframe object." )
    .method( "load", &RcppSFrame::load_from_sframe_index )
    .method( "ncol", &RcppSFrame::ncol )
    .method( "nrow", &RcppSFrame::nrow )
    .method( "sample", &RcppSFrame::sample )
    .method( "save", &RcppSFrame::save )
    .method( "colnames", &RcppSFrame::column_names )
    .method( "select_columns", &RcppSFrame::select_columns )
    .method( "select_one_column", &RcppSFrame::select_one_column )
    .method( "logical_filter", &RcppSFrame::logical_filter )
    .method( "show", &RcppSFrame::show )
    .method( "rename", &RcppSFrame::rename )
    .method( "empty", &RcppSFrame::empty )
    .method( "head", &RcppSFrame::head )
    .method( "tail", &RcppSFrame::tail )
    .method( "random_split", &RcppSFrame::random_split )
    .method( "topk", &RcppSFrame::topk )
    .method( "sortby", &RcppSFrame::sortby )
    .method( "remove_column", &RcppSFrame::remove_column )
    .method( "dropna", &RcppSFrame::dropna )
    .method( "group_by", &RcppSFrame::group_by )
    .method( "load_from_csvs", &RcppSFrame::load_from_csvs )
    .method( "from_dataframe", &RcppSFrame::from_dataframe )
    .method( "to_dataframe", &RcppSFrame::to_dataframe )
    .method( "from_sarray", &RcppSFrame::from_sarray )
    .method( "apply", &RcppSFrame::apply )
    .method( "add_column", &RcppSFrame::add_column )
    .method( "cbind", &RcppSFrame::cbind )
    .method( "rbind", &RcppSFrame::rbind )
    .method( "join", &RcppSFrame::join )
    .method( "unique", &RcppSFrame::unique )
    .method( "unpack", &RcppSFrame::unpack )
    .method( "pack", &RcppSFrame::pack )
    .method( "stack", &RcppSFrame::stack )
    .method( "unstack", &RcppSFrame::unstack )
    .method( "get", &RcppSFrame::get )
    ;
}

