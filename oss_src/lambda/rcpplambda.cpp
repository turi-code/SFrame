/*
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <sframe/sframe_rows.hpp>
#include <fileio/fs_utils.hpp>
#include <util/cityhash_gl.hpp>

#include <lambda/rcpplambda.hpp>
#include <lambda/rcpplambda_utils.hpp>
#include <serialization/rcpp_serialization.hpp>

namespace graphlab {
namespace lambda {

rcpplambda_evaluator::rcpplambda_evaluator(RInside * R) {
    m_current_lambda_hash = (size_t)(-1);
    R_ptr = R;
};

size_t rcpplambda_evaluator::make_lambda(const std::string& lambda_str) {

    size_t hash_key = hash64(lambda_str.c_str(), lambda_str.size());
    std::vector<Rcpp::Function> fun_lst;
    std::vector<std::string> fun_names;
    std::vector<std::string> strs;

    boost::split(strs, lambda_str, boost::is_any_of("\n"));

    if (strs[0] != "") {
        std::string lib_to_load = "suppressMessages(lapply(" + strs[0] + ", require, character.only = TRUE))";
        m_lambda_lib_hash[hash_key] = lib_to_load;
    } else {
        m_lambda_lib_hash[hash_key] = "";
    }

    R_ptr->parseEvalQ("library('RApiSerialize')");

    for (size_t i = 1; i < strs.size() - 1; i = i + 2) {
        fun_lst.push_back(Rcpp::Function(unserializeFromStr(strs[i])));
        fun_names.push_back(strs[i + 1]);
    }

    m_lambda_hash[hash_key] = fun_lst;
    m_lambda_name_hash[hash_key] = fun_names;
    return hash_key;

}

void rcpplambda_evaluator::release_lambda(size_t lambda_hash) {

    auto lambda_obj = m_lambda_hash.find(lambda_hash);

    if (lambda_obj == m_lambda_hash.end()) {
        throw("Cannot find the lambda hash to release " + std::to_string(lambda_hash));
    }

    if (m_current_lambda_hash == lambda_hash) {
        m_current_lambda_hash = (size_t)(-1);
        m_current_lambda.clear();
    }

    m_lambda_hash.erase(lambda_hash);
    m_lambda_hash[lambda_hash].clear();
    m_lambda_name_hash.erase(lambda_hash);
    m_lambda_name_hash[lambda_hash].clear();
    m_lambda_lib_hash[lambda_hash] = "";

}

void rcpplambda_evaluator::set_lambda(size_t lambda_hash) {
    if (m_current_lambda_hash == lambda_hash) return;

    auto lambda_obj = m_lambda_hash.find(lambda_hash);

    if (lambda_obj == m_lambda_hash.end()) {
        throw("Cannot find a lambda handle that is value " + std::to_string(lambda_hash));
    }

    m_current_lambda = m_lambda_hash[lambda_hash];
    m_current_lambda_hash = lambda_hash;
}

// not implemented yet
std::vector<flexible_type> rcpplambda_evaluator::bulk_eval(size_t lambda_hash,
        const std::vector<flexible_type>& args, bool skip_undefined, int seed) {
    std::vector<flexible_type> res{0};

    return res;
}

// not implemented yet
std::vector<flexible_type> rcpplambda_evaluator::bulk_eval_rows(size_t lambda_hash,
        const sframe_rows& values, bool skip_undefined, int seed) {

    std::vector<flexible_type> res;

    return res;

}

// just use this right now
std::vector<flexible_type> rcpplambda_evaluator::bulk_eval_dict(size_t lambda_hash,
        const std::vector<std::string>& keys,
        const std::vector<std::vector<flexible_type>>& values,
        bool skip_undefined, int seed) {

    std::vector<flexible_type> res(1);

    std::vector<Rcpp::Function> fun_lst = m_lambda_hash[lambda_hash];

    std::vector<std::string> fun_names = m_lambda_name_hash[lambda_hash];

    for (size_t i = 0; i < fun_lst.size(); i++) {
        (*R_ptr)[fun_names[i]] = fun_lst[i];
    }

    std::vector<flexible_type> row = values[0];

    // parse a row into a Rcpp::List and evaluate teh function
    Rcpp::List lst(row.size());
    for (size_t i = 0; i < row.size(); i++) {
        flex_type_enum type = row[i].get_type();
        switch(type) {
        case flex_type_enum::STRING: {
            lst[i] = Rcpp::wrap(row[i].to<flex_string>());
            break;
        }
        case flex_type_enum::FLOAT: {
            lst[i] = Rcpp::wrap(row[i].to<flex_float>());
            break;
        }
        case flex_type_enum::INTEGER: {
            lst[i] = Rcpp::wrap(row[i].to<flex_int>());
            break;
        }
        case flex_type_enum::UNDEFINED: {
            lst[i] = NA_REAL;
            break;
        }
        }
    }

    if (keys.size() > 0 ) {
        lst.attr("names") = keys;
        (*R_ptr)["lst"] = lst;

        R_ptr->parseEvalQ(m_lambda_lib_hash[lambda_hash]);
        SEXP t = R_ptr->parseEval(fun_names[fun_lst.size() - 1] + "(lst)");
        res[0] = Rcpp::as<flexible_type>(t);
        return res;
    } else {
        (*R_ptr)["lst"] = lst[0];
        R_ptr->parseEvalQ(m_lambda_lib_hash[lambda_hash]);
        SEXP t = R_ptr->parseEval(fun_names[fun_lst.size() - 1] + "(lst)");
        res[0] = Rcpp::as<flexible_type>(t);
        return res;
    }

}

// not implemented yet
std::vector<flexible_type> rcpplambda_evaluator::bulk_eval_dict_rows(size_t lambda_hash,
        const std::vector<std::string>& keys,
        const sframe_rows& values,
        bool skip_undefined, int seed) {

    std::vector<flexible_type> res;

    return res;

}

std::string rcpplambda_evaluator::initialize_shared_memory_comm() {
    return "";
}

// not implemented yet
flexible_type rcpplambda_evaluator::eval(size_t lambda_hash, const flexible_type& arg) {
    return 123;
}


} // namespace lambda
} // namespace graphlab
