/*
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_LAMBDA_RCPPAMBDA_EVALUATOR_HPP
#define GRAPHLAB_LAMBDA_RCPPAMBDA_EVALUATOR_HPP
#include <lambda/lambda_interface.hpp>
#include <flexible_type/flexible_type.hpp>
#include <string>
#include <RInside.h>

namespace graphlab {

class sframe_rows;

namespace lambda {

/**
 * \ingroup lambda
 *
 * A functor class wrapping a serialized R string.
 *
 * The lambda type is assumed to be either: S -> T or or List -> T.
 * where all types should be compatible  with flexible_type.
 *
 * \note: currently only bulk_eval_dict is implemented
 *
 * \internal
 * Internally, the class stores an RInside object and an R function will be created from the
 * serialized string upon construction.
 */

class rcpplambda_evaluator : public lambda_evaluator_interface {

  public:
    /**
     * Construct an evaluator from an RInside object
     */
    rcpplambda_evaluator(RInside * R) ;

    /**
     * Construct an empty evaluator.
     */
    rcpplambda_evaluator() {
        m_current_lambda_hash = (size_t)(-1);
    };

    /**
     * Sets the internal lambda from a serialized lambda string.
     *
     * Throws an exception if the construction failed.
     */
    size_t make_lambda(const std::string& lambda_str);

    /**
     * Release the cached lambda object
     */
    void release_lambda(size_t lambda_hash);

    // not implemented yet
    std::vector<flexible_type> bulk_eval(size_t lambda_hash, const std::vector<flexible_type>& args, bool skip_undefined, int seed);

    // not implemented yet
    std::vector<flexible_type> bulk_eval_rows(size_t lambda_hash,
            const sframe_rows& values, bool skip_undefined, int seed);


    /**
     * Convert the keys and values into a Rcpp::List and evaluate the R function
     */
    std::vector<flexible_type> bulk_eval_dict(size_t lambda_hash,
            const std::vector<std::string>& keys,
            const std::vector<std::vector<flexible_type>>& values,
            bool skip_undefined, int seed);

    // not implemented yet
    std::vector<flexible_type> bulk_eval_dict_rows(size_t lambda_hash,
            const std::vector<std::string>& keys,
            const sframe_rows& values,
            bool skip_undefined, int seed);

    std::string initialize_shared_memory_comm();

  private:

    // Set the lambda object for the next evaluation.
    void set_lambda(size_t lambda_hash);

    flexible_type eval(size_t lambda_hash, const flexible_type& arg);

    // the RInside object and R function object
    std::vector<Rcpp::Function> m_current_lambda;
    std::map<size_t, std::vector<Rcpp::Function> > m_lambda_hash;
    std::map<size_t, std::vector<std::string> > m_lambda_name_hash;
    std::map<size_t, std::string> m_lambda_lib_hash;
    size_t m_current_lambda_hash;
    RInside * R_ptr;

};

} // namespace lambda
} // namespace graphlab

#endif
