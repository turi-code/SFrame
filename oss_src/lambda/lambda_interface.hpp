/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_LAMBDA_INTERFACE_HPP
#define GRAPHLAB_LAMBDA_LAMBDA_INTERFACE_HPP
#include <flexible_type/flexible_type.hpp>
#include <cppipc/cppipc.hpp>
#include <sframe/sframe_rows.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {

namespace lambda {

enum class bulk_eval_serialized_tag:char {
  BULK_EVAL_ROWS = 0,
  BULK_EVAL_DICT_ROWS = 1,
};

GENERATE_INTERFACE_AND_PROXY(lambda_evaluator_interface, lambda_evaluator_proxy,
      (size_t, make_lambda, (const std::string&))
      (void, release_lambda, (size_t))
      (std::vector<flexible_type>, bulk_eval, (size_t)(const std::vector<flexible_type>&)(bool)(int))
      (std::vector<flexible_type>, bulk_eval_rows, (size_t)(const sframe_rows&)(bool)(int))
      (std::vector<flexible_type>, bulk_eval_dict, (size_t)(const std::vector<std::string>&)(const std::vector<std::vector<flexible_type>>&)(bool)(int))
      (std::vector<flexible_type>, bulk_eval_dict_rows, (size_t)(const std::vector<std::string>&)(const sframe_rows&)(bool)(int))
      (std::string, initialize_shared_memory_comm, )
    )
} // namespace lambda
} // namespace graphlab

#endif
