/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe_query_engine/execution/execution_node.hpp>
#include <sframe_query_engine/operators/sarray_source.hpp>
#include <sframe_query_engine/operators/lambda_transform.hpp>
#include <sframe/sarray.hpp>
#include <sframe/algorithm.hpp>
#include <cxxtest/TestSuite.h>

#include "check_node.hpp"

using namespace graphlab;
using namespace graphlab::query_eval;

// TODO: get a pickle string for simple lambda 
const std::string IDENTITY_LAMBDA_STRING="";
const std::string PLUS_ONE_LAMBDA_STRING="";

class transform_test: public CxxTest::TestSuite {
 public:
  void test_identity_transform() {
    std::vector<flexible_type> expected {0,1,2,3,4,5};
    auto sa = std::make_shared<sarray<flexible_type>>();
    sa->open_for_write();
    graphlab::copy(expected.begin(), expected.end(), *sa);
    sa->close();
    auto node = make_node(op_sarray_source(sa),
                          IDENTITY_LAMBDA_STRING, flex_type_enum::INTEGER);
    check_node(node, expected);
  }

  void test_plus_one() {
    std::vector<flexible_type> data{0,1,2,3,4,5};
    auto sa = std::make_shared<sarray<flexible_type>>();
    sa->open_for_write();
    graphlab::copy(data.begin(), data.end(), *sa);
    sa->close();
    std::vector<flexible_type> expected(data.size());
    auto f = [](const flexible_type& val) { return val + 1; };
    std::transform(data.begin(), data.end(), expected.begin(), f);
    auto node = make_node(op_sarray_source(sa), PLUS_ONE_LAMBDA_STRING, flex_type_enum::INTEGER);
    check_node(node, expected);
  }

 private:
  std::shared_ptr<execution_node> make_node(const op_sarray_source& source,
                                            const std::string& lambda_str, flex_type_enum type) {
    auto lambda_fn = std::make_shared<lambda::pylambda_function>(lambda_str);
    auto source_node = std::make_shared<execution_node>(std::make_shared<op_sarray_source>(source));
    auto node = std::make_shared<execution_node>(std::make_shared<op_lambda_transform>(lambda_fn, type),
                                                 std::vector<std::shared_ptr<execution_node>>({source_node}));
    return node;
  }
};
