/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe_query_engine/execution/execution_node.hpp>
#include <sframe_query_engine/operators/sarray_source.hpp>
#include <sframe_query_engine/operators/ternary_operator.hpp>
#include <sframe/sarray.hpp>
#include <sframe/algorithm.hpp>
#include <cxxtest/TestSuite.h>

#include "check_node.hpp"

using namespace graphlab;
using namespace graphlab::query_eval;

class ternary_operator_test: public CxxTest::TestSuite {
 public:
  void test_ternary() {
    std::vector<flexible_type> condition{0,1,0,1,0};
    std::vector<flexible_type> istrue{2,2,2,2,2};
    std::vector<flexible_type> isfalse{0,0,0,0,0};

    auto sa_condition = std::make_shared<sarray<flexible_type>>();
    sa_condition->open_for_write();
    graphlab::copy(condition.begin(), condition.end(), *sa_condition);
    sa_condition->close();

    auto sa_true = std::make_shared<sarray<flexible_type>>();
    sa_true->open_for_write();
    graphlab::copy(istrue.begin(), istrue.end(), *sa_true);
    sa_true->close();

    auto sa_false = std::make_shared<sarray<flexible_type>>();
    sa_false->open_for_write();
    graphlab::copy(isfalse.begin(), isfalse.end(), *sa_false);
    sa_false->close();

    std::vector<flexible_type> expected{0,2,0,2,0};
    auto node = make_node(op_sarray_source(sa_condition),
                          op_sarray_source(sa_true), 
                          op_sarray_source(sa_false));
    check_node(node, expected);
  }

 private:
  std::shared_ptr<execution_node> make_node(const op_sarray_source& condition,
                                            const op_sarray_source& source_true,
                                            const op_sarray_source& source_false) {
    auto condition_node = std::make_shared<execution_node>(std::make_shared<op_sarray_source>(condition));
    auto true_node = std::make_shared<execution_node>(std::make_shared<op_sarray_source>(source_true));
    auto false_node = std::make_shared<execution_node>(std::make_shared<op_sarray_source>(source_false));
    auto node = std::make_shared<execution_node>(std::make_shared<op_ternary_operator>(),
                             std::vector<std::shared_ptr<execution_node>>({condition_node, true_node, false_node}));
    return node;
  }
};
