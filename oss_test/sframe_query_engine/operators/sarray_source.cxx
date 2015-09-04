/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe_query_engine/execution/execution_node.hpp>
#include <sframe_query_engine/operators/sarray_source.hpp>
#include <sframe/sarray.hpp>
#include <sframe/algorithm.hpp>
#include <cxxtest/TestSuite.h>

#include "check_node.hpp"

using namespace graphlab;
using namespace graphlab::query_eval;

class sarray_source_test : public CxxTest::TestSuite {
 public:
  void test_empty_source() {
    auto sa = std::make_shared<sarray<flexible_type>>();
    sa->open_for_write();
    sa->close();
    auto node = make_node(sa);
    check_node(node, std::vector<flexible_type>());
  }

  void test_simple_sarray() {
    std::vector<flexible_type> expected {0,1,2,3,4,5};
    auto sa = std::make_shared<sarray<flexible_type>>();
    sa->open_for_write();
    graphlab::copy(expected.begin(), expected.end(), *sa);
    sa->close();
    auto node = make_node(sa);
    check_node(node, expected);
  }

 private:
  std::shared_ptr<execution_node> make_node(std::shared_ptr<sarray<flexible_type>> source) {
    auto node = std::make_shared<execution_node>(std::make_shared<op_sarray_source>(source));
    return node;
  }

};
