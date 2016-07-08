/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe_query_engine/execution/execution_node.hpp>
#include <sframe_query_engine/operators/sframe_source.hpp>
#include <sframe/sframe.hpp>
#include <sframe/algorithm.hpp>
#include <cxxtest/TestSuite.h>

#include "check_node.hpp"

using namespace graphlab;
using namespace graphlab::query_eval;

class sframe_source_test : public CxxTest::TestSuite {
 public:
  void test_empty_source() {
    sframe sf;
    std::vector<std::string> column_names;
    std::vector<flex_type_enum> column_types;
    sf.open_for_write(column_names, column_types);
    sf.close();
    auto node = make_node(sf);
    check_node(node, std::vector<std::vector<flexible_type>>());
  }

  void test_simple_sframe() {
    std::vector<std::vector<flexible_type>> expected {
      {0, "s0"}, {1, "s1"}, {2, "s2"}, {3, "s3"}, {4, "s4"}, {5, "s5"}
    };

    sframe sf;
    std::vector<std::string> column_names{"int", "string"};
    std::vector<flex_type_enum> column_types{flex_type_enum::INTEGER, flex_type_enum::STRING};
    sf.open_for_write(column_names, column_types);
    graphlab::copy(expected.begin(), expected.end(), sf);
    sf.close();
    auto node = make_node(sf);
    check_node(node, expected);
  }

 private:
  std::shared_ptr<execution_node> make_node(sframe source) {
    auto node = std::make_shared<execution_node>(std::make_shared<op_sframe_source>(source));
    return node;
  }

};
