/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe_query_engine/execution/execution_node.hpp>
#include <sframe_query_engine/operators/sframe_source.hpp>
#include <sframe_query_engine/operators/project.hpp>
#include <sframe/sframe.hpp>
#include <sframe/algorithm.hpp>
#include <cxxtest/TestSuite.h>

#include "check_node.hpp"

using namespace graphlab;
using namespace graphlab::query_eval;

class project_test : public CxxTest::TestSuite {
 public:
  void test_simple_case() {
    std::vector<std::vector<flexible_type>> data {
      {0, "s0"}, {1, "s1"}, {2, "s2"}, {3, "s3"}, {4, "s4"}, {5, "s5"}
    };
    std::vector<std::string> column_names{"int", "string"};
    std::vector<flex_type_enum> column_types{flex_type_enum::INTEGER, flex_type_enum::STRING};
    auto sf = make_sframe(column_names, column_types, data);

    std::vector<std::vector<size_t>> test_cases{{0}, {1}, {0, 1}, {1, 0}};
    for (auto& project_indices : test_cases) {
      std::vector<std::vector<flexible_type>> expected(data.size());
      std::transform(data.begin(), data.end(), expected.begin(),
                     [=](const std::vector<flexible_type>& row) {
                        std::vector<flexible_type> ret;
                        for (size_t i : project_indices)
                          ret.push_back(row[i]);
                        return ret;
                     });
      auto node = make_node(sf, project_indices);
      check_node(node, expected);
    }
  }

  void test_empty_sframe() {
    std::vector<std::vector<flexible_type>> data;
    std::vector<std::string> column_names{"int", "string"};
    std::vector<flex_type_enum> column_types{flex_type_enum::INTEGER, flex_type_enum::STRING};
    auto sf = make_sframe(column_names, column_types, data);

    std::vector<std::vector<size_t>> test_cases{{0}, {1}, {0, 1}, {1, 0}};
    for (auto& project_indices : test_cases) {
      std::vector<std::vector<flexible_type>> expected;
      auto node = make_node(sf, project_indices);
      check_node(node, expected);
    }
  }

  void test_project_out_of_bound() {
    // std::vector<std::vector<flexible_type>> data {
    //   {0, "s0"}, {1, "s1"}, {2, "s2"}, {3, "s3"}, {4, "s4"}, {5, "s5"}
    // };
    //
    // std::vector<std::string> column_names{"int", "string"};
    // std::vector<flex_type_enum> column_types{flex_type_enum::INTEGER, flex_type_enum::STRING};
    // auto sf = make_sframe(column_names, column_types, data);
    // auto node = make_node(sf, {2});
    // check_node_throws(node);
  }

 private:
  sframe make_sframe(const std::vector<std::string>& column_names,
                     const std::vector<flex_type_enum>& column_types,
                     const std::vector<std::vector<flexible_type>>& rows) {
    sframe sf;
    sf.open_for_write(column_names, column_types);
    graphlab::copy(rows.begin(), rows.end(), sf);
    sf.close();
    return sf;
  }

  std::shared_ptr<execution_node> make_node(sframe source, std::vector<size_t> project_indices) {
    auto source_node = std::make_shared<execution_node>(std::make_shared<op_sframe_source>(source));
    auto node = std::make_shared<execution_node>(std::make_shared<op_project>(project_indices),
                                                 std::vector<std::shared_ptr<execution_node>>({source_node}));
    return node;
  }
};
