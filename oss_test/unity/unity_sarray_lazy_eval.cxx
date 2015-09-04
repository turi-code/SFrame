/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <iostream>

#include <cxxtest/TestSuite.h>

#include <unistd.h>

#include <fileio/temp_files.hpp>
#include <unity/lib/unity_sarray.hpp>
using namespace graphlab;

class unity_sarray_lazy_eval_test: public CxxTest::TestSuite {
 const size_t ARRAY_SIZE = 1000000;

 public:
  unity_sarray_lazy_eval_test() {
    global_logger().set_log_level(LOG_FATAL);
  }

  /**
   * initial sarray construction is materialized
   **/
  void test_basic() {
    auto unity_array_ptr = construct_sarray(ARRAY_SIZE);
    assert_materialized(unity_array_ptr, true);
  }

  /**
   * scalar operator is lazily materialized.
   **/
  void test_left_scalar() {
    auto unity_array_ptr = construct_sarray(ARRAY_SIZE);

    auto u = unity_array_ptr->left_scalar_operator(2, "/");
    assert_materialized(u, false);
    TS_ASSERT_EQUALS(u->dtype(), flex_type_enum::FLOAT);

    // This will cause pipeline started, but not materialized
    auto max = u->max();
    assert_materialized(u, false);
    TS_ASSERT_EQUALS(max.get_type(), flex_type_enum::FLOAT);

    // second usage of the same tree will cause the operator to be materialized
    TS_ASSERT_EQUALS((float)max, (ARRAY_SIZE - 1)/(float)2);
  }

  /**
   * scalar operator is lazily materialized.
   **/
  void test_right_scalar() {
    auto unity_array_ptr = construct_sarray(ARRAY_SIZE);

    auto u = unity_array_ptr->right_scalar_operator(2, "/");
    assert_materialized(u, false);

    // This will cause pipeline started, but not materialized
    auto max = u->max();
    assert_materialized(u, false);

    auto min = u->min();
    assert_materialized(u, false);
  }

  /**
   * vector operator is lazily materialized.
   **/
  void test_vector_operator() {
    auto left = construct_sarray(ARRAY_SIZE);
    auto right = construct_sarray(ARRAY_SIZE);

    auto u = left->vector_operator(right, "+");
    assert_materialized(u, false);

    auto max = u->max();
    assert_materialized(u, false);
    auto min = u->min();
    assert_materialized(u, false);
  }

  /**
   * logical filter operator is lazily materialized.
   **/
  void test_logical_filter() {
    auto left = construct_sarray(ARRAY_SIZE);
    auto right = construct_sarray(ARRAY_SIZE);

    auto u = left->logical_filter(right);
    assert_materialized(u, false);

    // This will materialize the tree because max needs to know the size first and then run through the operation
    auto max = u->max();
    assert_materialized(u, true);
  }

  /**
   * Append operator is lazily materialized.
   **/
  void test_append() {
    auto sa1 = construct_sarray(ARRAY_SIZE);
    auto sa2 = construct_sarray(ARRAY_SIZE);

    assert_materialized(sa1, true);
    assert_materialized(sa2, true);

    auto u = sa1->append(sa2);
    assert_materialized(u, true);

    u = u->left_scalar_operator(1, "+");
    assert_materialized(u, false);

    u = u->append(u);
    assert_materialized(u, false);

    // Test disabled, not valid for the new query layer
    // auto max = u->max();
    // assert_materialized(u, false);
  }

  /**
   * combine the operators together
  **/
  void test_simple_pipeline() {
    size_t ARRAY_SIZE = 1000;

    std::vector<flexible_type> expected_result(ARRAY_SIZE);
    for(size_t i = 0; i < ARRAY_SIZE; i++) {
      expected_result[i] = (1000 - i) + (i -  2);
    }

    auto unity_array_ptr = construct_sarray(ARRAY_SIZE);

    auto u1 = unity_array_ptr->left_scalar_operator(2, "-");
    auto u2 = unity_array_ptr->right_scalar_operator(1000, "-");
    assert_materialized(u1, false);
    assert_materialized(u2, false);

    auto u_vector = u1->vector_operator(u2, "+");
    assert_materialized(u_vector, false);

    auto output = u_vector->_head((size_t)(-1));
    assert_materialized(u_vector, false);

    TS_ASSERT_EQUALS(output.size(), ARRAY_SIZE);
    for(size_t i = 0; i < ARRAY_SIZE; i++) {
      TS_ASSERT_EQUALS(expected_result[i], output[i]);
    }

    // stack a logical filter on top
    auto u3 = u_vector->logical_filter(u1);

    // append on top
    auto u4 = u3->append(u3);

    // another scalar op on top
    auto scalar_op =  u4->left_scalar_operator(4, "*");
    scalar_op->max();
  }

  /**
   * For cases like:
   *  t = some sarray
   *  t1 = t + 1
   *  t2 = t[t1]
   *  t3 = t + t2 <-- this operation causes the materialization of logical filter (t2)
   *  t3.max()
  **/
  void test_logical_filter_materialization() {
    auto t = construct_sarray(ARRAY_SIZE);
    auto t1 = t->left_scalar_operator(1, "+");
    auto t2 = t->logical_filter(t1);

    assert_materialized(t1, false);
    assert_materialized(t2, false);

    auto t3 = t->vector_operator(t2, "+");

    assert_materialized(t1, false);

    // logical filter gets materialized here as vector operator needs to ask for size to
    // make sure the operation is valid
    assert_materialized(t2, true);

    t3->max();
    assert_materialized(t3, false);
  }

  std::shared_ptr<unity_sarray_base> construct_sarray(size_t n) {
    std::vector<flexible_type> vec;
    std::shared_ptr<unity_sarray_base> array(new unity_sarray());

    for(size_t i = 0; i < n; i++) {
      vec.push_back(i);
    }

    array->construct_from_vector(vec, flex_type_enum::INTEGER);
    return array;
  }

  void assert_materialized(std::shared_ptr<unity_sarray_base> array_ptr, bool is_materialized) {
    TS_ASSERT_EQUALS(array_ptr->is_materialized(), is_materialized);
  }
};
