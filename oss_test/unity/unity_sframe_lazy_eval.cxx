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
#include <fileio/temp_files.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <sframe/dataframe.hpp>
#include <sframe/algorithm.hpp>
#include <cxxtest/TestSuite.h>
using namespace graphlab;

class unity_sframe_lazy_eval_test: public CxxTest::TestSuite {
  const size_t ARRAY_SIZE = 20000;

 private:
  dataframe_t _create_test_dataframe() {
    dataframe_t testdf;
    std::vector<flexible_type> a;
    std::vector<flexible_type> b;
    std::vector<flexible_type> c;
    // create a simple dataframe of 3 columns of 3 types
    for (size_t i = 0;i < ARRAY_SIZE; ++i) {
      a.push_back(i);
      b.push_back((float)i);
      c.push_back(std::to_string(i));
    }
    testdf.set_column("a", a, flex_type_enum::INTEGER);
    testdf.set_column("b", b, flex_type_enum::FLOAT);
    testdf.set_column("c", c, flex_type_enum::STRING);
    return testdf;
  }
 public:
  unity_sframe_lazy_eval_test() {
  }

  /**
   * initial sarray construction is materialized
   **/
  void test_basic() {
    dataframe_t testdf = _create_test_dataframe();
    // create a unity_sframe
    unity_sframe sframe;
    sframe.construct_from_dataframe(testdf);

    assert_materialized(sframe, true);
  }

  /**
   * Test logical filter
  **/
  void test_logical_filter() {
    dataframe_t testdf = _create_test_dataframe();
    unity_sframe sframe;
    sframe.construct_from_dataframe(testdf);

    // index array
    std::shared_ptr<unity_sarray_base> index_array(new unity_sarray);
    std::vector<flexible_type> vec(ARRAY_SIZE);
    for(size_t i = 0; i < ARRAY_SIZE; i++) {
      vec[i] = i % 2 == 0 ? 1 : 0;
    }
    index_array->construct_from_vector(vec, flex_type_enum::INTEGER);

    // logical filter
    auto new_sf = sframe.logical_filter(index_array);
    assert_materialized(new_sf, false);
  }

  /**
   * Test pipeline sframe and sarray without filter
  **/
  void test_pipe_line() {
    dataframe_t testdf = _create_test_dataframe();
    unity_sframe sframe;
    sframe.construct_from_dataframe(testdf);

    std::shared_ptr<unity_sarray_base> col_a = sframe.select_column(std::string("a"));
    std::shared_ptr<unity_sarray_base> col_b = sframe.select_column(std::string("b"));

    std::shared_ptr<unity_sarray_base> col_a_plus_b = col_a->vector_operator(col_b, "+");
    assert_materialized(col_a_plus_b, false);

    // construct new sframe
    unity_sframe new_sframe;

    new_sframe.add_column(col_b, std::string("a"));
    new_sframe.add_column(col_a_plus_b, std::string("ab"));
    assert_materialized(col_a_plus_b, false);
    assert_materialized(new_sframe, false);

    new_sframe.head(2);
    assert_materialized(new_sframe, false);
    assert_materialized(col_a_plus_b, false);

    new_sframe.tail(2);

    assert_materialized(new_sframe, true);

    // TODO These test do not pass yet
    assert_materialized(col_a, true);
    assert_materialized(col_b, true);

    // This test is no longer true with the new query layer
    // assert_materialized(col_a_plus_b, true);
  }

  /**
   * Test pipeline sframe and sarray with filter
   * filter will materialize some part of the tree that needs size
  **/
  void test_pipe_line_with_filter() {
    dataframe_t testdf = _create_test_dataframe();
    unity_sframe sframe;
    sframe.construct_from_dataframe(testdf);

    std::shared_ptr<unity_sarray_base> col_a = sframe.select_column(std::string("a"));
    std::shared_ptr<unity_sarray_base> col_b = sframe.select_column(std::string("b"));

    std::shared_ptr<unity_sarray_base> filter_a = col_a->logical_filter(col_b);
    assert_materialized(filter_a, false);

    // get size will cause materialization
    TS_ASSERT_EQUALS(filter_a->size(), ARRAY_SIZE - 1);
    assert_materialized(filter_a, true);
  }

  /**
   * Test sharing sarray object among different user
   * sf['one'] = sf['another'] = sa
   * sf[sf['a']]
  **/
  void test_share_operator() {
    dataframe_t testdf = _create_test_dataframe();
    unity_sframe sframe;
    sframe.construct_from_dataframe(testdf);

    std::shared_ptr<unity_sarray_base> col_a = sframe.select_column(std::string("a"));

    unity_sframe new_sframe;
    new_sframe.add_column(col_a, std::string("one"));
    new_sframe.add_column(col_a, std::string("another"));

    std::shared_ptr<unity_sframe_base> filtered_frame = new_sframe.logical_filter(col_a);
    filtered_frame->head(10);
  }

  void test_materialize_sframe() {
    unity_sframe sframe;

    // construct two columns in two different ways
    std::shared_ptr<unity_sarray_base> sa1(new unity_sarray);
    std::shared_ptr<unity_sarray_base> sa2(new unity_sarray);
    std::vector<flexible_type> vec1(100), vec2(100);
    for(size_t i = 0; i < 100; i++) {
      vec1[i] = i;
      vec2[i] = std::to_string(i);
    }

    sa1->construct_from_vector(vec1, flex_type_enum::INTEGER);
    sa2->construct_from_vector(vec2, flex_type_enum::STRING);

    // sa3 is lazily materialized
    auto sa3 = sa1->left_scalar_operator(1, "+");

    // cosntruct sframe
    std::shared_ptr<unity_sframe_base> sf(new unity_sframe);
    sf->add_column(sa2, "a");
    sf->add_column(sa3, "b");
    TS_ASSERT(sa1->is_materialized());
    TS_ASSERT(!sa3->is_materialized());
    TS_ASSERT(!sf->is_materialized());

    sf->materialize();
    TS_ASSERT(sf->is_materialized());
  }

  void assert_materialized(std::shared_ptr<unity_sframe_base> sframe_ptr, bool is_materialized) {
    TS_ASSERT_EQUALS(sframe_ptr->is_materialized(), is_materialized);
  }

  void assert_materialized(std::shared_ptr<unity_sarray_base> sarray_ptr, bool is_materialized) {
    TS_ASSERT_EQUALS(sarray_ptr->is_materialized(), is_materialized);
  }

  void assert_materialized(unity_sframe& sframe, bool is_materialized) {
    TS_ASSERT_EQUALS(sframe.is_materialized(), is_materialized);
  }

  void assert_materialized(unity_sarray& sarray, bool is_materialized) {
    TS_ASSERT_EQUALS(sarray.is_materialized(), is_materialized);
  }
};
