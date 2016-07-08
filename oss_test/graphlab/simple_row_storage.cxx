/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <vector>
#include <graphlab/util/simple_row_storage.hpp>
#include <graphlab/serialization/serialization_includes.hpp>
#include <sstream>
#include <graphlab/util/cityhash_gl.hpp>

using namespace graphlab;

static const size_t test_size = 100;

// Generic function to test saving, loading, and equality. 
template <typename SRS> void _test_equal(const SRS& data_1, const SRS& data_2) {

  // Test equality.
  TS_ASSERT(data_1 == data_2); 
  TS_ASSERT(!(data_1 != data_2)); 

  // Make sure the equality is actually good.
  TS_ASSERT_EQUALS(data_1.size(), data_2.size()); 
  TS_ASSERT_EQUALS(data_1.nnz(), data_2.nnz()); 

  // Make sure the row iteration is the same 
  for(size_t i = 0; i < data_1.size(); ++i) {
      
    auto row_1 = data_1[i];
    auto row_2 = data_2[i];

    TS_ASSERT(row_1 == row_2); 
    TS_ASSERT(!(row_1 != row_2)); 

    for(size_t j = 0; j < row_1.size(); ++j) {
      TS_ASSERT_EQUALS(row_1[j], row_2[j]);
      TS_ASSERT( !(row_1[j] != row_2[j]));
    }
  }
}

// Generic function to test saving, loading, and equality testing.
template <typename SRS> void _test_save_load_etc(const SRS& data) {
  // Save it.
  std::ostringstream save_stream;

  graphlab::oarchive oarc(save_stream);

  oarc << data;

  // Load it
  std::istringstream load_stream(save_stream.str());
  graphlab::iarchive iarc(load_stream);

  SRS l_data;
  iarc >> l_data;

  _test_equal(data, l_data);

  // Also test copy construction.
  _test_equal(data, SRS(data));
}


class test_simple_row_storage : public CxxTest::TestSuite {
 public:

  void test_simple() {

    simple_row_storage<size_t> data;

    size_t value = 0;
    std::vector<size_t> add_vect;

    for(size_t i = 0; i < test_size; ++i) {
      add_vect.clear();

      for(size_t j = 0; j < (i % 20); ++j) {
        add_vect.push_back(value);
        ++value;
      }

      TS_ASSERT_EQUALS(add_vect.size(), i % 20);

      data.add(add_vect);
    }

    TS_ASSERT_EQUALS(data.nnz(), value);
    TS_ASSERT_EQUALS(data.size(), test_size);
    TS_ASSERT(!data.empty());

    for(size_t i = 0; i < test_size; ++i)
      TS_ASSERT_EQUALS(data[i].size(), i % 20);

    {
      size_t row = 0;
      size_t v = 0;
      for(const auto& x : data) {

        TS_ASSERT_EQUALS(x.size(), data[row].size());

        TS_ASSERT(x == data[row]);
        TS_ASSERT_EQUALS(x.size(), row % 20);

        size_t j = 0;
        for(const auto& xv : x) {
          TS_ASSERT_EQUALS(xv, x[j]);
          TS_ASSERT_EQUALS(xv, v);
          ++v;
          ++j;
        }

        ++row;
      }

      TS_ASSERT_EQUALS(data.size(), row);
    }

    _test_save_load_etc(data); 
    
    data.clear();
    TS_ASSERT_EQUALS(data.size(), 0);
    TS_ASSERT_EQUALS(data.nnz(), 0);
    TS_ASSERT_EQUALS(data.empty(), true);
  }

  void test_simple_const() {

    simple_row_storage<size_t> data_fill;

    size_t value = 0;
    std::vector<size_t> add_vect;

    for(size_t i = 0; i < test_size; ++i) {
      add_vect.clear();

      for(size_t j = 0; j < (i % 20); ++j) {
        add_vect.push_back(value);
        ++value;
      }

      TS_ASSERT_EQUALS(add_vect.size(), i % 20);

      data_fill.add(add_vect);
    }

    _test_save_load_etc(data_fill);

    const simple_row_storage<size_t>& data = data_fill;

    _test_save_load_etc(data);

    TS_ASSERT_EQUALS(data.nnz(), value);
    TS_ASSERT_EQUALS(data.size(), test_size);
    TS_ASSERT(!data.empty());

    for(size_t i = 0; i < test_size; ++i)
      TS_ASSERT_EQUALS(data[i].size(), i % 20);

    {
      size_t row = 0;
      size_t v = 0;
      for(const auto& x : data) {

        TS_ASSERT_EQUALS(x.size(), data[row].size());

        TS_ASSERT(x == data[row]);
        TS_ASSERT_EQUALS(x.size(), row % 20);

        size_t j = 0;
        for(const auto& xv : x) {
          TS_ASSERT_EQUALS(xv, x[j]);
          TS_ASSERT_EQUALS(xv, v);
          ++v;
          ++j;
        }

        ++row;
      }

      TS_ASSERT_EQUALS(data.size(), row);
    }
  }

  void test_writeability() {

    simple_row_storage<size_t> data;

    for(size_t i = 0; i < test_size; ++i)
      data.add(100, 0);

    // Now go through and set a bunch of things.
    {
      size_t v = 0;
      for(size_t i = 0; i < test_size; ++i) {
        for(size_t j = 0; j < 100; ++j) {
          data[i][j] = v;
          ++v;
        }
      }
    }

    // Is it correct?
    {
      size_t v = 0;
      for(size_t i = 0; i < test_size; ++i) {
        for(size_t j = 0; j < 100; ++j) {
          TS_ASSERT_EQUALS(data[i][j], v);
          ++v;
        }
      }
    }
  }

  void test_mutability() {

    simple_row_storage<size_t> data;

    for(size_t i = 0; i < test_size; ++i)
      data.add(100, 0);

    {
      size_t v = 0;
      for(size_t i = 0; i < test_size; ++i) {
        for(size_t j = 0; j < 100; ++j) {
          data[i][j] += v;
          ++v;
        }
      }
    }

    // Is it correct?
    {
      size_t v = 0;
      for(size_t i = 0; i < test_size; ++i) {
        for(size_t j = 0; j < 100; ++j) {
          TS_ASSERT_EQUALS(data[i][j], v);
          ++v;
        }
      }
    }
  }

  void test_mutability_front_back() {

    simple_row_storage<size_t> data;

    for(size_t i = 0; i < 10; ++i)
      data.add(3, 0);

    for(auto& row : data) {
      row.front() += 1;
      row.back() += 2;
    }

    for(size_t i = 0; i < 10; ++i) {
      TS_ASSERT_EQUALS(data[i].front(), 1);
      TS_ASSERT_EQUALS(data[i].back(), 2);
    }
  }

  void test_filling_by_vector_cast() {

    simple_row_storage<double> data;

    std::vector<size_t> fill{0, 1, 2}; 

    data.add(fill);

    TS_ASSERT_EQUALS(data.size(), 1); 
    TS_ASSERT_EQUALS(data[0].size(), 3); 

    TS_ASSERT_EQUALS(data[0][0], 0);
    TS_ASSERT_EQUALS(data[0][1], 1);
    TS_ASSERT_EQUALS(data[0][2], 2);
  }

  void test_filling_by_iterator_direct() {

    simple_row_storage<size_t> data;

    std::vector<size_t> fill{0, 1, 2}; 

    data.add(fill.begin(), fill.end());

    TS_ASSERT_EQUALS(data.size(), 1); 
    TS_ASSERT_EQUALS(data[0].size(), 3); 

    TS_ASSERT_EQUALS(data[0][0], 0);
    TS_ASSERT_EQUALS(data[0][1], 1);
    TS_ASSERT_EQUALS(data[0][2], 2);
  }

  void test_filling_by_iterator_cast() {

    simple_row_storage<double> data;

    std::vector<size_t> fill{0, 1, 2}; 

    data.add(fill.begin(), fill.end());

    TS_ASSERT_EQUALS(data.size(), 1); 
    TS_ASSERT_EQUALS(data[0].size(), 3); 

    TS_ASSERT_EQUALS(data[0][0], 0);
    TS_ASSERT_EQUALS(data[0][1], 1);
    TS_ASSERT_EQUALS(data[0][2], 2);
  }

  void test_filling_by_pointer() {

    simple_row_storage<double> data;

    double x[3] = {0, 1, 2}; 

    data.add(x, x+3);

    TS_ASSERT_EQUALS(data.size(), 1); 
    TS_ASSERT_EQUALS(data[0].size(), 3); 

    TS_ASSERT_EQUALS(data[0][0], 0);
    TS_ASSERT_EQUALS(data[0][1], 1);
    TS_ASSERT_EQUALS(data[0][2], 2);
  }

  void test_filling_by_pointer_cast() {

    simple_row_storage<double> data;

    int x[3] = {0, 1, 2}; 

    data.add(x, x+3);

    TS_ASSERT_EQUALS(data.size(), 1); 
    TS_ASSERT_EQUALS(data[0].size(), 3); 

    TS_ASSERT_EQUALS(data[0][0], 0);
    TS_ASSERT_EQUALS(data[0][1], 1);
    TS_ASSERT_EQUALS(data[0][2], 2);
  }
  
  void test_indexed_data() {

    simple_row_storage<indexed_entry<size_t> > data;

    size_t idx_count = 0;

    // Get a somewhat more complicated structure
    for(size_t i = 0; i < 1000; ++i) {
      std::vector<indexed_entry<size_t> > x;

      for(size_t j = 0; j < i + 3; ++j) {
        x.push_back( {idx_count, simple_random_mapping(idx_count, 0) } );
        ++idx_count;
      }

      data.add(x);
    }

    _test_save_load_etc(data);
  }
};
