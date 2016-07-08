/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <graphlab/util/generics/symmetric_2d_array.hpp> 

using namespace graphlab;

class test_2d_array {
 public:
  test_2d_array(size_t _n)
  : n(_n)
  , data(n*n, 0)
  {}

  void set(size_t i, size_t j, size_t v) {
    data[i*n + j] = v; 
    data[j*n + i] = v; 
  }
  
  size_t get(size_t i, size_t j) const {
    return data[i*n + j];
  }

 private:
  size_t n;
  std::vector<size_t> data; 
};

void test_equal(const symmetric_2d_array<size_t>& X, const test_2d_array& XR) {

  ////////////////////////////////////////

  std::ostringstream save_stream;

  graphlab::oarchive oarc(save_stream);

  oarc << X;

  // Load it
  std::istringstream load_stream(save_stream.str());
  graphlab::iarchive iarc(load_stream);

  symmetric_2d_array<size_t> alt_X; 
  iarc >> alt_X;

  ////////////////////////////////////////
  
  size_t n = X.size(); 

  for(size_t i = 0; i < n; ++i) {
    for(size_t j = i; j < n; ++j) {
      TS_ASSERT_EQUALS(X(i,j), XR.get(i,j));
      TS_ASSERT_EQUALS(X(i,j), X(j,i)); 
      TS_ASSERT_EQUALS(X(i,j), alt_X(j,i)); 
    }
  }
}

class symmetric_array_test : public CxxTest::TestSuite {
 public:

  void _test_array(size_t n) {

    test_2d_array XR(n);
    symmetric_2d_array<size_t> X(n, 0);

    size_t c = 1;

    for(size_t i = 0; i < n; ++i) {
      for(size_t j = 0; j < n; ++j) {
        X(i,j) = c;
        XR.set(i,j, c);

        test_equal(X, XR);
        c *= 123142124123ULL;
        c += 455643; 
      }
    }
        
    for(size_t i = 0; i < n; ++i) {
      for(size_t j = 0; j < n; ++j) {
        X(i,j) += c;
        XR.set(i,j, XR.get(i,j) + c);

        test_equal(X, XR);
        c *= 123124134223ULL;
        c += 44455643; 
      }
    }
  }

  void test_corner() {
    _test_array(1);
  }

  void test_small() {
    _test_array(5);
  }

  void test_larger() {
    _test_array(16);
  }
};
