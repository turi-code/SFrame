/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <iostream>
#include <cppipc/client/issue.hpp>
#include <random>
#include <unordered_map>
#include <serialization/serialization_includes.hpp>
#include <parallel/pthread_tools.hpp>
#include <graphlab/dht/vector_dht.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <graphlab/util/token.hpp>

static const size_t n_keys = 100;
static const size_t n_elements_per_value  = 100;
static const size_t n_runs_per_test       = 5000;
static const double rwtest_prob_of_write  = 0.2;

using namespace graphlab;
using namespace std;

template <typename T>
struct value_tracker {

  void set(size_t key, size_t idx, T v) {
    values[key][idx] = v;
  };

  T get(size_t key, size_t idx) {
    return values[key][idx];
  };

  std::unordered_map<size_t, std:: unordered_map<size_t, T> > values;
};

static distributed_control dc;

template <typename T>
class _vector_dht_test_base {
 public:

  std::default_random_engine generator;

  void _reset_rng() { generator = std::default_random_engine(0); }

  long _gen_key() {
    return std::uniform_int_distribution<long>(0,n_keys-1)(generator);
  }

  size_t _gen_index() {
    size_t v;
    do {
      v = std::uniform_int_distribution<size_t>(0,n_elements_per_value)(generator);

      v -= (v % dc.numprocs());
      v += dc.procid();
    } while(v >= n_elements_per_value);

    return v;
  }

  T _gen_value() {
    return std::uniform_int_distribution<size_t>(1,100000)(generator) * T(1.5);
  }

  ////////////////////////////////////////////////////////////////////////////////

  template <typename DHT>
  void _test_default_value(DHT& vdht, T value) {
    _reset_rng();

    for(size_t i = 0; i < n_runs_per_test; ++i) {
      TS_ASSERT_EQUALS(vdht.get(Token(_gen_key()), _gen_index()), value);
    }
  }

  template <typename DHT>
  void _test_setting_simple(DHT& vdht) {
    _reset_rng();

    for(size_t i = 0; i < n_runs_per_test; ++i) {
      size_t key = _gen_key();
      size_t idx = _gen_index();

      T set_v = _gen_value();
      vdht.set(Token(key), idx, set_v);
      T retrieved_v = vdht.get(Token(key), idx);

      TS_ASSERT_EQUALS(set_v, retrieved_v);
    }
  }

  template <typename DHT>
  void _test_setting_staged(DHT& vdht) {
    _reset_rng();

    value_tracker<T> tracker;

    for(size_t i = 0; i < n_runs_per_test; ++i) {
      size_t key = _gen_key();
      size_t idx = _gen_index();

      T set_v = _gen_value();
      vdht.set(Token(key), idx, set_v);
      tracker.set(key, idx, set_v);
    }

    _reset_rng();

    for(size_t i = 0; i < n_runs_per_test; ++i) {
      size_t key = _gen_key();
      size_t idx = _gen_index();

      T retrieved_v = vdht.get(Token(key), idx);

      T set_v = tracker.get(key, idx);
      TS_ASSERT_EQUALS(set_v, retrieved_v);
    }
  }

  template <typename DHT>
  void _test_random_access(DHT& vdht) {
    _reset_rng();

    value_tracker<T> tracker;

    auto do_write = [this](){
      return (std::uniform_int_distribution<size_t>(1,100000)(generator)
              <= (size_t(floor(rwtest_prob_of_write * 100000))));
    };

    for(size_t i = 0; i < 10*n_runs_per_test; ++i) {

      if(do_write()) {

        size_t key = _gen_key();
        size_t idx = _gen_index();

        T set_v = _gen_value();

        vdht.set(Token(key), idx, set_v);
        tracker.set(key, idx, set_v);

        T check_v = vdht.get(Token(key), idx);

        TS_ASSERT_EQUALS(set_v, check_v);

      } else {

        size_t key = _gen_key();
        size_t idx = _gen_index();

        T retrieved_v = vdht.get(Token(key), idx);

        if(retrieved_v != 0) {
          T check_v = tracker.get(key, idx);

          TS_ASSERT_EQUALS(retrieved_v, check_v);
        }
      }
    }
  }


  template <typename DHT>
  void _test_vector_methods(DHT& vdht) {
    _reset_rng();

    value_tracker<T> tracker;

    auto do_write = [this](){
      return (std::uniform_int_distribution<size_t>(1,100000)(generator)
              <= (size_t(floor(rwtest_prob_of_write * 100000))));
    };

    for(size_t i = 0; i < 10*n_runs_per_test; ++i) {

      if(do_write()) {

        size_t key = _gen_key();
        size_t idx = _gen_index();
        
        vector<T> x = vdht.get_vector(key);

        if(x.size() != 0) {
          TS_ASSERT_EQUALS(x.size(), n_elements_per_value);
        } else {
          x.resize(n_elements_per_value);
        }
        
        T set_v = _gen_value();
        x[idx] = set_v;

        vdht.set_vector(Token(key), x); 
        tracker.set(key, idx, set_v);
        
        T check_v = vdht.get(Token(key), idx);

        TS_ASSERT_EQUALS(set_v, check_v);

      } else {

        size_t key = _gen_key();
        size_t idx = _gen_index();

        T retrieved_v = vdht.get(Token(key), idx);

        if(retrieved_v != 0) {
          T check_v = tracker.get(key, idx);

          TS_ASSERT_EQUALS(retrieved_v, check_v);
        }

        vector<T> retrieved_vec = vdht.get_vector(Token(key));

        if(retrieved_vec.size() != 0) {
          TS_ASSERT_EQUALS(retrieved_vec.size(), n_elements_per_value);

          if(retrieved_vec[idx] != 0) {
          
            T check_v = tracker.get(key, idx);

            TS_ASSERT_EQUALS(retrieved_vec[idx], check_v);
          }
        }
      }
    }
  }

  
  template <typename DHT>
  void _test_delta(DHT& vdht, T def_val = 0) {
    _reset_rng();

    value_tracker<T> tracker;

    auto do_write = [this](){
      return (std::uniform_int_distribution<size_t>(1,100000)(generator)
              <= (size_t(floor(rwtest_prob_of_write * 100000))));
    };

    auto test_delta = [&tracker, &vdht](size_t key, size_t idx) {
      
      T base_v = vdht.get(Token(key), idx);

      T delta_v_1 = vdht.apply_delta(Token(key), idx, T(1.5));

      T delta_v_2 = vdht.get(Token(key), idx);

      TS_ASSERT_EQUALS(delta_v_1, delta_v_2);
      TS_ASSERT_EQUALS(base_v + T(1.5), delta_v_1);

      T final_1 = vdht.apply_delta(Token(key), idx, T(-1.5));

      T final_2 = vdht.get(Token(key), idx);

      TS_ASSERT_EQUALS(final_1, final_2);
      TS_ASSERT_EQUALS(base_v, final_1);
    };
    
    for(size_t i = 0; i < 10*n_runs_per_test; ++i) {
      
      if(do_write()) {

        size_t key = _gen_key();
        size_t idx = _gen_index();

        T set_v = _gen_value();

        vdht.set(Token(key), idx, set_v);
        tracker.set(key, idx, set_v);

        T check_v = vdht.get(Token(key), idx);

        TS_ASSERT_EQUALS(set_v, check_v);

        test_delta(key, idx);

      } else {

        size_t key = _gen_key();
        size_t idx = _gen_index();

        test_delta(key, idx);

        T retrieved_v = vdht.get(Token(key), idx);

        if(retrieved_v != def_val) {
          T check_v = tracker.get(key, idx);

          TS_ASSERT_EQUALS(retrieved_v, check_v);
        }
      }
    }
  }
  
};

options_map _get_options(size_t def_vec_size, std::string def_val) {
  return options_map(std::map<std::string, std::string>(
      { {"default_value", def_val},
        {"vector_size",   std::to_string(def_vec_size)} }));
}

class vector_dht_test_long : public CxxTest::TestSuite {
 public:
  _vector_dht_test_base<long> vt;

  void test_default_value()
  {
    vector_dht<long> vdht(dc, _get_options(0, "555") );
    vt._test_default_value(vdht, long(555));
  }
  void test_setting_simple() {
    vector_dht<long> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    vector_dht<long> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    vector_dht<long> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_vector_methods() {
    vector_dht<long> vdht(dc);
    vt._test_vector_methods(vdht);
  }
  
  void test_delta() {
    vector_dht<long> vdht(dc);
    vt._test_delta(vdht);
  }

  void test_delta_with_default() {
    vector_dht<long> vdht(dc, _get_options(0, "5"));
    vt._test_delta(vdht, 5);
  }
};

class vector_dht_test_long_fixed_vector : public CxxTest::TestSuite {
 public:
  _vector_dht_test_base<long> vt;

  void test_default_value()
  {
    vector_dht<long> vdht(dc, _get_options(n_elements_per_value, "555"));
    vt._test_default_value(vdht, long(555));
  }
  void test_setting_simple() {
    vector_dht<long> vdht(dc, _get_options(n_elements_per_value, "0"));
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    vector_dht<long> vdht(dc, _get_options(n_elements_per_value, "0"));
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    vector_dht<long> vdht(dc, _get_options(n_elements_per_value, "0"));
    vt._test_random_access(vdht);
  }

  void test_vector_methods() {
    vector_dht<long> vdht(dc, _get_options(n_elements_per_value, "0"));
    vt._test_vector_methods(vdht);
  }
  
  void test_delta() {
    vector_dht<long> vdht(dc, _get_options(n_elements_per_value, "0"));
    vt._test_delta(vdht);
  }

  void test_delta_with_default() {
    vector_dht<long> vdht(dc, _get_options(n_elements_per_value, "5"));
    vt._test_delta(vdht, 5);
  }
};

class vector_dht_test_double : public CxxTest::TestSuite {
 public:
  _vector_dht_test_base<double> vt;

  void test_default_value()
  {
    vector_dht<double> vdht(dc, _get_options(0, "555.5"));
    vt._test_default_value(vdht, double(555.5));
  }
  void test_setting_simple() {
    vector_dht<double> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    vector_dht<double> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    vector_dht<double> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_vector_methods() {
    vector_dht<double> vdht(dc);
    vt._test_vector_methods(vdht);
  }
  
  void test_delta() {
    vector_dht<double> vdht(dc, _get_options(0, "0"));
    vt._test_delta(vdht);
  }

  void test_delta_with_default() {
    vector_dht<double> vdht(dc, _get_options(0, "5.5"));
    vt._test_delta(vdht, 5.5);
  }
};

