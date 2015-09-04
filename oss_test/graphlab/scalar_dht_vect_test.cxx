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
#include <graphlab/dht/scalar_dht.hpp>
#include <graphlab/dht/distributed_vector.hpp>
#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <graphlab/util/token.hpp>

static const size_t n_keys = 500;
static const size_t n_runs_per_test = 2000;
static const double rwtest_prob_of_write  = 0.2;

using namespace graphlab;

template <typename T>
struct value_tracker {

  void set(size_t key, T v) {
    values[key] = v;
  };

  T get(size_t key) {
    return values[key];
  };

  std::unordered_map<size_t, T> values;
};

static distributed_control dc;

#define RUN_SCALAR 0
#define RUN_BATCH_SINGLE 1
#define RUN_BATCH_BATCH 2


template <typename T, int vector_instance_mode>
class _scalar_dht_test_base {

////////////////////////////////////////////////////////////////////////////////
// A few things to make some stuff easier

  template <typename DHT, typename K>
  void set(DHT& dht, const K& k, T value) {

    switch(vector_instance_mode) {
      case RUN_SCALAR:
        dht.set(k,value);
        break;
      case RUN_BATCH_SINGLE:
        dht.batch_set(std::vector<K>{k}, value);
        break;
      case RUN_BATCH_BATCH:
        dht.batch_set(std::vector<K>{k}, std::vector<T>{value});
        break;
    }
  }


  template <typename DHT, typename K>
  T apply_delta(DHT& dht, const K& k, T delta) {

    switch(vector_instance_mode) {
      case RUN_SCALAR:
        return dht.apply_delta_return_new(k, delta);
      case RUN_BATCH_SINGLE:
        {
          std::vector<T> ret_v = dht.batch_apply_delta_return_new(std::vector<K>{k}, delta);
          return ret_v[0];
        }
      case RUN_BATCH_BATCH:
        {
          std::vector<T> ret_v = dht.batch_apply_delta_return_new(std::vector<K>{k}, std::vector<T>{delta});
          return ret_v[0];
        }
    }
  }

  template <typename DHT, typename K>
  typename DHT::ValueType get(DHT& dht, const K& k) {
    switch(vector_instance_mode) {
      case RUN_SCALAR:
        return dht.get(k);
      case RUN_BATCH_SINGLE:
      case RUN_BATCH_BATCH:
        {
          std::vector<T> ret_v = dht.batch_get(std::vector<K>{k});
          return ret_v[0];
        }
    }
  }


 public:

  std::default_random_engine generator;

  void _reset_rng() { generator = std::default_random_engine(0); }

  size_t _gen_key() {
    size_t v;
    do {
      v = std::uniform_int_distribution<size_t>(0,n_keys)(generator);

      v -= (v % dc.numprocs());
      v += dc.procid();
    } while(v >= n_keys);

    return v; 
  }

  T _gen_value() {
    return std::uniform_int_distribution<size_t>(1,100000)(generator) * T(1.5);
  }

  ////////////////////////////////////////////////////////////////////////////////

  template <typename DHT>
  void _test_setting_simple(DHT& vdht) {
    _reset_rng();

    for(size_t i = 0; i < n_runs_per_test; ++i) {
      size_t key = _gen_key();

      T set_v = _gen_value();
      set(vdht, key, set_v);
      T retrieved_v = get(vdht, key);

      TS_ASSERT_EQUALS(set_v, retrieved_v);
    }
  }

  template <typename DHT>
  void _test_setting_staged(DHT& vdht) {
    _reset_rng();

    value_tracker<T> tracker;

    for(size_t i = 0; i < n_runs_per_test; ++i) {
      size_t key = _gen_key();

      T set_v = _gen_value();
      set(vdht, key, set_v);
      tracker.set(key, set_v);
    }

    _reset_rng();

    for(size_t i = 0; i < n_runs_per_test; ++i) {
      size_t key = _gen_key();

      T retrieved_v = get(vdht, key);

      T set_v = tracker.get(key);
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

        T set_v = _gen_value();

        set(vdht, key, set_v);
        tracker.set(key, set_v);

        T check_v = get(vdht, key);

        TS_ASSERT_EQUALS(set_v, check_v);

      } else {

        size_t key = _gen_key();

        T retrieved_v = get(vdht, key);

        if(retrieved_v != 0) {
          T check_v = tracker.get(key);

          TS_ASSERT_EQUALS(retrieved_v, check_v);
        }
      }
    }
  }

  template <typename DHT>
  void _test_delta(DHT& vdht) {
    _reset_rng();

    value_tracker<T> tracker;

    auto do_write = [this](){
      return (std::uniform_int_distribution<size_t>(1,100000)(generator)
              <= (size_t(floor(rwtest_prob_of_write * 100000))));
    };

    auto test_delta = [this, &tracker, &vdht](size_t key) {
      
      T base_v = get(vdht, key);

      T delta_v_1 = apply_delta(vdht, key, T(1.5));

      T delta_v_2 = get(vdht, key);

      TS_ASSERT_EQUALS(delta_v_1, delta_v_2);
      TS_ASSERT_EQUALS(base_v + T(1.5), delta_v_1);

      T final_1 = apply_delta(vdht, key, T(-1.5));

      T final_2 = get(vdht, key);

      TS_ASSERT_EQUALS(final_1, final_2);
      TS_ASSERT_EQUALS(base_v, final_1);
    };
    
    for(size_t i = 0; i < 10*n_runs_per_test; ++i) {
      
      if(do_write()) {

        size_t key = _gen_key();

        T set_v = _gen_value();

        set(vdht, key, set_v);
        tracker.set(key, set_v);

        T check_v = get(vdht, key);

        TS_ASSERT_EQUALS(set_v, check_v);

        test_delta(key);

      } else {

        size_t key = _gen_key();

        test_delta(key);

        T retrieved_v = get(vdht, key);

        if(retrieved_v != 0) {
          T check_v = tracker.get(key);

          TS_ASSERT_EQUALS(retrieved_v, check_v);
        }
      }
    }
  }
};

options_map _get_options(size_t def_vec_size) {
  return options_map(std::map<std::string, std::string>(
      { {"vector_size",   std::to_string(def_vec_size)} }));
}


////////////////////////////////////////////////////////////////////////////////
// Methods that just test the basic stuff

class scalar_dht_test_long : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<long, RUN_SCALAR> vt;

  void test_setting_simple() {
    scalar_dht<long> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    scalar_dht<long> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    scalar_dht<long> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    scalar_dht<long> vdht(dc);
    vt._test_delta(vdht);
  }

  void test_delta_with_default() {
    scalar_dht<long> vdht(dc);
    vt._test_delta(vdht);
  }
};

class scalar_dht_test_double : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<double, RUN_SCALAR> vt;

  void test_setting_simple() {
    scalar_dht<double> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    scalar_dht<double> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    scalar_dht<double> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    scalar_dht<double> vdht(dc);
    vt._test_delta(vdht);
  }
};

class distributed_vector_test_long : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<long, RUN_SCALAR> vt;

  void test_setting_simple() {
    distributed_vector<long> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    distributed_vector<long> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    distributed_vector<long> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    distributed_vector<long> vdht(dc);
    vt._test_delta(vdht);
  }
};

class distributed_vector_test_double : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<double, RUN_SCALAR> vt;

  void test_setting_simple() {
    distributed_vector<double> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    distributed_vector<double> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    distributed_vector<double> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    distributed_vector<double> vdht(dc, _get_options(0));
    vt._test_delta(vdht);
  }
};

////////////////////////////////////////////////////////////////////////////////
// Now with the batch setting

class scalar_dht_test_long_batch_single : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<long, RUN_BATCH_SINGLE> vt;

  void test_setting_simple() {
    scalar_dht<long> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    scalar_dht<long> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    scalar_dht<long> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    scalar_dht<long> vdht(dc);
    vt._test_delta(vdht);
  }

  void test_delta_with_default() {
    scalar_dht<long> vdht(dc);
    vt._test_delta(vdht);
  }
};

class scalar_dht_test_double_batch_single : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<double, RUN_BATCH_SINGLE> vt;

  void test_setting_simple() {
    scalar_dht<double> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    scalar_dht<double> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    scalar_dht<double> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    scalar_dht<double> vdht(dc);
    vt._test_delta(vdht);
  }
};

class distributed_vector_test_long_batch_single : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<long, RUN_BATCH_SINGLE> vt;

  void test_setting_simple() {
    distributed_vector<long> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    distributed_vector<long> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    distributed_vector<long> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    distributed_vector<long> vdht(dc);
    vt._test_delta(vdht);
  }
};

class distributed_vector_test_double_batch_single : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<double, RUN_BATCH_SINGLE> vt;

  void test_setting_simple() {
    distributed_vector<double> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    distributed_vector<double> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    distributed_vector<double> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    distributed_vector<double> vdht(dc, _get_options(0));
    vt._test_delta(vdht);
  }
};

////////////////////////////////////////////////////////////////////////////////
// Now with the batch setting with batch updates

class scalar_dht_test_long_batch_batch : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<long, RUN_BATCH_BATCH> vt;

  void test_setting_simple() {
    scalar_dht<long> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    scalar_dht<long> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    scalar_dht<long> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    scalar_dht<long> vdht(dc);
    vt._test_delta(vdht);
  }

  void test_delta_with_default() {
    scalar_dht<long> vdht(dc);
    vt._test_delta(vdht);
  }
};

class scalar_dht_test_double_batch_batch : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<double, RUN_BATCH_BATCH> vt;

  void test_setting_simple() {
    scalar_dht<double> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    scalar_dht<double> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    scalar_dht<double> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    scalar_dht<double> vdht(dc);
    vt._test_delta(vdht);
  }
};

class distributed_vector_test_long_batch_batch : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<long, RUN_BATCH_BATCH> vt;

  void test_setting_simple() {
    distributed_vector<long> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    distributed_vector<long> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    distributed_vector<long> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    distributed_vector<long> vdht(dc);
    vt._test_delta(vdht);
  }
};

class distributed_vector_test_double_batch_batch : public CxxTest::TestSuite {
 public:
  _scalar_dht_test_base<double, RUN_BATCH_BATCH> vt;

  void test_setting_simple() {
    distributed_vector<double> vdht(dc);
    vt._test_setting_simple(vdht);
  }

  void test_setting_staged() {
    distributed_vector<double> vdht(dc);
    vt._test_setting_staged(vdht);
  }

  void test_random_access() {
    distributed_vector<double> vdht(dc);
    vt._test_random_access(vdht);
  }

  void test_delta() {
    distributed_vector<double> vdht(dc, _get_options(0));
    vt._test_delta(vdht);
  }
};

