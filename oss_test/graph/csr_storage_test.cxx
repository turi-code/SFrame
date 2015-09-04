/*  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */
#include <iostream>
#include <cxxtest/TestSuite.h>

#include <graphlab/util/generics/csr_storage.hpp>
#include <graphlab/util/generics/dynamic_csr_storage.hpp>
#include <graphlab/util/generics/shuffle.hpp>
#include <logger/assertions.hpp>

class csr_storage_test : public CxxTest::TestSuite {  
 public:
  typedef int valuetype;
  typedef size_t keytype;
  typedef size_t sizetype;

  typedef graphlab::csr_storage<valuetype, sizetype> csr_storage;
  typedef graphlab::dynamic_csr_storage<valuetype, sizetype, 2> dcsr2_t;
  typedef graphlab::dynamic_csr_storage<valuetype, sizetype, 4> dcsr4_t;
  typedef graphlab::dynamic_csr_storage<valuetype, sizetype, 8> dcsr8_t;
  typedef graphlab::dynamic_csr_storage<valuetype, sizetype, 16> dcsr16_t;
  typedef graphlab::dynamic_csr_storage<valuetype, sizetype, 64> dcsr64_t;
  
 public:
  csr_storage_test() {
    keytype keyin_arr[] = {1, 3, 6, 9, 5, 2};
    valuetype valin_arr[] = {3, 2, 1, 4, 4, 4};

    _keyin.assign(keyin_arr, keyin_arr + sizeof(keyin_arr) / sizeof(keytype));
    _valin.assign(valin_arr, valin_arr + sizeof(valin_arr) / sizeof(valuetype));

    keytype keyout_arr[] = {1, 2, 3, 5, 6, 9};
    valuetype valout_arr[] = {3, 4, 2, 4, 1, 4};

    _keyout.assign(keyout_arr, keyout_arr + sizeof(keyout_arr) / sizeof(keytype));
    _valout.assign(valout_arr, valout_arr + sizeof(valout_arr) / sizeof(valuetype));
  }

  void test_csr_storage() {
    std::cout << "Test csr_storage constructor" << std::endl;
    csr_storage csr(get_keyin(), get_valin());
    check(csr, get_keyout(), get_valout());
    csr.print(std::cout);
    printf("+ Pass test: csr_storage constructor :)\n\n");
  }

  void test_csr_storage2() {
    std::cout << "Test csr_storage wrap " << std::endl;
    std::vector<keytype> keys(get_keyin());
    std::vector<valuetype> values(get_valin());

    std::vector<sizetype> permute_index;
    std::vector<sizetype> prefix;

    graphlab::counting_sort(keys, permute_index, &prefix);
    graphlab::outofplace_shuffle(values, permute_index);

    csr_storage csr;
    csr.wrap(prefix, values);
    check(csr, get_keyout(), get_valout());
    printf("+ Pass test: csr_storage wrap :)\n\n");
  }

  template<typename csr_type>
  void dynamic_csr_storage_constructor_test() {
    std::cout << "Test dynamic csr_storage constructor" << std::endl;
    csr_type csr(get_keyin(), get_valin());
    check(csr, get_keyout(), get_valout());
    printf("+ Pass test: dynamic_csr_storage constructor :)\n\n");

    std::cout << "Test dynamic csr_storage wrap" << std::endl;

    csr.clear();
    std::vector<keytype> keys(get_keyin());
    std::vector<valuetype> values(get_valin());
    std::vector<sizetype> permute_index;
    std::vector<sizetype> prefix;
    
    graphlab::counting_sort(keys, permute_index, &prefix);
    graphlab::outofplace_shuffle(values, permute_index);

    csr.wrap(prefix, values);
    check(csr, get_keyout(), get_valout());
    printf("+ Pass test: dynamic_csr_storage wrap:)\n\n");
  }

  template<typename csr_type>
  void dynamic_csr_storage_insertion_test() {
    std::cout << "Test dynamic csr_storage insertion" << std::endl;
    std::vector<keytype> keys(get_keyin());
    std::vector<valuetype> values(get_valin());

    csr_type csr;
    for (size_t i = 0; i < keys.size(); ++i) {
      csr.insert(keys[i], values[i]);
    }
    csr.get_values().print(std::cerr);
    check(csr, get_keyout(), get_valout());
    csr.repack();
    check(csr, get_keyout(), get_valout());
    printf("+ Pass test: dynamic_csr_storage insertion:)\n\n");
  }

  void test_dynamic_csr_storage_constructor() {
      dynamic_csr_storage_constructor_test<dcsr2_t>();
      dynamic_csr_storage_constructor_test<dcsr4_t>();
      dynamic_csr_storage_constructor_test<dcsr8_t>();
      dynamic_csr_storage_constructor_test<dcsr16_t>();
  }
 
  void test_dynamic_csr_storage_insertion() {
      dynamic_csr_storage_insertion_test<dcsr2_t>();
      dynamic_csr_storage_insertion_test<dcsr4_t>();
      dynamic_csr_storage_insertion_test<dcsr8_t>();
      dynamic_csr_storage_insertion_test<dcsr16_t>();
  }

 
  template<typename csr_type>
  void dynamic_csr_storage_range_insertion_test(size_t nkey, size_t nval) {
    std::cout << "Test dynamic csr_storage range insertion" << std::endl;
    csr_type csr;
    for (size_t i = 0; i < nkey; ++i) {
      std::vector<valuetype> vals(nval, i);
      csr.insert(i, vals.begin(), vals.end());
    }
    // csr.print(std::cout);
    check_dcsr(csr, nkey, nval);

    csr.clear();
    TS_ASSERT_EQUALS(csr.num_keys(), 0);
    TS_ASSERT_EQUALS(csr.num_values(), 0);
    TS_ASSERT_EQUALS(csr.get_values().num_blocks(), 0);

    for (int i = nkey-1; i >= 0; --i) {
      std::vector<valuetype> vals(nval, i);
      csr.insert((keytype)i, vals.begin(), vals.end());
    }
    csr.get_values().print(std::cout);
    check_dcsr(csr, nkey, nval);
    std::cout << "test repack..." << std::endl;
    csr.repack();
    check_dcsr(csr, nkey, nval);
    printf("+ Pass test: dynamic_csr_storage range insertion:)\n\n");
  }

  void test_dynamic_csr_storage_range_insertion() {
      dynamic_csr_storage_range_insertion_test<dcsr2_t>(4, 4);
      dynamic_csr_storage_range_insertion_test<dcsr4_t>(6, 9);
      dynamic_csr_storage_range_insertion_test<dcsr8_t>(8, 3);
      dynamic_csr_storage_range_insertion_test<dcsr16_t>(20, 64);
  }

  void test_dynamic_csr_storage_stress_insertion() {
      stress_insertion_test<dcsr2_t>(4, 4);
      stress_insertion_test<dcsr8_t>(6, 9);
      stress_insertion_test<dcsr4_t>(8, 3);
      stress_insertion_test<dcsr64_t>(982, 294);
  }



  template<typename csr_type>
  void stress_insertion_test(size_t nkey, size_t nval) {
    std::cout << "Test dynamic csr_storage stess insertion" << std::endl;
    // stress test single insertion
    csr_type csr;
    csr_type csr_copy;

    for (size_t j = 0; j < nval; ++j) {
      for (size_t i = 0; i < nkey; i+=2) {
        csr.insert(i, i);
      }
    }
    for (size_t j = 0; j < nval; ++j) {
      for (int i = nkey-1; i >= 0; i-=2) {
        csr.insert((keytype)i, i);
      }
    }
    check_dcsr(csr, nkey, nval);
    csr_copy = csr;
    ASSERT_TRUE(csr_copy == csr);

    csr.clear();
    // stress test range insertion
    for (size_t i = 0; i < nkey; i+=2) {
      std::vector<valuetype> values(nval, i);
      csr.insert(i, values.begin(), values.end());
    }
    for (int i = nkey-1; i>=0; i-=2) {
      std::vector<valuetype> values(nval, i);
      csr.insert((keytype)i, values.begin(), values.end());
    }
    check_dcsr(csr, nkey, nval);

    csr_copy = csr;
    ASSERT_TRUE(csr_copy == csr);
    printf("+ Pass test: dynamic_csr_storage stress insertion:)\n\n");
  }

 private:
  template<typename csr_type>
      void check(csr_type& csr,
                 std::vector<keytype> keyout,
                 std::vector<valuetype> valout) {
        typedef typename csr_type::iterator iterator;
        size_t id = 0;
        for (size_t i = 0; i < csr.num_keys(); ++i) {
          iterator iter = csr.begin(i);
          while (iter != csr.end(i)) {
            TS_ASSERT_EQUALS(i, keyout[id]);
            TS_ASSERT_EQUALS(*iter, valout[id]); 
            ++iter;
            ++id;
          }
        }
      }
  template<typename csr_type>
      void check_dcsr(csr_type& csr,
                      size_t nkey,
                      size_t nval) {
    TS_ASSERT_EQUALS(csr.num_keys(), nkey);
    TS_ASSERT_EQUALS(csr.num_values(), nkey*nval);
    for (size_t i = 0; i < csr.num_keys(); ++i) {
      typename csr_type::iterator iter = csr.begin(i);
      size_t size = 0;
      while(iter != csr.end(i)) {
        TS_ASSERT_EQUALS(*iter, (valuetype)i);
        ++iter;
        ++size;
      }
      TS_ASSERT_EQUALS(size, nval);
    }
    csr.meminfo(std::cout);
  }


  std::vector<keytype> get_keyin() { return std::vector<keytype>(_keyin); }

  std::vector<valuetype> get_valin() { return std::vector<valuetype>(_valin); }
  
  std::vector<keytype> get_keyout() { return std::vector<keytype>(_keyout); }

  std::vector<valuetype> get_valout() { return std::vector<valuetype>(_valout); }

  std::vector<keytype> _keyin;
  std::vector<keytype> _keyout;
  std::vector<valuetype> _valin;
  std::vector<valuetype> _valout;
}; // end of test
