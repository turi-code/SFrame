/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <random>
#include <bitset>
#include <set>
#include <sstream>
#include <vector>
#include <algorithm>

#undef HAVE_BUILTIN_POPCOUNT
#define HAVE_BUILTIN_POPCOUNT

#undef HAVE_BUILTIN_CTZ
#define HAVE_BUILTIN_CTZ

#undef HAVE_BUILTIN_CLZ
#define HAVE_BUILTIN_CLZ

#include <graphlab/util/bitops.hpp>

using namespace graphlab;
using namespace std;

static const size_t number_per_check = 10000;

// Get fool proof versions of all the bitop tests

template <typename T> unsigned int check_n_bits_on(T v) {
  unsigned int c = 0;

  while(v) {
    c += (v & 0x1) ? 1 : 0;
    v >>= 1;
  }

  return c;
}

template <typename T> bool check_is_power_of_2(T v) {
  return (check_n_bits_on(v) <= 1);
}

template <typename T> int check_n_trailing_zeros(T v) {
  if(v == 0)
    return bitsizeof(T);

  unsigned int c = 0;
  unsigned int bit = 0;

  while( (v & (T(1) << bit)) == 0 && c < bitsizeof(v)) {
    ++c; ++bit;
  }

  return c;
}

template <typename T> int check_n_leading_zeros(T v) {
  if(v == 0)
    return bitsizeof(T);

  unsigned int c = 0;
  unsigned int bit = bitsizeof(T) - 1;

  while( (v & (T(1) << bit)) == 0 && bit != 0) {
    ++c; --bit;
  }

  return c;
}

template <typename T> int check_index_first_on_bit(T v) {
  unsigned int bit = 0;

  for(bit = 0; bit < bitsizeof(T); ++bit) {
    if(v & (T(1) << bit) )
      break;
  }
  return bit;
}

template <typename T> int check_index_last_on_bit(T v) {
  int bit = bitsizeof(T);

  for(; (bit--) != 0;) {
    if(v & (T(1) << bit) )
      break;
  }
  return bit < 0 ? bitsizeof(T) : bit;
}

template <typename T> int check_log2_floor(T v) {

  int l;

  for(l = -1; v != 0; ++l) {
    v >>= 1;
  }

  return l;
}

template <typename T> int check_log2_ceil(T v) {

  int l = -1;

  if(check_n_bits_on(v) != 1) {
    ++l;
  }

  for(; v != 0; ++l) {
    v >>= 1;
  }

  return l;
}

template <typename T>
class check_that_type {
 public:
  check_that_type() {
    values.push_back(8);
    values.push_back(0);

    T v = 1;
    while(v) {
      values.push_back(v);
      values.push_back(~v);
      values.push_back(v-1);
      values.push_back(~(v-1));
      values.push_back(v + 1);
      values.push_back(v + (v >> 1) );
      values.push_back(v + (v >> 2) );

      v <<= 1;
    }

    std::default_random_engine generator(0);

    for(size_t i = 0; i < number_per_check; ++i) {
      T v = 0;

      for(size_t j = 0; j < bitsizeof(T); ++j) {
        bool is_on = std::uniform_real_distribution<double>()(generator) < 0.25;
        if(is_on)
          v |= (T(1) << j);
      }

      values.push_back(v);
    }
  }

  void _test_is_power_of_2() {
    for(T v : values) {
      TS_ASSERT_EQUALS(is_power_of_2(v), check_is_power_of_2(v));
    }
  }


  void _test_bit_mask() {
    for(unsigned i = 0; i < bitsizeof(T); ++i) {
      T s = bit_mask<T>(i);

      TS_ASSERT_EQUALS(n_trailing_ones(s), i);
      TS_ASSERT_EQUALS(n_leading_zeros(s), bitsizeof(T) - i);
    }
  }

  void _test_n_leading_zeros() {
    for(T v : values) {
      // cerr << std::bitset<bitsizeof(T)>(v) << ":" << endl;
      TS_ASSERT_EQUALS(n_leading_zeros(v), check_n_leading_zeros(v));
    }
  }

  void _test_n_trailing_zeros() {
    for(T v : values) {
      // cerr << std::bitset<bitsizeof(T)>(v) << ":" << endl;
      TS_ASSERT_EQUALS(n_trailing_zeros(v), check_n_trailing_zeros(v));
    }
  }

  void _test_index_last_on_bit() {
    for(T v : values) {
      TS_ASSERT_EQUALS(index_last_on_bit(v), check_index_last_on_bit(v));
    }
  }

  void _test_index_first_on_bit() {
    for(T v : values) {
      TS_ASSERT_EQUALS(index_first_on_bit(v), check_index_first_on_bit(v));
    }
  }

  void _test_log2_floor() {
    for(T v : values) {
      // cerr << std::bitset<bitsizeof(T)>(v) << ":" << endl;

      if(v == 0) {
        TS_ASSERT_EQUALS(bitwise_log2_floor(v), 0);
      } else {

        if(bitsizeof(T) <= 16)
          TS_ASSERT_EQUALS(check_log2_floor(v), unsigned(floor(log2(double(v)))));

        TS_ASSERT_EQUALS(bitwise_log2_floor(v), check_log2_floor(v));
      }
    }
  }

  void _test_log2_ceil() {
    for(T v : values) {

      if(v == 0) {
        TS_ASSERT_EQUALS(bitwise_log2_ceil(v), 0);
      } else {

        if(bitsizeof(T) <= 16)
          TS_ASSERT_EQUALS(check_log2_ceil(v), unsigned(ceil(log2(double(v)))));

        TS_ASSERT_EQUALS(bitwise_log2_ceil(v), check_log2_ceil(v));
      }
    }
  }

  void _test_bitwise_pow2_mod() {
    for(T v : values) {

      for(unsigned int i = 0; i <= bitsizeof(T); ++i) {
        // cerr << "i = " << i << "; v = " << std::bitset<bitsizeof(T)>(v) << ":" << endl;
        TS_ASSERT_EQUALS(bitwise_pow2_mod(v, i), v % (T(1) << i));
      }
    }
  }

private:
  vector<T> values;
};

class uint8_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<uint8_t> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};


class uint16_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<uint16_t> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};

class uint32_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<uint32_t> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};

class uint64_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<uint64_t> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};

class uint128_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<uint128_t> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};


class ui_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<unsigned int> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};

class ul_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<unsigned long> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};

class ull_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<unsigned long long> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};

class u_bitops_test : public CxxTest::TestSuite {
 public:
  check_that_type<unsigned> ch;

  void test_is_power_of_2() { ch._test_is_power_of_2(); }
  void test_bit_mask() { ch._test_bit_mask(); }
  void test_n_leading_zeros() {ch._test_n_leading_zeros(); }
  void test_n_trailing_zeros() {ch._test_n_trailing_zeros(); }
  void test_index_first_on_bit() {ch._test_index_first_on_bit() ; }
  void test_index_last_on_bit() {ch._test_index_last_on_bit() ; }
  void test_log2_floor() { ch._test_log2_floor(); }
  void test_log2_ceil() { ch._test_log2_ceil(); }
  void test_bitwise_pow2_mod() { ch._test_bitwise_pow2_mod(); }
};

