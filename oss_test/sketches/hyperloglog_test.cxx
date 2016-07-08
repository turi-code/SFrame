/*
* Copyright (C) 2016 Turi
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
#include <string>
#include <iostream>
#include <sketches/hyperloglog.hpp>
#include <random/random.hpp>
#include <cxxtest/TestSuite.h>
class hyperloglog_test: public CxxTest::TestSuite {  
 private:
  void random_integer_length_test(size_t len, 
                                  size_t random_range,
                                  size_t hllbits) {
    graphlab::sketches::hyperloglog hll(hllbits);
    std::vector<size_t> v(len);
    for (size_t i = 0;i < len; ++i) {
      v[i] = graphlab::random::fast_uniform<size_t>(0, random_range - 1);
      hll.add(v[i]);
    }
    std::sort(v.begin(), v.end());
    auto ret = std::unique(v.begin(), v.end());
    size_t num_unique = std::distance(v.begin(), ret);

    auto lower = hll.estimate() - 2 * hll.error_bound();
    auto upper = hll.estimate() + 2 * hll.error_bound();
    std::cout << num_unique << 
              " vs (" << lower << ", " 
                     << upper << ")\n";
    TS_ASSERT_LESS_THAN(lower, num_unique);
    TS_ASSERT_LESS_THAN(num_unique, upper);
  }

  void parallel_combine_test(size_t len, 
                             size_t random_range,
                             size_t hllbits) {
    // make a bunch of "parallel" hyperloglogs which can be combined
    using graphlab::sketches::hyperloglog;
    std::vector<hyperloglog> hllarr(16, hyperloglog(hllbits));
    // test against the sequential one
    hyperloglog sequential_hll(hllbits);

    std::vector<size_t> v(len);
    for (size_t i = 0;i < len; ++i) {
      v[i] = graphlab::random::fast_uniform<size_t>(0, random_range - 1);
      hllarr[i% 16].add(v[i]);
      sequential_hll.add(v[i]);
    }
    // make the final hyperloglog which comprises of all the 
    // hyperloglogs combined
    hyperloglog hll(hllbits);
    for (size_t i = 0;i < hllarr.size(); ++i) hll.combine(hllarr[i]);

    std::sort(v.begin(), v.end());
    auto ret = std::unique(v.begin(), v.end());
    size_t num_unique = std::distance(v.begin(), ret);

    auto lower = hll.estimate() - 2 * hll.error_bound();
    auto upper = hll.estimate() + 2 * hll.error_bound();
    std::cout << num_unique << 
              " vs (" << lower << ", " 
                     << upper << ")\n";
    TS_ASSERT_LESS_THAN(lower, num_unique);
    TS_ASSERT_LESS_THAN(num_unique, upper);
    TS_ASSERT_EQUALS(hll.estimate(), sequential_hll.estimate());
  }
 public:
  void test_stuff() {
    graphlab::random::seed(1001);
    std::vector<size_t> lens{1024, 65536, 1024*1024};
    std::vector<size_t> ranges{128, 1024, 65536, 1024*1024};
    std::vector<size_t> bits{8, 12, 16};
    for (auto len: lens) {
      for (auto range: ranges) {
        for (auto bit: bits) {
          std::cout << "Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Num Buckets: 2^" << bit << "\n";
          random_integer_length_test(len, range, bit);
        }
      }
    }

    std::cout << "\n\nReset random seed and repeating with \'parallel\' test\n";
    graphlab::random::seed(1001);
    for (auto len: lens) {
      for (auto range: ranges) {
        for (auto bit: bits) {
          std::cout << "Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Num Buckets: 2^" << bit << "\n";
          parallel_combine_test(len, range, bit);
        }
      }
    }
  }
};
