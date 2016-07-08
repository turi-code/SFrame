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
#include <set>
#include <vector>
#include <sgraph/hilbert_parallel_for.hpp>
#include <cxxtest/TestSuite.h>

using namespace graphlab;

class hilbert_par_for_test: public CxxTest::TestSuite {
 public:
  void test_runner(size_t n, size_t threads) {
    std::vector<std::pair<size_t, size_t> > preamble_hits;
    mutex lock;
    std::vector<std::pair<size_t, size_t> > parallel_hits;
    sgraph_compute::hilbert_blocked_parallel_for(n, 
                                 [&](std::vector<std::pair<size_t, size_t> > v) {
                                   std::copy(v.begin(), v.end(), std::inserter(preamble_hits, preamble_hits.end()));
                                 },
                                 [&](std::pair<size_t, size_t> v) {
                                   lock.lock();
                                   parallel_hits.push_back(v);
                                   lock.unlock();
                                 }, threads);
    TS_ASSERT_EQUALS(preamble_hits.size(), n * n);
    TS_ASSERT_EQUALS(parallel_hits.size(), n * n);
    std::set<std::pair<size_t, size_t> > unique_vals;
    std::copy(preamble_hits.begin(), preamble_hits.end(), std::inserter(unique_vals, unique_vals.end()));
    TS_ASSERT_EQUALS(unique_vals.size(), n * n);
    // insert parallel again. We don't need to clear unique
    std::copy(parallel_hits.begin(), parallel_hits.end(), std::inserter(unique_vals, unique_vals.end()));
    TS_ASSERT_EQUALS(unique_vals.size(), n * n);
  }


  void test_hilbert_par_for() {
    test_runner(4, 4);
    // try an odd number
    test_runner(16, 3);
    // sequential?
    test_runner(16, 1);
  }
};
